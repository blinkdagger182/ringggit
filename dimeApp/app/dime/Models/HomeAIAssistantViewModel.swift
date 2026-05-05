//
//  HomeAIAssistantViewModel.swift
//  dime
//

import CoreData
import Foundation
import SwiftUI
import UIKit

struct AttachmentItem: Identifiable {
    let id = UUID()
    let thumbnail: UIImage
    let pages: [UIImage]
    let label: String

    init(image: UIImage, label: String) {
        self.thumbnail = image
        self.pages = [image]
        self.label = label
    }

    init(pdfPages: [UIImage], filename: String) {
        self.thumbnail = pdfPages.first ?? UIImage()
        self.pages = pdfPages
        let count = pdfPages.count
        self.label = "\(filename) · \(count) \(count == 1 ? "page" : "pages")"
    }
}

@MainActor
final class HomeAIAssistantViewModel: ObservableObject {
    @Published var isPresented = false
    @Published var draftMessage = ""
    @Published private(set) var messages: [HomeAIMessage] = []
    @Published private(set) var isResponding = false
    @Published private(set) var isAnimatingReply = false
    @Published private(set) var statusText: String = ""
    @Published var pendingAttachments: [AttachmentItem] = []

    var dataController: DataController?

    let quickActions: [HomeAIQuickAction] = [
        HomeAIQuickAction(title: "Add transaction", prompt: "Add transaction", systemImage: "plus.circle.fill"),
        HomeAIQuickAction(title: "Where did my money go?", prompt: "Where did my money go?", systemImage: "chart.pie.fill"),
        HomeAIQuickAction(title: "Show this month's spending", prompt: "Show this month's spending", systemImage: "calendar"),
        HomeAIQuickAction(title: "Can I afford this?", prompt: "Can I afford this?", systemImage: "checkmark.shield.fill")
    ]

    var quickHint: String {
        messages.isEmpty ? "Swipe down to open your finance assistant" : "Continue your last finance question"
    }

    var hasContent: Bool {
        !draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || !pendingAttachments.isEmpty
    }

    func expand() {
        guard !isPresented else { return }
        isPresented = true
        UIImpactFeedbackGenerator(style: .soft).impactOccurred()
    }

    func collapse() {
        guard isPresented else { return }
        isPresented = false
        UIApplication.shared.endEditing()
        UIImpactFeedbackGenerator(style: .light).impactOccurred()
    }

    func removeAttachment(id: UUID) {
        pendingAttachments.removeAll { $0.id == id }
    }

    func triggerQuickAction(_ quickAction: HomeAIQuickAction) {
        draftMessage = quickAction.prompt
        sendDraftMessage()
    }

    func sendDraftMessage() {
        let trimmed = draftMessage.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty || !pendingAttachments.isEmpty,
              !isResponding, !isAnimatingReply else { return }

        let messageText = trimmed.isEmpty ? "Please extract all transactions from the attached image(s)." : trimmed
        let capturedAttachments = pendingAttachments

        appendUserMessage(messageText, attachments: capturedAttachments)
        draftMessage = ""
        pendingAttachments = []
        isResponding = true

        // Tool executor: AI calls this when it needs transaction data for a date range
        let toolExecutor: (_ name: String, _ args: [String: Any]) async -> String = { [weak self] name, args in
            await MainActor.run {
                if name == "fetch_transactions",
                   let start = args["start_date"] as? String,
                   let end = args["end_date"] as? String {
                    self?.statusText = "Fetching transactions \(start) → \(end)"
                } else {
                    self?.statusText = "Fetching data"
                }
            }
            let result = await MainActor.run {
                self?.executeTool(name: name, args: args) ?? "No data available."
            }
            await MainActor.run { self?.statusText = "Analyzing" }
            return result
        }

        Task {
            statusText = "Thinking"
            do {
                let imageDataList = capturedAttachments
                    .flatMap { $0.pages }
                    .compactMap { $0.jpegData(compressionQuality: 0.65) }
                let systemPrompt = buildSystemPrompt()
                let response = try await AIService.send(
                    systemPrompt: systemPrompt,
                    conversationMessages: messages,
                    images: imageDataList,
                    toolExecutor: toolExecutor
                )
                statusText = "Writing response"
                executeActions(response.actions)
                await animateReply(response)
                statusText = ""
            } catch AIServiceError.apiError(let msg) {
                messages.append(HomeAIMessage(role: .assistant, text: "API error: \(msg)", date: .now))
                isResponding = false
                statusText = ""
            } catch let urlError as URLError where urlError.code == .timedOut {
                messages.append(HomeAIMessage(role: .assistant, text: "Request timed out — the file is large. Try a shorter document or fewer pages.", date: .now))
                isResponding = false
                statusText = ""
            } catch {
                messages.append(HomeAIMessage(role: .assistant, text: "Couldn't connect. Check your API key and internet connection.", date: .now))
                isResponding = false
                statusText = ""
            }
        }
    }

    // MARK: - Word-by-word animation

    private func animateReply(_ response: AIResponse) async {
        let now = Date()
        // thinking = nil during animation so ThinkingDisclosure state doesn't reset each word
        messages.append(HomeAIMessage(role: .assistant, text: "", date: now))
        isResponding = false
        isAnimatingReply = true

        let reply = response.reply.isEmpty ? "Done." : response.reply
        let words = reply.components(separatedBy: " ")
        var built = ""

        for (i, word) in words.enumerated() {
            if i > 0 { built += " " }
            built += word
            if let idx = messages.indices.last {
                messages[idx] = HomeAIMessage(role: .assistant, text: built, date: now)
            }
            try? await Task.sleep(nanoseconds: 28_000_000)
        }

        // Attach thinking + visual only after animation — stable state from here
        if let idx = messages.indices.last {
            messages[idx] = HomeAIMessage(role: .assistant, text: built, date: now, visual: response.visual, thinking: response.thinking)
        }
        isAnimatingReply = false
    }

    private func appendUserMessage(_ text: String, attachments: [AttachmentItem] = []) {
        messages.append(HomeAIMessage(role: .user, text: text, date: .now, attachments: attachments))
    }

    // MARK: - Tool execution (called by AI on demand)

    private func executeTool(name: String, args: [String: Any]) -> String {
        guard name == "fetch_transactions",
              let startStr = args["start_date"] as? String,
              let endStr = args["end_date"] as? String,
              let dc = dataController else {
            return "Tool not available."
        }

        let df = DateFormatter()
        df.dateFormat = "yyyy-MM-dd"
        df.locale = Locale(identifier: "en_US_POSIX")

        guard let startDate = df.date(from: startStr),
              let rawEnd = df.date(from: endStr),
              let endDate = Calendar.current.date(byAdding: .day, value: 1, to: rawEnd) else {
            return "Invalid date range: \(startStr) to \(endStr)."
        }

        let context = dc.container.viewContext
        let req = NSFetchRequest<Transaction>(entityName: "Transaction")
        req.predicate = NSPredicate(format: "date >= %@ AND date < %@", startDate as NSDate, endDate as NSDate)
        req.sortDescriptors = [NSSortDescriptor(key: "date", ascending: true)]
        let txs = (try? context.fetch(req)) ?? []

        if txs.isEmpty {
            return "No transactions found between \(startStr) and \(endStr)."
        }

        let totalExpense = txs.filter { !$0.income }.reduce(0.0) { $0 + $1.amount }
        let totalIncome = txs.filter { $0.income }.reduce(0.0) { $0 + $1.amount }

        let lines: [String] = txs.compactMap { tx in
            guard let date = tx.date else { return nil }
            let dateStr = df.string(from: date)
            let sign = tx.income ? "+" : "-"
            let cat = tx.category?.name ?? "Uncategorized"
            let note = (tx.note?.isEmpty == false) ? tx.note! : "(no note)"
            return "\(dateStr) | \(sign)RM\(String(format: "%.2f", tx.amount)) | \(note) | \(cat)"
        }

        return """
        \(txs.count) transactions (\(startStr) → \(endStr)):
        Total expenses: RM\(String(format: "%.2f", totalExpense))
        Total income: RM\(String(format: "%.2f", totalIncome))

        \(lines.joined(separator: "\n"))
        """
    }

    // MARK: - System prompt (lightweight — no transaction dump)

    private func buildSystemPrompt() -> String {
        var expenseList = "None"
        var incomeList = "None"

        if let dc = dataController {
            let context = dc.container.viewContext
            let catRequest = NSFetchRequest<Category>(entityName: "Category")
            catRequest.sortDescriptors = [NSSortDescriptor(key: "name", ascending: true)]
            let categories = (try? context.fetch(catRequest)) ?? []

            if !categories.isEmpty {
                let expenseNames = categories.filter { !$0.income }.compactMap { $0.name }
                let incomeNames = categories.filter { $0.income }.compactMap { $0.name }
                expenseList = expenseNames.isEmpty ? "None" : expenseNames.map { "• \($0)" }.joined(separator: "\n")
                incomeList = incomeNames.isEmpty ? "None" : incomeNames.map { "• \($0)" }.joined(separator: "\n")
            }
        }

        let dateStr = DateFormatter.localizedString(from: .now, dateStyle: .full, timeStyle: .none)

        return """
        You are Renvo AI, a smart financial assistant inside the Dime expense tracking app (Malaysia, currency MYR / RM).

        Today: \(dateStr)

        EXPENSE categories (copy exact string, pick closest match):
        \(expenseList)

        INCOME categories (copy exact string, pick closest match):
        \(incomeList)

        You have access to a fetch_transactions tool. Use it whenever the user asks about spending, income, or transaction history for ANY time period — this month, last month, last year, a specific date, or any range. Do NOT make up numbers; always fetch real data first.

        When the user pastes ANY text or shares ANY image — receipts, screenshots, bank statements, PDFs, WhatsApp messages, invoices — parse ALL financial transactions and log them automatically. Extract dates from the source; do not default to today unless no date is present.

        Always respond with valid JSON only, exactly:
        {
          "thinking": "2-4 sentence chain-of-thought: what you see, how you determined income vs expense, which category fits and why",
          "reply": "friendly message confirming what you logged, or answering their question",
          "actions": [
            {
              "type": "create_transaction",
              "amount": 12.50,
              "note": "Coffee at Starbucks",
              "category_name": "Food & Drink",
              "income": false,
              "date": "YYYY-MM-DD",
              "repeat_type": 0,
              "repeat_coefficient": 1
            }
          ],
          "visual": { ... }
        }

        Rules:
        - thinking: always include — briefly explain your interpretation of the input, the income/expense decision, and category choice. Max 4 sentences.
        - reply: confirm every transaction logged by name and amount, or answer the financial question naturally in English
        - actions: array of create_transaction objects, empty [] if just answering a question
        - date: "YYYY-MM-DDTHH:mm" if time is visible on the receipt/statement, otherwise "YYYY-MM-DD". Never default to midnight — if time is unknown, omit the time component entirely.
        - amount: always a positive number regardless of sign in source
        - Extract ALL transactions from pasted content or images even if many
        - repeat_type: 0=one-time (default), 1=daily, 2=weekly, 3=monthly — set when user says "every month", "weekly", "recurring", "subscription", etc.
        - repeat_coefficient: the interval number (e.g. "every 2 weeks" → repeat_type=2, repeat_coefficient=2). Default 1. Omit if not recurring.

        INCOME vs EXPENSE — set income: true or false based on money direction:
        - income: true → money received: salary, allowance, bonus, refund, cashback, dividend, interest, "CR", "Credit", "credited", transfer IN, top-up received
        - income: false → money going out: purchase, payment, bill, fee, subscription, withdrawal, "DR", "Debit", "debited", transfer OUT, any spending
        - In bank statements: Debit / DR column = expense (income: false). Credit / CR column = income (income: true).
        - Malaysian bank statements (Maybank, RHB, etc.) show the sign AFTER the amount: "170.00+" = credit = income: true. "200.00-" = debit = income: false. This trailing +/- is the definitive signal.
        - A leading negative sign (-RM) on an amount = expense. A leading positive or no sign = check CR/DR label.
        - When unsure, default to income: false (expense).

        CATEGORY — CRITICAL: category_name MUST be copied verbatim from the lists above. Do NOT invent names.
        - Pick the closest match from the correct list (expense vs income). If no perfect match exists, pick whichever listed category fits best.
        - Common mapping signals: Grab/Uber/LRT/bus/petrol/Shell → Transport-like; McDonald's/restaurant/cafe/mamak/food/drinks → Food-like; Netflix/Spotify/cinema/game → Entertainment-like; clinic/pharmacy/hospital → Health-like; salary/gaji/allowance/bonus → Income; electricity/Unifi/telco/phone bill → Utilities-like; supermarket/Tesco/Giant/grocery → Groceries-like; rent/house/sewa/rumah/mortgage/housing → Housing/Bills/Home-like
        - Return the exact string — no emoji, no suffix like "(expense)" or "(income)"
        - Never leave category_name blank; always pick the nearest option

        Visual card rules — include "visual" when it adds value:

        1. When you log 1+ transactions, include:
           "visual": { "type": "transactions_logged" }

        2. When answering a spending/income/balance question, include:
           "visual": {
             "type": "summary",
             "total_expense": <number>,
             "total_income": <number>,
             "count": <number of transactions in that period>,
             "period": "<e.g. this month, this week, today>",
             "comparison_pct": <optional signed number — negative means spending decreased vs prior period>
           }

        3. When the user asks about recurring payments, subscriptions, or you detect a pattern, include:
           "visual": {
             "type": "recurring",
             "items": [
               { "name": "Netflix", "amount": 15.90, "income": false, "frequency": "monthly" },
               { "name": "Salary", "amount": 5000, "income": true, "frequency": "monthly" }
             ]
           }

        - Omit "visual" entirely when not relevant (e.g. simple one-line answers).
        - Never include "visual" for error messages.
        """
    }

    // MARK: - Execute AI Actions

    private func executeActions(_ actions: [AITransactionAction]) {
        guard let dc = dataController, !actions.isEmpty else { return }

        let context = dc.container.viewContext
        let catRequest = NSFetchRequest<Category>(entityName: "Category")
        let allCategories = (try? context.fetch(catRequest)) ?? []

        for action in actions {
            let category = matchCategory(name: action.categoryName, income: action.income, from: allCategories)
            _ = dc.newTransaction(
                note: action.note,
                category: category,
                income: action.income,
                amount: action.amount,
                date: action.date,
                repeatType: action.repeatType,
                repeatCoefficient: action.repeatCoefficient,
                delay: false
            )
        }
    }

    private func matchCategory(name: String, income: Bool, from categories: [Category]) -> Category? {
        let stripped = name
            .replacingOccurrences(of: "\\(.*?\\)", with: "", options: .regularExpression)
            .replacingOccurrences(of: "[^\\p{L}\\p{N} &'/-]", with: "", options: .regularExpression)
            .trimmingCharacters(in: .whitespaces)
            .lowercased()

        func n(_ cat: Category) -> String { cat.name?.lowercased() ?? "" }

        // 1. Exact
        if let m = categories.first(where: { n($0) == stripped }) { return m }
        // 2. Category name contains search term
        if let m = categories.first(where: { n($0).contains(stripped) && !stripped.isEmpty }) { return m }
        // 3. Search term contains category name
        if let m = categories.first(where: { stripped.contains(n($0)) && !n($0).isEmpty }) { return m }

        // 4. Word-overlap scoring (prefer same income type)
        let searchWords = Set(stripped.split(separator: " ").map(String.init).filter { $0.count > 2 })
        if !searchWords.isEmpty {
            let sameType = categories.filter { $0.income == income }
            var bestScore = 0
            var bestCat: Category? = nil
            for cat in sameType {
                let catWords = Set(n(cat).split(separator: " ").map(String.init).filter { $0.count > 2 })
                let overlap = searchWords.intersection(catWords).count
                if overlap > bestScore { bestScore = overlap; bestCat = cat }
            }
            if let best = bestCat { return best }
        }

        // 5. Fallback: first category of matching income type
        return categories.first { $0.income == income }
    }
}
