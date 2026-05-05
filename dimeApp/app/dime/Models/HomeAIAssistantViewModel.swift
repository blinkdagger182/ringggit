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
    let thumbnail: UIImage  // shown in UI
    let pages: [UIImage]    // all images sent to API (1 for photos, N for PDF pages)
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
        guard !trimmed.isEmpty || !pendingAttachments.isEmpty, !isResponding else { return }

        let messageText = trimmed.isEmpty ? "Please extract all transactions from the attached image(s)." : trimmed
        let capturedAttachments = pendingAttachments

        appendUserMessage(messageText, attachments: capturedAttachments)
        draftMessage = ""
        pendingAttachments = []
        isResponding = true

        Task {
            do {
                let imageDataList = capturedAttachments
                    .flatMap { $0.pages }
                    .compactMap { $0.jpegData(compressionQuality: 0.65) }
                let systemPrompt = buildSystemPrompt()
                let response = try await AIService.send(
                    systemPrompt: systemPrompt,
                    conversationMessages: messages,
                    images: imageDataList
                )
                executeActions(response.actions)
                messages.append(HomeAIMessage(role: .assistant, text: response.reply, date: .now))
            } catch AIServiceError.apiError(let msg) {
                messages.append(HomeAIMessage(role: .assistant, text: "API error: \(msg)", date: .now))
            } catch let urlError as URLError where urlError.code == .timedOut {
                messages.append(HomeAIMessage(role: .assistant, text: "Request timed out — the file is large. Try a shorter document or fewer pages.", date: .now))
            } catch {
                messages.append(HomeAIMessage(role: .assistant, text: "Couldn't connect. Check your API key and internet connection.", date: .now))
            }
            isResponding = false
        }
    }

    private func appendUserMessage(_ text: String, attachments: [AttachmentItem] = []) {
        messages.append(HomeAIMessage(role: .user, text: text, date: .now, attachments: attachments))
    }

    // MARK: - AI Context

    private func buildSystemPrompt() -> String {
        var categoriesText = "None configured"
        var spendingSummary = ""

        if let dc = dataController {
            let context = dc.container.viewContext

            let catRequest = NSFetchRequest<Category>(entityName: "Category")
            catRequest.sortDescriptors = [NSSortDescriptor(key: "name", ascending: true)]
            let categories = (try? context.fetch(catRequest)) ?? []

            if !categories.isEmpty {
                categoriesText = categories.map { cat in
                    let type = cat.income ? "income" : "expense"
                    let emoji = cat.emoji ?? ""
                    let name = cat.name ?? "Unknown"
                    return "\(emoji) \(name) (\(type))"
                }.joined(separator: ", ")
            }

            let calendar = Calendar.current
            if let startOfMonth = calendar.date(from: calendar.dateComponents([.year, .month], from: .now)) {
                let txRequest = NSFetchRequest<Transaction>(entityName: "Transaction")
                txRequest.predicate = NSPredicate(format: "date >= %@", startOfMonth as NSDate)
                let txs = (try? context.fetch(txRequest)) ?? []

                let totalExpenses = txs.filter { !$0.income }.reduce(0.0) { $0 + $1.amount }
                let totalIncome = txs.filter { $0.income }.reduce(0.0) { $0 + $1.amount }
                spendingSummary = "This month so far: RM\(String(format: "%.2f", totalExpenses)) expenses, RM\(String(format: "%.2f", totalIncome)) income, \(txs.count) transactions."
            }
        }

        let dateStr = DateFormatter.localizedString(from: .now, dateStyle: .full, timeStyle: .none)

        return """
        You are Renvo AI, a smart financial assistant inside the Dime expense tracking app (Malaysia, currency MYR / RM).

        Today: \(dateStr)
        Available categories: \(categoriesText)
        \(spendingSummary)

        When the user pastes ANY text or shares ANY image — receipts, screenshots, bank statements, PDFs rendered as images, WhatsApp messages, invoices — parse ALL financial transactions and log them automatically. Extract dates from the source material; do not default to today unless no date is present.

        Always respond with valid JSON only, exactly:
        {
          "reply": "friendly message confirming what you logged, or answering their question",
          "actions": [
            {
              "type": "create_transaction",
              "amount": 12.50,
              "note": "Coffee at Starbucks",
              "category_name": "Food & Drink",
              "income": false,
              "date": "YYYY-MM-DD"
            }
          ]
        }

        Rules:
        - reply: confirm every transaction logged by name and amount, or answer the financial question naturally in English
        - actions: array of create_transaction objects, empty [] if just answering a question
        - category_name: must match one of the available category names exactly (use closest match)
        - income: false for expenses/purchases, true for salary/transfers received/cashback
        - date: "YYYY-MM-DDTHH:mm" if time is visible on the receipt/statement, otherwise "YYYY-MM-DD". Never default to midnight — if time is unknown, omit the time component entirely.
        - amount: always a positive number
        - Extract ALL transactions from pasted content or images even if many
        - If a category is unclear, pick the closest available one
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
                repeatType: 0,
                repeatCoefficient: 1,
                delay: false
            )
        }
    }

    private func matchCategory(name: String, income: Bool, from categories: [Category]) -> Category? {
        let lower = name.lowercased()
        return categories.first { $0.name?.lowercased() == lower }
            ?? categories.first { $0.name?.lowercased().contains(lower) == true }
            ?? categories.first { lower.contains($0.name?.lowercased() ?? "") && !($0.name?.isEmpty ?? true) }
            ?? categories.first { $0.income == income }
    }
}
