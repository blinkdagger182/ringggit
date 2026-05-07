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
    let sourceText: String?

    init(image: UIImage, label: String) {
        self.thumbnail = image
        self.pages = [image]
        self.label = label
        self.sourceText = nil
    }

    init(pdfPages: [UIImage], filename: String, sourceText: String? = nil) {
        self.thumbnail = pdfPages.first ?? UIImage()
        self.pages = pdfPages
        let count = pdfPages.count
        self.label = "\(filename) · \(count) \(count == 1 ? "page" : "pages")"
        self.sourceText = sourceText
    }
}

struct AIConversationWorkspace: Identifiable, Equatable {
    let id: String
    let title: String
    let bucketID: UUID?

    static let general = AIConversationWorkspace(id: "general", title: "General", bucketID: nil)
}

@MainActor
final class HomeAIAssistantViewModel: ObservableObject {
    private struct ExtractionPage {
        let image: UIImage
        let sourceText: String?
        let label: String
    }

    private struct ExtractionJob {
        let originalMessage: String
        let pages: [ExtractionPage]
        var nextPageIndex: Int
        var batchSize: Int
        var loggedActions: [AITransactionAction]
    }

    private struct PDFTransactionCandidate {
        let rawLine: String
        let amount: Double
        let income: Bool
        let date: Date
        let note: String
    }

    @Published var isPresented = false
    @Published var draftMessage = ""
    @Published private(set) var messages: [HomeAIMessage] = []
    @Published private(set) var workspaces: [AIConversationWorkspace] = [.general]
    @Published private(set) var activeWorkspaceID: String = AIConversationWorkspace.general.id
    @Published private(set) var isResponding = false
    @Published private(set) var isAnimatingReply = false
    @Published private(set) var statusText: String = ""
    @Published private(set) var streamingText: String = ""
    @Published private(set) var messageContentRevision = 0
    @Published var pendingAttachments: [AttachmentItem] = []

    var dataController: DataController?
    private var resumableExtractionJob: ExtractionJob?
    private var workspaceMessages: [String: [HomeAIMessage]] = [AIConversationWorkspace.general.id: []]

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
        !draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            || !pendingAttachments.isEmpty
            || resumableExtractionJob != nil
    }

    var isStreaming: Bool {
        isResponding || isAnimatingReply
    }

    var activeWorkspace: AIConversationWorkspace {
        workspaces.first(where: { $0.id == activeWorkspaceID }) ?? .general
    }

    var activeBucket: Bucket? {
        guard let bucketID = activeWorkspace.bucketID,
              let dc = dataController else { return nil }
        return dc.getBucket(id: bucketID)
    }

    func expand() {
        guard !isPresented else { return }
        reloadWorkspaces()
        isPresented = true
        UIImpactFeedbackGenerator(style: .soft).impactOccurred()
    }

    func collapse() {
        guard isPresented else { return }
        isPresented = false
        UIApplication.shared.endEditing()
        UIImpactFeedbackGenerator(style: .light).impactOccurred()
    }

    func reloadWorkspaces() {
        var updated: [AIConversationWorkspace] = [.general]

        if let dc = dataController {
            let bucketWorkspaces = dc.getAllBuckets().sorted {
                ($0.name ?? "").localizedCaseInsensitiveCompare($1.name ?? "") == .orderedAscending
            }.map { bucket in
                AIConversationWorkspace(
                    id: "bucket-\(bucket.id?.uuidString ?? UUID().uuidString)",
                    title: bucket.name ?? "Untitled Bucket",
                    bucketID: bucket.id
                )
            }
            updated.append(contentsOf: bucketWorkspaces)
        }

        workspaces = updated
        updated.forEach { workspaceMessages[$0.id] = workspaceMessages[$0.id] ?? [] }

        let resolvedWorkspaceID = updated.contains(where: { $0.id == activeWorkspaceID })
            ? activeWorkspaceID
            : AIConversationWorkspace.general.id
        activeWorkspaceID = resolvedWorkspaceID
        messages = workspaceMessages[resolvedWorkspaceID] ?? []
        messageContentRevision &+= 1
    }

    func selectWorkspace(_ id: String) {
        let resolvedWorkspaceID = workspaces.contains(where: { $0.id == id })
            ? id
            : AIConversationWorkspace.general.id
        activeWorkspaceID = resolvedWorkspaceID
        messages = workspaceMessages[resolvedWorkspaceID] ?? []
        streamingText = ""
        statusText = ""
        messageContentRevision &+= 1
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
        let hasResumableExtraction = pendingAttachments.isEmpty && resumableExtractionJob != nil
        guard !trimmed.isEmpty || !pendingAttachments.isEmpty || hasResumableExtraction,
              !isResponding, !isAnimatingReply else { return }

        let messageText = trimmed.isEmpty
            ? (hasResumableExtraction ? "Continue extraction" : "Please extract all transactions from the attached image(s).")
            : trimmed
        let capturedAttachments = pendingAttachments
        let capturedAttachmentReference = capturedAttachments.first.flatMap { TransactionAttachmentStore.saveAIAttachment($0)?.serializedReference }

        appendUserMessage(messageText, attachments: capturedAttachments)
        let assistantMessageID = appendAssistantPlaceholder()
        draftMessage = ""
        pendingAttachments = []
        isResponding = true
        streamingText = ""

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
                let systemPrompt = buildSystemPrompt()
                if let existingJob = resumableExtractionJob,
                   capturedAttachments.isEmpty {
                    await processExtractionJob(existingJob, systemPrompt: systemPrompt, assistantMessageID: assistantMessageID, reference: nil)
                    return
                }

                if !capturedAttachments.isEmpty {
                    let pages = Self.makeExtractionPages(from: capturedAttachments)
                    guard !pages.isEmpty else {
                        completeAssistantMessage(
                            id: assistantMessageID,
                            text: "I couldn't read any pages from that attachment."
                        )
                        return
                    }

                    let job = ExtractionJob(
                        originalMessage: messageText,
                        pages: pages,
                        nextPageIndex: 0,
                        batchSize: Self.initialBatchSize(for: pages.count),
                        loggedActions: []
                    )
                    resumableExtractionJob = job
                    await processExtractionJob(job, systemPrompt: systemPrompt, assistantMessageID: assistantMessageID, reference: capturedAttachmentReference)
                    return
                }

                let response = try await AIService.send(
                    systemPrompt: systemPrompt,
                    conversationMessages: messages,
                    toolExecutor: toolExecutor
                )
                statusText = "Writing response"
                executeActions(response.actions, reference: capturedAttachmentReference)
                await animateReply(response, assistantMessageID: assistantMessageID)
                statusText = ""
            } catch AIServiceError.apiError(let msg) {
                completeAssistantMessage(id: assistantMessageID, text: "API error: \(msg)")
            } catch let urlError as URLError where urlError.code == .timedOut {
                completeAssistantMessage(
                    id: assistantMessageID,
                    text: "Request timed out — the file is large. Try a shorter document or fewer pages."
                )
            } catch {
                completeAssistantMessage(
                    id: assistantMessageID,
                    text: "Couldn't connect. Check your API key and internet connection."
                )
            }
        }
    }

    private func processExtractionJob(
        _ initialJob: ExtractionJob,
        systemPrompt: String,
        assistantMessageID: UUID,
        reference: String?
    ) async {
        var job = initialJob
        var loggedActions = job.loggedActions

        do {
            while job.nextPageIndex < job.pages.count {
                let startIndex = job.nextPageIndex
                let endIndex = min(startIndex + job.batchSize, job.pages.count)
                let batchPages = Array(job.pages[startIndex..<endIndex])
                let pageRangeText = startIndex + 1 == endIndex
                    ? "page \(startIndex + 1)"
                    : "pages \(startIndex + 1)-\(endIndex)"
                statusText = "Extracting \(pageRangeText) of \(job.pages.count)"

                let batchImages = batchPages.compactMap { $0.image.jpegData(compressionQuality: 0.88) }
                let batchSourceText = Self.combinedSourceText(from: batchPages)
                let batchRawText = Self.rawSourceText(from: batchPages)
                let batchPrompt = """
                \(job.originalMessage)

                Process only \(pageRangeText) of this attachment. Return transactions visible in this batch only. If a row is continued from the previous page, use the visible text evidence and do not invent missing values.
                """
                let batchMessages = [HomeAIMessage(role: .user, text: batchPrompt, date: .now)]

                let response = try await AIService.send(
                    systemPrompt: systemPrompt,
                    conversationMessages: batchMessages,
                    images: batchImages,
                    sourceText: batchSourceText
                )

                let reconciledActions = Self.reconciledActions(
                    aiActions: response.actions,
                    candidates: Self.transactionCandidates(in: batchRawText)
                )
                executeActions(reconciledActions, reference: reference)
                loggedActions.append(contentsOf: reconciledActions)
                job.loggedActions = loggedActions
                job.nextPageIndex = endIndex
                resumableExtractionJob = job
            }

            resumableExtractionJob = nil
            statusText = "Writing response"
            let response = AIResponse(
                reply: "Logged \(loggedActions.count) transactions from \(job.pages.count) \(job.pages.count == 1 ? "page" : "pages").",
                thinking: "Processed in page batches; saved each successful batch before moving to the next.",
                actions: [],
                visual: loggedActions.isEmpty ? nil : .transactionsLogged(loggedActions)
            )
            await animateReply(response, assistantMessageID: assistantMessageID)
            statusText = ""
        } catch let urlError as URLError where urlError.code == .timedOut {
            job.batchSize = 1
            resumableExtractionJob = job
            completeAssistantMessage(
                id: assistantMessageID,
                text: "Timed out on page \(job.nextPageIndex + 1). I saved completed pages already. Send “continue” and I’ll resume from page \(job.nextPageIndex + 1) with one page per request."
            )
        } catch AIServiceError.apiError(let msg) {
            resumableExtractionJob = job
            completeAssistantMessage(
                id: assistantMessageID,
                text: "API error on page \(job.nextPageIndex + 1): \(msg). Completed pages were saved. Send “continue” to retry from page \(job.nextPageIndex + 1)."
            )
        } catch {
            resumableExtractionJob = job
            completeAssistantMessage(
                id: assistantMessageID,
                text: "Stopped on page \(job.nextPageIndex + 1). Completed pages were saved. Send “continue” to retry from page \(job.nextPageIndex + 1)."
            )
        }
    }

    private static func combinedSourceText(from attachments: [AttachmentItem]) -> String {
        combinedSourceText(from: makeExtractionPages(from: attachments))
    }

    private static func combinedSourceText(from pages: [ExtractionPage]) -> String {
        let textBlocks = sourceTextBlocks(from: pages)

        guard !textBlocks.isEmpty else { return "" }
        let fullText = textBlocks.joined(separator: "\n\n--- NEXT PDF ---\n\n")
        let candidateLines = detectedSignedTransactionLines(in: fullText)
        let candidateSection = candidateLines.isEmpty
            ? ""
            : """

        Detected signed transaction lines from PDF text. Treat every line below as a transaction candidate unless it is clearly a statement total/header:
        \(candidateLines.joined(separator: "\n"))

        """

        return """

        Extracted PDF text evidence:
        \(candidateSection)
        Full extracted PDF text:
        \(fullText)
        """
    }

    private static func rawSourceText(from pages: [ExtractionPage]) -> String {
        sourceTextBlocks(from: pages).joined(separator: "\n\n")
    }

    private static func sourceTextBlocks(from pages: [ExtractionPage]) -> [String] {
        pages
            .compactMap(\.sourceText)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }

    private static func reconciledActions(
        aiActions: [AITransactionAction],
        candidates: [PDFTransactionCandidate]
    ) -> [AITransactionAction] {
        guard !candidates.isEmpty else { return aiActions }

        var remainingAICounts = countedKeys(for: aiActions)
        var missing: [AITransactionAction] = []

        for candidate in candidates {
            let key = transactionKey(
                date: candidate.date,
                amount: candidate.amount,
                income: candidate.income
            )

            if let count = remainingAICounts[key], count > 0 {
                remainingAICounts[key] = count - 1
                continue
            }

            missing.append(AITransactionAction(
                amount: candidate.amount,
                note: candidate.note,
                categoryName: "",
                income: candidate.income,
                date: candidate.date,
                repeatType: 0,
                repeatCoefficient: 1
            ))
        }

        return aiActions + missing
    }

    private static func countedKeys(for actions: [AITransactionAction]) -> [String: Int] {
        actions.reduce(into: [String: Int]()) { result, action in
            let key = transactionKey(date: action.date, amount: action.amount, income: action.income)
            result[key, default: 0] += 1
        }
    }

    private static func transactionKey(date: Date, amount: Double, income: Bool) -> String {
        let day = Calendar.current.startOfDay(for: date).timeIntervalSince1970
        let cents = Int((amount * 100).rounded())
        return "\(Int(day))|\(income ? "in" : "out")|\(cents)"
    }

    private static func transactionCandidates(in text: String) -> [PDFTransactionCandidate] {
        let datePattern = #"(\b\d{2}/\d{2}/\d{2}\b)"#
        let signedAmountPattern = #"(\b\d[\d,]*(?:\.\d{2})\s*[+\-−–—])"#
        guard let lineRegex = try? NSRegularExpression(pattern: "\(datePattern)(.+?)\(signedAmountPattern)", options: []),
              let amountRegex = try? NSRegularExpression(pattern: signedAmountPattern) else {
            return []
        }

        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "dd/MM/yy"
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")

        var candidates: [PDFTransactionCandidate] = []
        for line in text.components(separatedBy: .newlines) {
            let normalizedLine = normalizeStatementLine(line)
            guard !normalizedLine.isEmpty else { continue }

            let range = NSRange(normalizedLine.startIndex..<normalizedLine.endIndex, in: normalizedLine)
            guard let match = lineRegex.firstMatch(in: normalizedLine, range: range),
                  match.numberOfRanges >= 4,
                  let dateRange = Range(match.range(at: 1), in: normalizedLine),
                  let descriptionRange = Range(match.range(at: 2), in: normalizedLine),
                  let amountRange = Range(match.range(at: 3), in: normalizedLine),
                  let date = dateFormatter.date(from: String(normalizedLine[dateRange])),
                  let signedAmount = signedAmount(from: String(normalizedLine[amountRange])) else {
                continue
            }

            let leadingDescription = String(normalizedLine[..<dateRange.lowerBound])
            let trailingDescription = String(normalizedLine[descriptionRange])
                .replacingOccurrences(of: #"\s+"#, with: " ", options: .regularExpression)
                .trimmingCharacters(in: .whitespacesAndNewlines)
            let leadingNote = leadingDescription
                .replacingOccurrences(of: "|", with: " ")
                .replacingOccurrences(of: #"\s+"#, with: " ", options: .regularExpression)
                .trimmingCharacters(in: .whitespacesAndNewlines)
            let description = trailingDescription.isEmpty ? leadingNote : trailingDescription
            let note = description.isEmpty ? normalizedLine : description

            // Require the signed amount match to be from this row, not a statement header/footer.
            guard amountRegex.firstMatch(in: normalizedLine, range: range) != nil else { continue }

            candidates.append(PDFTransactionCandidate(
                rawLine: normalizedLine,
                amount: abs(signedAmount),
                income: signedAmount > 0,
                date: date,
                note: note
            ))
        }

        return candidates
    }

    private static func signedAmount(from raw: String) -> Double? {
        let normalized = raw
            .replacingOccurrences(of: "−", with: "-")
            .replacingOccurrences(of: "–", with: "-")
            .replacingOccurrences(of: "—", with: "-")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let income = normalized.hasSuffix("+")
        let expense = normalized.hasSuffix("-")
        guard income || expense else { return nil }

        let numeric = normalized
            .dropLast()
            .replacingOccurrences(of: ",", with: "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard let amount = Double(numeric) else { return nil }
        return income ? amount : -amount
    }

    private static func normalizeStatementLine(_ line: String) -> String {
        line
            .replacingOccurrences(of: "−", with: "-")
            .replacingOccurrences(of: "–", with: "-")
            .replacingOccurrences(of: "—", with: "-")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func makeExtractionPages(from attachments: [AttachmentItem]) -> [ExtractionPage] {
        attachments.flatMap { attachment in
            let pageTexts = splitPDFTextIntoPages(attachment.sourceText)
            return attachment.pages.enumerated().map { index, image in
                ExtractionPage(
                    image: image,
                    sourceText: pageTexts.indices.contains(index) ? pageTexts[index] : attachment.sourceText,
                    label: "\(attachment.label) page \(index + 1)"
                )
            }
        }
    }

    private static func splitPDFTextIntoPages(_ text: String?) -> [String] {
        guard let text, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return []
        }

        let lines = text.components(separatedBy: .newlines)
        let pageHeaderRegex = try? NSRegularExpression(pattern: #"^Page \d+:"#)
        var pages: [String] = []
        var current: [String] = []

        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            let range = NSRange(trimmed.startIndex..<trimmed.endIndex, in: trimmed)
            let isPageHeader = pageHeaderRegex?.firstMatch(in: trimmed, range: range) != nil

            if isPageHeader, !current.isEmpty {
                pages.append(current.joined(separator: "\n"))
                current = [line]
            } else {
                current.append(line)
            }
        }

        if !current.isEmpty {
            pages.append(current.joined(separator: "\n"))
        }

        return pages.isEmpty ? [text] : pages
    }

    private static func initialBatchSize(for pageCount: Int) -> Int {
        pageCount > 1 ? 2 : 1
    }

    private static func detectedSignedTransactionLines(in text: String) -> [String] {
        let datePattern = #"\b\d{2}/\d{2}/\d{2}\b"#
        let signedAmountPattern = #"\b\d[\d,]*(?:\.\d{2})[+\-−–—]\b|\b\d[\d,]*(?:\.\d{2})\s*[+\-−–—]"#
        guard let dateRegex = try? NSRegularExpression(pattern: datePattern),
              let amountRegex = try? NSRegularExpression(pattern: signedAmountPattern) else {
            return []
        }

        var candidates: [String] = []
        for line in text.components(separatedBy: .newlines) {
            let normalizedLine = line
                .replacingOccurrences(of: "−", with: "-")
                .replacingOccurrences(of: "–", with: "-")
                .replacingOccurrences(of: "—", with: "-")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            guard !normalizedLine.isEmpty else { continue }

            let range = NSRange(normalizedLine.startIndex..<normalizedLine.endIndex, in: normalizedLine)
            let hasDate = dateRegex.firstMatch(in: normalizedLine, range: range) != nil
            let hasSignedAmount = amountRegex.firstMatch(in: normalizedLine, range: range) != nil
            guard hasDate && hasSignedAmount else { continue }
            candidates.append("- \(normalizedLine)")
        }

        return Array(candidates.prefix(250))
    }

    // MARK: - Reply streaming

    private func animateReply(_ response: AIResponse, assistantMessageID: UUID) async {
        let reply = response.reply.isEmpty ? "Done." : response.reply

        isResponding = false
        isAnimatingReply = true
        statusText = ""
        streamingText = ""

        var built = ""
        for chunk in replyChunks(for: reply) {
            built += chunk
            updateAssistantMessage(id: assistantMessageID) { message in
                message.text = built
                message.visual = nil
                message.thinking = nil
            }
            streamingText = built
            try? await Task.sleep(nanoseconds: streamingDelay(for: chunk))
        }

        updateAssistantMessage(id: assistantMessageID) { message in
            message.text = built
            message.visual = response.visual
            message.thinking = response.thinking
        }
        streamingText = built
        isAnimatingReply = false
    }

    private func appendUserMessage(_ text: String, attachments: [AttachmentItem] = []) {
        messages.append(HomeAIMessage(role: .user, text: text, date: .now, attachments: attachments))
        syncActiveWorkspaceMessages()
    }

    private func appendAssistantPlaceholder() -> UUID {
        let message = HomeAIMessage(role: .assistant, text: "", date: .now)
        messages.append(message)
        syncActiveWorkspaceMessages()
        return message.id
    }

    private func completeAssistantMessage(
        id: UUID,
        text: String,
        thinking: String? = nil,
        visual: AIVisualCard? = nil
    ) {
        updateAssistantMessage(id: id) { message in
            message.text = text
            message.thinking = thinking
            message.visual = visual
        }
        streamingText = text
        isResponding = false
        isAnimatingReply = false
        statusText = ""
    }

    private func updateAssistantMessage(id: UUID, update: (inout HomeAIMessage) -> Void) {
        guard let index = messages.firstIndex(where: { $0.id == id }) else { return }
        var message = messages[index]
        update(&message)
        messages[index] = message
        syncActiveWorkspaceMessages()
    }

    private func syncActiveWorkspaceMessages() {
        workspaceMessages[activeWorkspaceID] = messages
        messageContentRevision &+= 1
    }

    private func replyChunks(for reply: String) -> [String] {
        var chunks: [String] = []
        var current = ""

        for character in reply {
            current.append(character)

            if character.isWhitespace || ".!,?:;\n".contains(character) || current.count >= 3 {
                chunks.append(current)
                current = ""
            }
        }

        if !current.isEmpty {
            chunks.append(current)
        }

        return chunks.isEmpty ? [reply] : chunks
    }

    private func streamingDelay(for chunk: String) -> UInt64 {
        if chunk.contains(where: \.isNewline) {
            return 55_000_000
        }

        if chunk.contains(where: { ".!,?:;".contains($0) }) {
            return 42_000_000
        }

        if chunk.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return 18_000_000
        }

        return 30_000_000
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
        if let bucket = activeBucket {
            req.predicate = NSPredicate(format: "date >= %@ AND date < %@ AND bucket == %@", startDate as NSDate, endDate as NSDate, bucket)
        } else {
            req.predicate = NSPredicate(format: "date >= %@ AND date < %@", startDate as NSDate, endDate as NSDate)
        }
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
            let bucketText = tx.bucket?.name.map { " | bucket: \($0)" } ?? ""
            return "\(dateStr) | \(sign)RM\(String(format: "%.2f", tx.amount)) | \(note) | \(cat)\(bucketText)"
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
        let bucketContext: String
        if let bucket = activeBucket {
            bucketContext = """

            Current bucket workspace: \(bucket.name ?? "Bucket").
            In this workspace, treat imported, created, and summarized transactions as part of this bucket unless the user explicitly says otherwise.
            When using fetch_transactions, focus on this bucket only.
            """
        } else {
            bucketContext = ""
        }

        return """
        You are Renvo AI, a smart financial assistant inside the Dime expense tracking app (Malaysia, currency MYR / RM).

        Today: \(dateStr)
        \(bucketContext)

        EXPENSE categories (copy exact string, pick closest match):
        \(expenseList)

        INCOME categories (copy exact string, pick closest match):
        \(incomeList)

        You have access to a fetch_transactions tool. Use it whenever the user asks about spending, income, or transaction history for ANY time period — this month, last month, last year, a specific date, or any range. Do NOT make up numbers; always fetch real data first.

        When the user pastes ANY text or shares ANY image — receipts, screenshots, bank statements, PDFs, WhatsApp messages, invoices — parse ALL financial transactions and log them automatically. Extract dates from the source; do not default to today unless no date is present.

        If "Extracted PDF text evidence" is present, treat it as the highest-priority source for exact amount signs and running balances. Use the image only to recover layout/continuations when the text wraps awkwardly. Never override a PDF text amount like "700.00-" with a visual guess.
        If "Detected signed transaction lines from PDF text" is present, scan that list first and create one action for every transaction candidate line. Do not stop at page boundaries; a date at the top of a new page is still a real transaction.

        Always respond with valid JSON only, exactly:
        {
          "thinking": "brief chain-of-thought: page count, total transactions found, balance check result",
          "reply": "friendly message confirming what you logged, or answering their question",
          "actions": [
            {
              "type": "create_transaction",
              "signed_amount": -12.50,
              "raw_amount_text": "12.50-",
              "raw_row_text": "2026-03-04 TRANSFER FROM A/C LIM QIU SHI 700.00- 41.33",
              "raw_debit_text": "",
              "raw_credit_text": "",
              "raw_previous_balance_text": "741.33",
              "raw_balance_text": "41.33",
              "note": "Coffee at Starbucks",
              "category_name": "Food & Drink",
              "date": "YYYY-MM-DD",
              "repeat_type": 0,
              "repeat_coefficient": 1
            }
          ],
          "visual": { ... }
        }

        CRITICAL — evidence-first extraction:
        - For every bank statement / receipt / PDF / image transaction, copy raw evidence fields exactly when visible.
        - raw_amount_text: exact amount cell text including trailing sign, e.g. "700.00-" or "300.00+".
        - raw_row_text: exact visible row text around the transaction including date, description, amount, and balance.
        - raw_debit_text / raw_credit_text: exact value from debit/withdrawal or credit/deposit columns when the statement uses separate columns. Use "" if not present.
        - raw_previous_balance_text: the running balance before this transaction, if visible/inferable from adjacent row. Use "" if not known.
        - raw_balance_text: the running balance after this transaction, if visible. Use "" if not present.
        - The app will use these raw fields to determine income/expense deterministically. Do not omit them for statement/image extraction.

        signed_amount rules:
        - signed_amount is a number. Negative = expense (money out). Positive = income (money in).
        - "700.00-" in statement → signed_amount: -700.00
        - "300.00+" in statement → signed_amount: 300.00
        - "1,172.00-" in statement → signed_amount: -1172.00
        - "2,800.00+" in statement → signed_amount: 2800.00
        - User says "coffee 12" → signed_amount: -12.00 (expense by default)
        - User says "salary 5000" → signed_amount: 5000.00 (income)
        - Always use the trailing +/- from the statement amount column to set the sign. Never infer sign from description words.

        MULTI-PAGE STATEMENT EXTRACTION — follow these rules exactly:

        STEP 1 — SCAN EVERY ROW. For each page, read every single row that has an amount value. Do not skip any row. A bank statement with 5 pages may have 50–100+ transactions — log them all.

        STEP 2 — HANDLE PAGE BREAKS. Bank statement pages often split or continue transactions across pages:
          • The last row on a page may show date + partial description with NO amount yet
          • The first rows on the next page may show the rest of the description + the amount
          • The first complete dated row on a new page may belong to the same calendar date as the previous page — include it.
          • Treat these as ONE transaction. Use the date and amount from whichever page shows them.
          • Rows at the top of a page with no date/no amount are ALWAYS continuations — never skip them.

        STEP 3 — NEVER DEDUPLICATE. The same merchant name, amount, and even the same date can appear multiple times as separate real transactions. Log every occurrence as its own transaction. Example: three "IBK FUND TFR TO A/C MUHAMMAD AZHAN RIZH* 300.00+" on different or same dates = three separate actions.

        STEP 4 — USE RUNNING BALANCE TO VERIFY. After extracting all rows, check: each transaction should move the running balance by exactly its amount in the correct direction. If the balance jumps unexpectedly, you missed a transaction — go back and find it.

        Other rules:
        - reply: confirm every transaction logged by name and amount, or answer the financial question naturally in English
        - actions: array of create_transaction objects, empty [] if just answering a question
        - date: "YYYY-MM-DDTHH:mm" if time is visible on the receipt/statement, otherwise "YYYY-MM-DD". Never default to midnight — if time is unknown, omit the time component entirely. For continuation rows, inherit the date from the row above.
        - repeat_type: 0=one-time (default), 1=daily, 2=weekly, 3=monthly — set when user says "every month", "weekly", "recurring", "subscription", etc.
        - repeat_coefficient: the interval number (e.g. "every 2 weeks" → repeat_type=2, repeat_coefficient=2). Default 1. Omit if not recurring.

        SIGNED AMOUNT SIGN — how to determine negative vs positive:

        METHOD A — BALANCE DELTA (use this whenever a running balance column is visible):
        For each transaction row that has a before-balance and after-balance:
          signed_amount = balance_after − balance_before
          If result is negative → expense (money out). If positive → income (money in).
        Example: balance was 741.33, now 41.33 → 41.33 − 741.33 = −700 → signed_amount: −700
        Example: balance was 41.33, now 341.33 → 341.33 − 41.33 = +300 → signed_amount: 300
        This is math — it is always correct regardless of description wording.
        Use the BEGINNING BALANCE row as the reference for the first transaction.

        METHOD B — TRAILING SIGN (use when no running balance column exists):
        Amount ending in "+" → positive. Amount ending in "−" → negative.
        "700.00−" → −700. "300.00+" → 300.
        Reference suffixes like "MBB CT−" "DUITNOW QR−" are NOT amount signs — ignore them.

        METHOD C — COLUMNS/LABELS (use when no trailing sign):
        Debit/Withdrawal column → negative. Credit/Deposit column → positive.
        DR/Debit label → negative. CR/Credit/REFUND label → positive.

        METHOD D — KEYWORDS (last resort, no other signal):
        Positive: salary, gaji, bonus, allowance, cashback, refund, dividend, interest
        Negative: purchase, payment, bill, fee, subscription, withdrawal, charge

        Default: negative.

        BALANCE VERIFICATION (always run for bank statements):
        After extracting all transactions, verify:
          expected_ending = beginning_balance + sum(positive signed_amounts) + sum(negative signed_amounts)
          If expected_ending ≠ actual ending balance shown (diff > 0.01):
            • You missed a transaction or got a sign wrong — find and fix it
          Always report in reply: "✓ Balance verified RM X → RM Y" or "⚠️ Off by RM Z"

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

    private func executeActions(_ actions: [AITransactionAction], reference: String? = nil) {
        guard let dc = dataController, !actions.isEmpty else { return }

        let context = dc.container.viewContext
        let catRequest = NSFetchRequest<Category>(entityName: "Category")
        let allCategories = (try? context.fetch(catRequest)) ?? []
        let bucket = activeBucket

        for action in actions {
            let category = matchCategory(name: action.categoryName, income: action.income, from: allCategories)
            let transaction = dc.newTransaction(
                note: action.note,
                category: category,
                income: action.income,
                amount: action.amount,
                date: action.date,
                repeatType: action.repeatType,
                repeatCoefficient: action.repeatCoefficient,
                delay: false
            )
            transaction.bucket = bucket
            transaction.reference = reference
        }
        dc.save()
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
