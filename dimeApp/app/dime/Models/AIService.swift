//
//  AIService.swift
//  dime
//

import Foundation

// MARK: - Visual card types

struct AIVisualSummary {
    let totalExpense: Double
    let totalIncome: Double
    let count: Int
    let period: String
    let comparisonPct: Double?  // negative = spending went down (good)
}

struct AIRecurringItem {
    let name: String
    let amount: Double
    let income: Bool
    let frequency: String
}

enum AIVisualCard {
    case summary(AIVisualSummary)
    case transactionsLogged([AITransactionAction])
    case recurring([AIRecurringItem])
}

// MARK: - Response / action types

struct AIResponse {
    let reply: String
    let thinking: String?
    let actions: [AITransactionAction]
    let visual: AIVisualCard?
}

struct AITransactionAction {
    let amount: Double
    let note: String
    let categoryName: String
    let income: Bool
    let date: Date
    let repeatType: Int        // 0=none 1=daily 2=weekly 3=monthly
    let repeatCoefficient: Int // interval (e.g. every 2 weeks = type 2, coeff 2)
}

enum AIServiceError: Error {
    case invalidResponse
    case invalidJSON
    case apiError(String)
}

// MARK: - Service

struct AIService {
    static func send(
        systemPrompt: String,
        conversationMessages: [HomeAIMessage],
        images: [Data] = [],
        sourceText: String = "",
        toolExecutor: ((_ name: String, _ args: [String: Any]) async -> String)? = nil
    ) async throws -> AIResponse {
        let useVision = !images.isEmpty
        let model = useVision ? AIConfig.visionModel : AIConfig.model

        var apiMessages: [[String: Any]] = [
            ["role": "system", "content": systemPrompt]
        ]

        let historyMessages = conversationMessages.dropLast()
        for msg in historyMessages {
            apiMessages.append([
                "role": msg.role == .user ? "user" : "assistant",
                "content": msg.text
            ])
        }

        if let lastMsg = conversationMessages.last {
            let textContent = lastMsg.text + sourceText
            if useVision && lastMsg.role == .user {
                var contentParts: [[String: Any]] = [
                    ["type": "text", "text": textContent]
                ]
                for imageData in images {
                    let base64 = imageData.base64EncodedString()
                    contentParts.append([
                        "type": "image_url",
                        "image_url": [
                            "url": "data:image/jpeg;base64,\(base64)",
                            "detail": "high"
                        ]
                    ])
                }
                apiMessages.append(["role": "user", "content": contentParts])
            } else {
                apiMessages.append([
                    "role": lastMsg.role == .user ? "user" : "assistant",
                    "content": textContent
                ])
            }
        }

        // Build request body — include tools only for text queries (not vision)
        var body: [String: Any] = [
            "model": model,
            "messages": apiMessages,
            "response_format": ["type": "json_object"],
            "temperature": 0.1
        ]

        if toolExecutor != nil && !useVision {
            body["tools"] = makeTools()
            body["tool_choice"] = "auto"
        }

        // First API call
        let (data, _) = try await performRequest(body: body)

        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let choices = json["choices"] as? [[String: Any]],
              let firstChoice = choices.first,
              let responseMessage = firstChoice["message"] as? [String: Any] else {
            throw AIServiceError.invalidResponse
        }

        // Check if AI wants to call tools
        let finishReason = firstChoice["finish_reason"] as? String
        if finishReason == "tool_calls",
           let toolCalls = responseMessage["tool_calls"] as? [[String: Any]],
           !toolCalls.isEmpty,
           let executor = toolExecutor {

            // Execute EVERY tool call — leaving any unanswered causes API error
            var toolResponses: [[String: Any]] = []
            for call in toolCalls {
                guard let fn = call["function"] as? [String: Any],
                      let toolCallId = call["id"] as? String,
                      let fnName = fn["name"] as? String,
                      let argsStr = fn["arguments"] as? String,
                      let argsData = argsStr.data(using: .utf8),
                      let args = try? JSONSerialization.jsonObject(with: argsData) as? [String: Any]
                else { continue }

                let result = await executor(fnName, args)
                toolResponses.append(["role": "tool", "tool_call_id": toolCallId, "content": result])
            }

            // Second request: original messages + assistant tool_calls + all tool results
            var messages2 = apiMessages
            messages2.append(["role": "assistant", "content": NSNull(), "tool_calls": toolCalls])
            messages2.append(contentsOf: toolResponses)

            let body2: [String: Any] = [
                "model": model,
                "messages": messages2,
                "response_format": ["type": "json_object"],
                "temperature": 0.1
            ]

            let (data2, _) = try await performRequest(body: body2)

            guard let json2 = try? JSONSerialization.jsonObject(with: data2) as? [String: Any],
                  let choices2 = json2["choices"] as? [[String: Any]],
                  let msg2 = choices2.first?["message"] as? [String: Any],
                  let content2 = msg2["content"] as? String else {
                throw AIServiceError.invalidResponse
            }

            return try parseContent(content2)
        }

        // No tool call — direct JSON response
        guard let content = responseMessage["content"] as? String else {
            throw AIServiceError.invalidResponse
        }
        return try parseContent(content)
    }

    // MARK: - Tool definition

    private static func makeTools() -> [[String: Any]] {
        [[
            "type": "function",
            "function": [
                "name": "fetch_transactions",
                "description": "Retrieve transaction records for a date range. ONLY call for history queries (e.g. 'what did I spend last month?', 'show last year'). Do NOT call when the user is logging a new transaction.",
                "parameters": [
                    "type": "object",
                    "properties": [
                        "start_date": [
                            "type": "string",
                            "description": "Start date inclusive, YYYY-MM-DD"
                        ],
                        "end_date": [
                            "type": "string",
                            "description": "End date inclusive, YYYY-MM-DD"
                        ]
                    ],
                    "required": ["start_date", "end_date"]
                ]
            ]
        ]]
    }

    // MARK: - HTTP helper

    private static func performRequest(body: [String: Any]) async throws -> (Data, URLResponse) {
        var request = URLRequest(url: AIConfig.endpoint)
        request.httpMethod = "POST"
        request.setValue("Bearer \(AIConfig.openAIKey)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        request.timeoutInterval = 120

        let (data, response) = try await URLSession.shared.data(for: request)

        if let http = response as? HTTPURLResponse, http.statusCode != 200 {
            let errorBody = (try? JSONSerialization.jsonObject(with: data) as? [String: Any])?["error"] as? [String: Any]
            let message = errorBody?["message"] as? String ?? "HTTP \(http.statusCode)"
            throw AIServiceError.apiError(message)
        }

        return (data, response)
    }

    // MARK: - Parse

    private static func parseContent(_ content: String) throws -> AIResponse {
        guard let data = content.data(using: .utf8),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw AIServiceError.invalidJSON
        }

        let reply = json["reply"] as? String ?? "Done."
        let thinking = json["thinking"] as? String
        var actions: [AITransactionAction] = []

        if let actionsArray = json["actions"] as? [[String: Any]] {
            let datetimeFormatter = DateFormatter()
            datetimeFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm"
            datetimeFormatter.locale = Locale(identifier: "en_US_POSIX")

            let dateOnlyFormatter = DateFormatter()
            dateOnlyFormatter.dateFormat = "yyyy-MM-dd"
            dateOnlyFormatter.locale = Locale(identifier: "en_US_POSIX")

            for action in actionsArray {
                guard (action["type"] as? String) == "create_transaction" else { continue }
                let note = action["note"] as? String ?? ""
                let categoryName = action["category_name"] as? String ?? ""

                let amount: Double
                let income: Bool
                if let resolved = deterministicAmountAndIncome(from: action) {
                    amount = resolved.amount
                    income = resolved.income
                } else if let signed = numericValue(action["signed_amount"]) {
                    income = signed > 0
                    amount = abs(signed)
                } else if let raw = numericValue(action["amount"]),
                          let aiIncome = action["income"] as? Bool {
                    // fallback: legacy fields
                    income = aiIncome
                    amount = abs(raw)
                } else {
                    continue
                }

                let date: Date
                if let dateStr = action["date"] as? String {
                    if let parsed = datetimeFormatter.date(from: dateStr) {
                        date = parsed
                    } else if let parsedDay = dateOnlyFormatter.date(from: dateStr) {
                        let now = Date()
                        let cal = Calendar.current
                        let timeComponents = cal.dateComponents([.hour, .minute, .second], from: now)
                        date = cal.date(byAdding: timeComponents, to: parsedDay) ?? parsedDay
                    } else {
                        date = .now
                    }
                } else {
                    date = .now
                }

                let repeatType = (action["repeat_type"] as? Int) ?? 0
                let repeatCoefficient = max(1, (action["repeat_coefficient"] as? Int) ?? 1)

                actions.append(AITransactionAction(
                    amount: abs(amount),
                    note: note,
                    categoryName: categoryName,
                    income: income,
                    date: date,
                    repeatType: repeatType,
                    repeatCoefficient: repeatCoefficient
                ))
            }
        }

        var visual: AIVisualCard? = nil
        if let visualObj = json["visual"] as? [String: Any],
           let type = visualObj["type"] as? String {
            switch type {
            case "summary":
                let expense = (visualObj["total_expense"] as? Double)
                    ?? (visualObj["total_expense"] as? Int).map(Double.init) ?? 0
                let income = (visualObj["total_income"] as? Double)
                    ?? (visualObj["total_income"] as? Int).map(Double.init) ?? 0
                let count = (visualObj["count"] as? Int) ?? actions.count
                let period = (visualObj["period"] as? String) ?? "this period"
                let pct = visualObj["comparison_pct"] as? Double
                visual = .summary(AIVisualSummary(
                    totalExpense: expense,
                    totalIncome: income,
                    count: count,
                    period: period,
                    comparisonPct: pct
                ))
            case "transactions_logged":
                if !actions.isEmpty {
                    visual = .transactionsLogged(actions)
                }
            case "recurring":
                if let items = visualObj["items"] as? [[String: Any]] {
                    let recurringItems: [AIRecurringItem] = items.compactMap { item in
                        guard let name = item["name"] as? String,
                              let amount = (item["amount"] as? Double)
                                  ?? (item["amount"] as? Int).map(Double.init)
                        else { return nil }
                        return AIRecurringItem(
                            name: name,
                            amount: abs(amount),
                            income: item["income"] as? Bool ?? false,
                            frequency: item["frequency"] as? String ?? "monthly"
                        )
                    }
                    if !recurringItems.isEmpty { visual = .recurring(recurringItems) }
                }
            default:
                break
            }
        }

        return AIResponse(reply: reply, thinking: thinking, actions: actions, visual: visual)
    }

    private static func deterministicAmountAndIncome(from action: [String: Any]) -> (amount: Double, income: Bool)? {
        let rawAmount = stringValue(
            action["raw_amount_text"]
                ?? action["raw_amount"]
                ?? action["amount_text"]
        )
        let rawRow = stringValue(
            action["raw_row_text"]
                ?? action["row_text"]
                ?? action["source_row"]
        )

        let explicitAmount = numericValue(action["amount"])
            ?? numericValue(action["signed_amount"]).map(abs)
            ?? parseStatementNumber(rawAmount).map(abs)
            ?? parseSignedAmountFromRow(rawRow)?.amount

        if let amount = explicitAmount,
           let income = directionFromRawAmount(rawAmount, matchingAmount: amount)
            ?? directionFromRawRow(rawRow, matchingAmount: amount)
            ?? directionFromColumns(action) {
            return (amount, income)
        }

        if let amount = explicitAmount,
           let income = directionFromBalanceDelta(action) {
            return (amount, income)
        }

        if let signed = numericValue(action["signed_amount"]) {
            return (abs(signed), signed > 0)
        }

        return nil
    }

    private static func directionFromRawAmount(_ raw: String?, matchingAmount amount: Double) -> Bool? {
        guard let raw else { return nil }
        let normalized = normalizeMinus(raw)
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .uppercased()
        guard normalized.rangeOfCharacter(from: .decimalDigits) != nil else { return nil }

        if normalized.hasSuffix("+") { return true }
        if normalized.hasSuffix("-") { return false }
        if normalized.hasPrefix("("), normalized.hasSuffix(")") { return false }
        if normalized.hasSuffix(" DR") || normalized.hasSuffix("DR") { return false }
        if normalized.hasSuffix(" CR") || normalized.hasSuffix("CR") { return true }

        if let signed = parseSignedAmountFromRow(normalized, matchingAmount: amount) {
            return signed.income
        }

        return nil
    }

    private static func directionFromRawRow(_ raw: String?, matchingAmount amount: Double) -> Bool? {
        parseSignedAmountFromRow(raw, matchingAmount: amount)?.income
    }

    private static func directionFromColumns(_ action: [String: Any]) -> Bool? {
        let debitKeys = ["raw_debit_text", "debit_text", "withdrawal_text", "debit_amount", "withdrawal_amount"]
        let creditKeys = ["raw_credit_text", "credit_text", "deposit_text", "credit_amount", "deposit_amount"]

        let hasDebit = debitKeys.contains { key in
            hasMeaningfulValue(action[key])
        }
        let hasCredit = creditKeys.contains { key in
            hasMeaningfulValue(action[key])
        }

        if hasDebit && !hasCredit { return false }
        if hasCredit && !hasDebit { return true }
        return nil
    }

    private static func directionFromBalanceDelta(_ action: [String: Any]) -> Bool? {
        let previous = parseStatementNumber(
            action["raw_previous_balance_text"]
                ?? action["previous_balance_text"]
                ?? action["balance_before"]
                ?? action["previous_balance"]
        )
        let current = parseStatementNumber(
            action["raw_balance_text"]
                ?? action["balance_text"]
                ?? action["balance_after"]
                ?? action["current_balance"]
        )

        guard let previous, let current else { return nil }
        let delta = current - previous
        guard abs(delta) > 0.005 else { return nil }
        return delta > 0
    }

    private static func parseSignedAmountFromRow(_ raw: String?, matchingAmount amount: Double? = nil) -> (amount: Double, income: Bool)? {
        guard let raw else { return nil }
        let normalized = normalizeMinus(raw)
        let pattern = #"(?<![A-Za-z0-9])(?:RM|MYR)?\s*\(?\d[\d,]*(?:\.\d{2})?\)?\s*[+-]|(?<![A-Za-z0-9])(?:RM|MYR)?\s*\(\s*\d[\d,]*(?:\.\d{2})?\s*\)"#

        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
            return nil
        }

        let nsRange = NSRange(normalized.startIndex..<normalized.endIndex, in: normalized)
        let matches = regex.matches(in: normalized, range: nsRange)
        let candidates: [(amount: Double, income: Bool)] = matches.compactMap { match in
            guard let range = Range(match.range, in: normalized) else { return nil }
            let token = String(normalized[range])
            guard let amount = parseStatementNumber(token)?.magnitude else { return nil }

            let trimmed = token.trimmingCharacters(in: .whitespacesAndNewlines)
            let income = trimmed.hasSuffix("+") ? true : false
            return (amount, income)
        }

        guard !candidates.isEmpty else { return nil }

        if let amount {
            let tolerance = max(0.01, amount * 0.0001)
            if let exact = candidates.first(where: { abs($0.amount - amount) <= tolerance }) {
                return exact
            }
        }

        return candidates.last
    }

    private static func parseStatementNumber(_ value: Any?) -> Double? {
        if let value = numericValue(value) {
            return value
        }

        guard var raw = stringValue(value) else { return nil }
        raw = normalizeMinus(raw)
            .uppercased()
            .replacingOccurrences(of: "MYR", with: "")
            .replacingOccurrences(of: "RM", with: "")
            .replacingOccurrences(of: ",", with: "")
            .trimmingCharacters(in: .whitespacesAndNewlines)

        let isNegative = raw.hasSuffix("-") || raw.hasSuffix("DR") || (raw.hasPrefix("(") && raw.hasSuffix(")"))
        raw = raw
            .replacingOccurrences(of: "+", with: "")
            .replacingOccurrences(of: "-", with: "")
            .replacingOccurrences(of: "CR", with: "")
            .replacingOccurrences(of: "DR", with: "")
            .replacingOccurrences(of: "(", with: "")
            .replacingOccurrences(of: ")", with: "")
            .trimmingCharacters(in: .whitespacesAndNewlines)

        guard let number = Double(raw) else { return nil }
        return isNegative ? -number : number
    }

    private static func numericValue(_ value: Any?) -> Double? {
        switch value {
        case let double as Double:
            return double
        case let int as Int:
            return Double(int)
        case let number as NSNumber:
            return number.doubleValue
        case let string as String:
            return Double(string.replacingOccurrences(of: ",", with: ""))
        default:
            return nil
        }
    }

    private static func stringValue(_ value: Any?) -> String? {
        switch value {
        case let string as String:
            let trimmed = string.trimmingCharacters(in: .whitespacesAndNewlines)
            return trimmed.isEmpty ? nil : trimmed
        case let number as NSNumber:
            return number.stringValue
        default:
            return nil
        }
    }

    private static func hasMeaningfulValue(_ value: Any?) -> Bool {
        if let number = numericValue(value) {
            return abs(number) > 0.005
        }
        if let string = stringValue(value) {
            let cleaned = string
                .replacingOccurrences(of: "-", with: "")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            return !cleaned.isEmpty
        }
        return false
    }

    private static func normalizeMinus(_ value: String) -> String {
        value
            .replacingOccurrences(of: "−", with: "-")
            .replacingOccurrences(of: "–", with: "-")
            .replacingOccurrences(of: "—", with: "-")
    }
}
