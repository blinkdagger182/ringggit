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
            if useVision && lastMsg.role == .user {
                var contentParts: [[String: Any]] = [
                    ["type": "text", "text": lastMsg.text]
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
                    "content": lastMsg.text
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

        // Check if AI wants to call a tool
        let finishReason = firstChoice["finish_reason"] as? String
        if finishReason == "tool_calls",
           let toolCalls = responseMessage["tool_calls"] as? [[String: Any]],
           let firstCall = toolCalls.first,
           let fn = firstCall["function"] as? [String: Any],
           let toolCallId = firstCall["id"] as? String,
           let fnName = fn["name"] as? String,
           let argsStr = fn["arguments"] as? String,
           let argsData = argsStr.data(using: .utf8),
           let args = try? JSONSerialization.jsonObject(with: argsData) as? [String: Any],
           let executor = toolExecutor {

            // Execute the tool (runs on main actor via the closure)
            let toolResult = await executor(fnName, args)

            // Second request: original messages + assistant tool_call + tool result
            var messages2 = apiMessages
            messages2.append(["role": "assistant", "content": NSNull(), "tool_calls": toolCalls])
            messages2.append(["role": "tool", "tool_call_id": toolCallId, "content": toolResult])

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
                "description": "Retrieve the user's transaction records for a date range. Call this for ANY question about spending, income, or financial history — past or recent.",
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
        var actions: [AITransactionAction] = []

        if let actionsArray = json["actions"] as? [[String: Any]] {
            let datetimeFormatter = DateFormatter()
            datetimeFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm"
            datetimeFormatter.locale = Locale(identifier: "en_US_POSIX")

            let dateOnlyFormatter = DateFormatter()
            dateOnlyFormatter.dateFormat = "yyyy-MM-dd"
            dateOnlyFormatter.locale = Locale(identifier: "en_US_POSIX")

            for action in actionsArray {
                guard (action["type"] as? String) == "create_transaction",
                      let amount = (action["amount"] as? Double) ?? (action["amount"] as? Int).map(Double.init),
                      let note = action["note"] as? String,
                      let categoryName = action["category_name"] as? String,
                      let income = action["income"] as? Bool else { continue }

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

        return AIResponse(reply: reply, actions: actions, visual: visual)
    }
}
