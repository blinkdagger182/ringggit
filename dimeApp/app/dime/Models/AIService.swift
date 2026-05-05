//
//  AIService.swift
//  dime
//

import Foundation

struct AIResponse {
    let reply: String
    let actions: [AITransactionAction]
}

struct AITransactionAction {
    let amount: Double
    let note: String
    let categoryName: String
    let income: Bool
    let date: Date
}

enum AIServiceError: Error {
    case invalidResponse
    case invalidJSON
    case apiError(String)
}

struct AIService {
    static func send(
        systemPrompt: String,
        conversationMessages: [HomeAIMessage],
        images: [Data] = []
    ) async throws -> AIResponse {
        let useVision = !images.isEmpty
        let model = useVision ? AIConfig.visionModel : AIConfig.model

        var messages: [[String: Any]] = [
            ["role": "system", "content": systemPrompt]
        ]

        // History — all but the last user message
        let historyMessages = conversationMessages.dropLast()
        for msg in historyMessages {
            messages.append([
                "role": msg.role == .user ? "user" : "assistant",
                "content": msg.text
            ])
        }

        // Last user message — attach images if present
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
                messages.append(["role": "user", "content": contentParts])
            } else {
                messages.append([
                    "role": lastMsg.role == .user ? "user" : "assistant",
                    "content": lastMsg.text
                ])
            }
        }

        let body: [String: Any] = [
            "model": model,
            "messages": messages,
            "response_format": ["type": "json_object"],
            "temperature": 0.1
        ]

        var request = URLRequest(url: AIConfig.endpoint)
        request.httpMethod = "POST"
        request.setValue("Bearer \(AIConfig.openAIKey)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        request.timeoutInterval = 120

        let (data, response): (Data, URLResponse) = try await URLSession.shared.data(for: request)

        if let http = response as? HTTPURLResponse, http.statusCode != 200 {
            let errorBody = (try? JSONSerialization.jsonObject(with: data) as? [String: Any])?["error"] as? [String: Any]
            let message = errorBody?["message"] as? String ?? "HTTP \(http.statusCode)"
            throw AIServiceError.apiError(message)
        }

        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let choices = json["choices"] as? [[String: Any]],
              let firstChoice = choices.first,
              let message = firstChoice["message"] as? [String: Any],
              let content = message["content"] as? String else {
            throw AIServiceError.invalidResponse
        }

        return try parseContent(content)
    }

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
                        // AI provided full datetime — use as-is
                        date = parsed
                    } else if let parsedDay = dateOnlyFormatter.date(from: dateStr) {
                        // AI provided date only — inject current time-of-day so it doesn't land at midnight
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

                actions.append(AITransactionAction(
                    amount: abs(amount),
                    note: note,
                    categoryName: categoryName,
                    income: income,
                    date: date
                ))
            }
        }

        return AIResponse(reply: reply, actions: actions)
    }
}
