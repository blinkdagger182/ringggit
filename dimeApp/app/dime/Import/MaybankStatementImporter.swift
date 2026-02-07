//
//  MaybankStatementImporter.swift
//  dime
//
//  Created by Codex on 26/1/26.
//

import Foundation

struct MaybankStatementImportResult {
    let transactions: [MaybankStatementTransaction]
}

enum MaybankStatementImportError: Error {
    case invalidFile
    case network(statusCode: Int, message: String)
    case malformedResponse
    case noTransactions
}

final class MaybankStatementImporter {
    private static let fallbackBaseURL = "https://ringggit-mae-pdf-api-szzk9.ondigitalocean.app"
    private static let fallbackMode = "m2u_current_account_debit"

    static func importTransactions(from url: URL) async throws -> MaybankStatementImportResult {
        guard let fileData = try? Data(contentsOf: url), !fileData.isEmpty else {
            throw MaybankStatementImportError.invalidFile
        }

        let baseURLString = (Bundle.main.object(forInfoDictionaryKey: "MaybankImportAPIBaseURL") as? String) ?? fallbackBaseURL

        guard let endpointURL = URL(string: baseURLString)?.appendingPathComponent("process") else {
            throw MaybankStatementImportError.malformedResponse
        }

        let filename = url.lastPathComponent.isEmpty ? "statement.pdf" : url.lastPathComponent
        let configuredMode = (Bundle.main.object(forInfoDictionaryKey: "MaybankImportMode") as? String) ?? fallbackMode
        let candidateModes = orderedUniqueModes([
            configuredMode,
            fallbackMode,
            "maybank_debit",
            "m2u_current_account_debit",
            "maybank_credit"
        ])

        var lastNetworkError: MaybankStatementImportError?

        for mode in candidateModes {
            do {
                let data = try await processRequest(
                    endpointURL: endpointURL,
                    mode: mode,
                    fileData: fileData,
                    filename: filename
                )

                let rows = try extractRows(from: data)
                let transactions = rows.compactMap(mapRowToTransaction)

                if !transactions.isEmpty {
                    return MaybankStatementImportResult(transactions: transactions)
                }
            } catch let error as MaybankStatementImportError {
                if case .network = error {
                    lastNetworkError = error
                }
            } catch {
                continue
            }
        }

        if let lastNetworkError {
            throw lastNetworkError
        }

        throw MaybankStatementImportError.noTransactions
    }

    private static func processRequest(
        endpointURL: URL,
        mode: String,
        fileData: Data,
        filename: String
    ) async throws -> Data {
        let boundary = "Boundary-\(UUID().uuidString)"
        var request = URLRequest(url: endpointURL)
        request.httpMethod = "POST"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.httpBody = buildMultipartBody(
            boundary: boundary,
            mode: mode,
            responseFormat: "json",
            fileData: fileData,
            filename: filename
        )

        let (data, response) = try await URLSession.shared.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw MaybankStatementImportError.malformedResponse
        }

        guard (200 ... 299).contains(httpResponse.statusCode) else {
            let responseBody = String(data: data, encoding: .utf8) ?? "<non-utf8 response body>"
            print("[MaybankImport] HTTP \(httpResponse.statusCode) for mode=\(mode). Raw body: \(responseBody)")
            let message = parseErrorMessage(from: data)
            throw MaybankStatementImportError.network(statusCode: httpResponse.statusCode, message: message)
        }

        return data
    }

    private static func orderedUniqueModes(_ modes: [String]) -> [String] {
        var seen = Set<String>()
        var ordered = [String]()

        for mode in modes {
            let trimmed = mode.trimmingCharacters(in: .whitespacesAndNewlines)
            if trimmed.isEmpty || seen.contains(trimmed) {
                continue
            }
            seen.insert(trimmed)
            ordered.append(trimmed)
        }

        return ordered
    }

    private static func buildMultipartBody(
        boundary: String,
        mode: String,
        responseFormat: String,
        fileData: Data,
        filename: String
    ) -> Data {
        var body = Data()

        func append(_ string: String) {
            body.append(string.data(using: .utf8) ?? Data())
        }

        append("--\(boundary)\r\n")
        append("Content-Disposition: form-data; name=\"mode\"\r\n\r\n")
        append("\(mode)\r\n")

        append("--\(boundary)\r\n")
        append("Content-Disposition: form-data; name=\"response_format\"\r\n\r\n")
        append("\(responseFormat)\r\n")

        append("--\(boundary)\r\n")
        append("Content-Disposition: form-data; name=\"files\"; filename=\"\(filename)\"\r\n")
        append("Content-Type: application/pdf\r\n\r\n")
        body.append(fileData)
        append("\r\n")

        append("--\(boundary)--\r\n")
        return body
    }

    private static func parseErrorMessage(from data: Data) -> String {
        guard
            let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            return "Server request failed"
        }

        if let detail = object["detail"] as? String, !detail.isEmpty {
            return detail
        }

        if let detailObject = object["detail"] {
            if let detailData = try? JSONSerialization.data(withJSONObject: detailObject),
               let detailText = String(data: detailData, encoding: .utf8),
               !detailText.isEmpty {
                return detailText
            }
        }

        if let message = object["message"] as? String, !message.isEmpty {
            if let errors = object["errors"] as? [[String: Any]],
               let first = errors.first,
               let detail = first["error"] as? String,
               !detail.isEmpty {
                return "\(message) \(detail)"
            }
            return message
        }

        return "Server request failed"
    }

    private static func extractRows(from data: Data) throws -> [[String: Any]] {
        guard
            let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
            let rows = object["rows"] as? [[String: Any]]
        else {
            throw MaybankStatementImportError.malformedResponse
        }

        return rows
    }

    private static func mapRowToTransaction(_ row: [String: Any]) -> MaybankStatementTransaction? {
        guard let date = parseDate(from: row) else {
            return nil
        }

        var amountInfo = parseAmountAndDirection(from: row)

        if amountInfo == nil,
           let creditValue = numericValue(for: ["Amount (CR)"], in: row), creditValue > 0 {
            amountInfo = (creditValue, true)
        }

        if amountInfo == nil,
           let debitValue = numericValue(for: ["Amount (DR)"], in: row), debitValue > 0 {
            amountInfo = (debitValue, false)
        }

        guard let resolvedAmount = amountInfo else {
            return nil
        }

        let description = stringValue(
            for: [
                "Transaction Description",
                "description",
                "Description",
                "Transaction Description2",
                "Transaction Type/Description",
                "Transaction Type"
            ],
            in: row
        )?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "Maybank Transaction"

        let reference = stringValue(
            for: [
                "Recipient Reference",
                "reference",
                "RefNum"
            ],
            in: row
        )

        return MaybankStatementTransaction(
            date: date,
            description: description.isEmpty ? "Maybank Transaction" : description,
            amount: abs(resolvedAmount.amount),
            isCredit: resolvedAmount.isCredit,
            accountSource: "Maybank API",
            reference: reference
        )
    }

    private static func parseDate(from row: [String: Any]) -> Date? {
        let candidateKeys = ["Entry Date", "Date", "date", "entry_date", "entry_date_raw"]

        guard let raw = stringValue(for: candidateKeys, in: row) else {
            return nil
        }

        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return nil
        }

        let isoFormatter = ISO8601DateFormatter()
        isoFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = isoFormatter.date(from: trimmed) {
            return date
        }
        isoFormatter.formatOptions = [.withInternetDateTime]
        if let date = isoFormatter.date(from: trimmed) {
            return date
        }

        let dateFormats = [
            "yyyy-MM-dd",
            "dd/MM/yy",
            "dd/MM/yyyy",
            "dd-MM-yy",
            "dd-MM-yyyy"
        ]

        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)

        for format in dateFormats {
            formatter.dateFormat = format
            if let date = formatter.date(from: trimmed) {
                return date
            }
        }

        return nil
    }

    private static func parseAmountAndDirection(from row: [String: Any]) -> (amount: Double, isCredit: Bool)? {
        if let numericAmount = numericValue(for: ["Transaction Amount", "Amount", "amount"], in: row),
           numericAmount > 0 {
            if let flow = stringValue(for: ["flow", "output"], in: row)?.lowercased() {
                if flow.contains("inflow") || flow.contains("deposit") || flow.contains("credit") {
                    return (numericAmount, true)
                }
                if flow.contains("outflow") || flow.contains("withdrawal") || flow.contains("debit") {
                    return (numericAmount, false)
                }
            }

            if let boolCredit = row["isCredit"] as? Bool {
                return (numericAmount, boolCredit)
            }

            return (numericAmount, false)
        }

        let raw = stringValue(
            for: [
                "Transaction Amount",
                "Amount",
                "amount"
            ],
            in: row
        )

        if let raw {
            let normalized = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            if !normalized.isEmpty {
                let signHint = normalized.last == "+" ? true : (normalized.last == "-" ? false : nil)
                let cleaned = normalized
                    .replacingOccurrences(of: ",", with: "")
                    .replacingOccurrences(of: "+", with: "")
                    .replacingOccurrences(of: "-", with: "")

                if let value = Double(cleaned) {
                    if let signHint {
                        return (value, signHint)
                    }

                    if let flow = stringValue(for: ["flow", "output"], in: row)?.lowercased() {
                        if flow.contains("inflow") || flow.contains("deposit") || flow.contains("credit") {
                            return (value, true)
                        }
                        if flow.contains("outflow") || flow.contains("withdrawal") || flow.contains("debit") {
                            return (value, false)
                        }
                    }

                    if let boolCredit = row["isCredit"] as? Bool {
                        return (value, boolCredit)
                    }

                    return (value, false)
                }
            }
        }

        if let amount = numericValue(for: ["amount"], in: row), let boolCredit = row["isCredit"] as? Bool {
            return (amount, boolCredit)
        }

        return nil
    }

    private static func stringValue(for keys: [String], in row: [String: Any]) -> String? {
        for key in keys {
            if let value = row[key] as? String {
                return value
            }
        }

        let lowered = Dictionary(uniqueKeysWithValues: row.map { ($0.key.lowercased(), $0.value) })
        for key in keys {
            if let value = lowered[key.lowercased()] as? String {
                return value
            }
        }

        return nil
    }

    private static func numericValue(for keys: [String], in row: [String: Any]) -> Double? {
        for key in keys {
            if let value = row[key] as? Double {
                return value
            }
            if let value = row[key] as? Int {
                return Double(value)
            }
            if let value = row[key] as? String {
                let cleaned = value.replacingOccurrences(of: ",", with: "")
                if let output = Double(cleaned) {
                    return output
                }
            }
        }

        let lowered = Dictionary(uniqueKeysWithValues: row.map { ($0.key.lowercased(), $0.value) })
        for key in keys {
            if let value = lowered[key.lowercased()] as? Double {
                return value
            }
            if let value = lowered[key.lowercased()] as? Int {
                return Double(value)
            }
            if let value = lowered[key.lowercased()] as? String {
                let cleaned = value.replacingOccurrences(of: ",", with: "")
                if let output = Double(cleaned) {
                    return output
                }
            }
        }

        return nil
    }
}
