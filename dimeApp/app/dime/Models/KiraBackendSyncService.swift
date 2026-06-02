import Foundation

struct KiraRemoteTransaction: Decodable {
    let id: String
    let receiptId: String?
    let source: String
    let description: String
    let amount: Double
    let currency: String
    let transactionDate: String
    let category: String?
    let updatedAt: String
}

struct KiraTransactionSyncResponse: Decodable {
    let transactions: [KiraRemoteTransaction]
    let nextCursor: String?
}

enum KiraBackendSyncError: Error {
    case invalidURL
    case invalidResponse
}

struct KiraBackendSyncService {
    static let baseURL = URL(string: "https://withkira.app")!

    static func fetchTransactions(accessToken: String, since: String?) async throws -> KiraTransactionSyncResponse {
        var components = URLComponents(url: baseURL.appendingPathComponent("/api/sync/transactions"), resolvingAgainstBaseURL: false)

        if let since, !since.isEmpty {
            components?.queryItems = [
                URLQueryItem(name: "since", value: since)
            ]
        }

        guard let url = components?.url else {
            throw KiraBackendSyncError.invalidURL
        }

        var request = URLRequest(url: url)
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await URLSession.shared.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse,
              (200 ..< 300).contains(httpResponse.statusCode) else {
            throw KiraBackendSyncError.invalidResponse
        }

        return try JSONDecoder().decode(KiraTransactionSyncResponse.self, from: data)
    }
}
