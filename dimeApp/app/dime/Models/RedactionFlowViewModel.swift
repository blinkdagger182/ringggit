//
//  RedactionFlowViewModel.swift
//  dime
//

import SwiftUI
import UIKit

// MARK: - Manual redaction box

struct ManualRedactionBox: Identifiable {
    let id: UUID
    let pageIndex: Int
    let boundingBox: CGRect  // normalized UIKit coords 0–1

    init(pageIndex: Int, boundingBox: CGRect) {
        self.id = UUID()
        self.pageIndex = pageIndex
        self.boundingBox = boundingBox
    }
}

// MARK: - ViewModel

@MainActor
final class RedactionFlowViewModel: ObservableObject {

    enum FlowState { case idle, loading, ready, error }

    @Published var showFlow   = false
    @Published var showIntro  = false
    @Published private(set) var flowState: FlowState = .idle
    @Published private(set) var redactionItems: [RedactionItem] = []
    @Published var manualBoxes: [ManualRedactionBox] = []

    private(set) var sourcePages:  [UIImage] = []
    private(set) var sourceText:   String?
    private(set) var ocrText:      String?
    private(set) var filename:     String = "Document"

    private let service = PIIRedactionService()

    var hasManualBoxes: Bool { !manualBoxes.isEmpty }

    // MARK: - Public interface

    func startRedaction(pages: [UIImage], filename: String, sourceText: String?) {
        self.sourcePages  = pages
        self.filename     = filename
        self.sourceText   = sourceText
        self.ocrText      = nil
        self.redactionItems = []
        self.manualBoxes  = []
        self.flowState    = .idle

        showFlow = true
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) { [weak self] in
            self?.showIntro = true
        }
    }

    func markIntroSeen() {
        showIntro = false
        beginDetection()
    }

    func cancelFlow() {
        showIntro  = false
        showFlow   = false
        flowState  = .idle
        manualBoxes = []
    }

    func retryDetection() { beginDetection() }

    // MARK: - Manual boxes

    func addManualBox(_ rect: CGRect, pageIndex: Int) {
        manualBoxes.append(ManualRedactionBox(pageIndex: pageIndex, boundingBox: rect))
    }

    func removeManualBox(_ id: UUID) {
        manualBoxes.removeAll { $0.id == id }
    }

    func removeAutoRedaction(_ id: UUID) {
        redactionItems.removeAll { $0.id == id }
    }

    func removeLastManualBox() {
        _ = manualBoxes.popLast()
    }

    // MARK: - Generate

    func generateRedactedCopy() async -> [UIImage] {
        var result: [UIImage] = []
        for (index, page) in sourcePages.enumerated() {
            let redacted = await service.applyRedactions(
                redactionItems,
                manualBoxes: manualBoxes,
                to: page,
                pageIndex: index
            )
            result.append(redacted)
        }
        return result
    }

    // MARK: - Detection

    private func beginDetection() {
        flowState = .loading
        Task {
            do {
                var allItems: [RedactionItem] = []
                var allText = ""
                for (index, page) in sourcePages.enumerated() {
                    let (found, pageText) = try await service.detectPII(in: page, pageIndex: index)
                    allItems.append(contentsOf: found)
                    if !pageText.isEmpty {
                        allText += "Page \(index + 1):\n\(pageText)\n\n"
                    }
                }
                redactionItems = allItems
                if sourceText == nil {
                    ocrText = allText.trimmingCharacters(in: .whitespacesAndNewlines)
                }
                flowState = .ready
            } catch {
                flowState = .error
            }
        }
    }
}
