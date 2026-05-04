//
//  HomeAIAssistantViewModel.swift
//  dime
//

import Foundation
import SwiftUI
import UIKit

@MainActor
final class HomeAIAssistantViewModel: ObservableObject {
    @Published var isPresented = false
    @Published var draftMessage = ""
    @Published private(set) var messages: [HomeAIMessage] = []
    @Published private(set) var isResponding = false

    let quickActions: [HomeAIQuickAction] = [
        HomeAIQuickAction(title: "Add transaction", prompt: "Add transaction", systemImage: "plus.circle.fill"),
        HomeAIQuickAction(title: "Where did my money go?", prompt: "Where did my money go?", systemImage: "chart.pie.fill"),
        HomeAIQuickAction(title: "Show this month's spending", prompt: "Show this month's spending", systemImage: "calendar"),
        HomeAIQuickAction(title: "Can I afford this?", prompt: "Can I afford this?", systemImage: "checkmark.shield.fill")
    ]

    var quickHint: String {
        messages.isEmpty ? "Swipe down to open your finance assistant" : "Continue your last finance question"
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

    func triggerQuickAction(_ quickAction: HomeAIQuickAction) {
        draftMessage = quickAction.prompt
        sendDraftMessage()
    }

    func sendDraftMessage() {
        let trimmed = draftMessage.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, !isResponding else { return }

        appendUserMessage(trimmed)
        draftMessage = ""
        isResponding = true

        Task {
            try? await Task.sleep(nanoseconds: 350_000_000)
            let response = mockResponse(for: trimmed)
            messages.append(HomeAIMessage(role: .assistant, text: response, date: .now))
            isResponding = false
        }
    }

    private func appendUserMessage(_ text: String) {
        messages.append(HomeAIMessage(role: .user, text: text, date: .now))
    }

    private func mockResponse(for prompt: String) -> String {
        let normalizedPrompt = prompt.lowercased()

        if normalizedPrompt.contains("add transaction") {
            return "I can help you log a new expense or income. The next step is to capture the amount, note, date, and category, then save it."
        }

        if normalizedPrompt.contains("where did my money go") {
            return "I’d summarize your biggest categories first, then highlight unusual spending spikes and recurring charges once live transaction analysis is connected."
        }

        if normalizedPrompt.contains("this month") || normalizedPrompt.contains("month's spending") {
            return "I’m ready to surface this month’s spending summary here. The UI is wired for local mock responses now, and backend insights can plug into this view model later."
        }

        if normalizedPrompt.contains("afford") {
            return "I can turn this into an affordability check by comparing the purchase against your current budget, recurring bills, and recent cash flow."
        }

        return "I can help explain spending patterns, suggest budget actions, and draft the next step for your finances once the live AI service is connected."
    }
}
