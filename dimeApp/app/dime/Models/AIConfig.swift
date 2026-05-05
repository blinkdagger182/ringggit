//
//  AIConfig.swift
//  dime
//

import Foundation

enum AIConfig {
    // Paste your OpenAI API key here
    static let openAIKey = "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"
    static let model = "gpt-4o-mini"
    static let visionModel = "gpt-4o"
    static let endpoint = URL(string: "https://api.openai.com/v1/chat/completions")!
}
