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
