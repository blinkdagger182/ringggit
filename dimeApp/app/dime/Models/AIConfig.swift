//
//  AIConfig.swift
//  dime
//

import Foundation

enum AIConfig {
    // Paste your OpenAI API key here
    static let openAIKey = "skeyhere"
    static let model = "gpt-4o-mini"
    static let visionModel = "gpt-4o"
    static let endpoint = URL(string: "https://api.openai.com/v1/chat/completions")!
}
