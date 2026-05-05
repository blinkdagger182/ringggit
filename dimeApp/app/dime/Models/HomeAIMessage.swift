//
//  HomeAIMessage.swift
//  dime
//

import Foundation
import UIKit

struct HomeAIMessage: Identifiable {
    enum Role {
        case user
        case assistant
    }

    let id = UUID()
    let role: Role
    let text: String
    let date: Date
    let attachments: [AttachmentItem]
    let visual: AIVisualCard?

    init(role: Role, text: String, date: Date, attachments: [AttachmentItem] = [], visual: AIVisualCard? = nil) {
        self.role = role
        self.text = text
        self.date = date
        self.attachments = attachments
        self.visual = visual
    }
}

extension HomeAIMessage: Equatable {
    static func == (lhs: HomeAIMessage, rhs: HomeAIMessage) -> Bool {
        lhs.id == rhs.id
    }
}
