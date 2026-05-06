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

    let id: UUID
    let role: Role
    var text: String
    var date: Date
    var attachments: [AttachmentItem]
    var visual: AIVisualCard?
    var thinking: String?

    init(
        id: UUID = UUID(),
        role: Role,
        text: String,
        date: Date,
        attachments: [AttachmentItem] = [],
        visual: AIVisualCard? = nil,
        thinking: String? = nil
    ) {
        self.id = id
        self.role = role
        self.text = text
        self.date = date
        self.attachments = attachments
        self.visual = visual
        self.thinking = thinking
    }
}

extension HomeAIMessage: Equatable {
    static func == (lhs: HomeAIMessage, rhs: HomeAIMessage) -> Bool {
        lhs.id == rhs.id
    }
}
