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
    let attachmentThumbnails: [UIImage]

    init(role: Role, text: String, date: Date, attachmentThumbnails: [UIImage] = []) {
        self.role = role
        self.text = text
        self.date = date
        self.attachmentThumbnails = attachmentThumbnails
    }
}

extension HomeAIMessage: Equatable {
    static func == (lhs: HomeAIMessage, rhs: HomeAIMessage) -> Bool {
        lhs.id == rhs.id
    }
}
