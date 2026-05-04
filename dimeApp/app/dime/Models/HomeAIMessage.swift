//
//  HomeAIMessage.swift
//  dime
//

import Foundation

struct HomeAIMessage: Identifiable, Equatable {
    enum Role {
        case user
        case assistant
    }

    let id = UUID()
    let role: Role
    let text: String
    let date: Date
}
