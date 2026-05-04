//
//  HomeAIQuickAction.swift
//  dime
//

import Foundation

struct HomeAIQuickAction: Identifiable, Equatable {
    let id = UUID()
    let title: String
    let prompt: String
    let systemImage: String
}
