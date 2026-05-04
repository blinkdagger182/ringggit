//
//  ChartTimeFrame.swift
//  Bonsai
//
//  Created by Rafael Soh on 3/6/22.
//

import Foundation

enum ChartTimeFrame: String, CaseIterable {
    case week = "this week"
    case lastMonth = "last 30 days"
    case lastSixMonths = "last 6 months"
    case lastTwelveMonths = "last 12 months"
    case thisMonth = "this month"
    case thisYear = "this year"

    static var allCases: [ChartTimeFrame] {
        return [.lastMonth, .lastSixMonths, .lastTwelveMonths, .thisMonth, .thisYear]
    }
}
