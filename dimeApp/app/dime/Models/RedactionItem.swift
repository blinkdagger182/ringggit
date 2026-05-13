//
//  RedactionItem.swift
//  dime
//

import Foundation
import CoreGraphics
import SwiftUI

enum RedactionType: String, CaseIterable, Identifiable {
    case name            = "Name"
    case accountNumber   = "Account / Card Number"
    case contactInfo     = "Contact Info"
    case referenceNumber = "Reference Number"
    case icPassport      = "IC / Passport"
    case email           = "Email"
    case address         = "Address"

    var id: String { rawValue }

    var systemIcon: String {
        switch self {
        case .name:            return "person.fill"
        case .accountNumber:   return "creditcard.fill"
        case .contactInfo:     return "phone.fill"
        case .referenceNumber: return "number.circle.fill"
        case .icPassport:      return "doc.text.fill"
        case .email:           return "envelope.fill"
        case .address:         return "mappin.fill"
        }
    }

    var accentColor: Color {
        switch self {
        case .name:            return .blue
        case .accountNumber:   return .orange
        case .contactInfo:     return .green
        case .referenceNumber: return Color(.systemGray)
        case .icPassport:      return .red
        case .email:           return .teal
        case .address:         return .purple
        }
    }

    /// Minimum confidence required to auto-enable this redaction type.
    /// Items below threshold appear under "Suggested Review" (disabled by default).
    var autoEnableThreshold: Double {
        switch self {
        case .email:           return 0.95
        case .contactInfo:     return 0.85
        case .icPassport:      return 0.95
        case .accountNumber:   return 0.80
        case .referenceNumber: return 0.70
        case .name:            return 0.75
        case .address:         return 0.75
        }
    }

    /// Merge priority — higher number wins when two detections overlap.
    /// More specific / less-ambiguous types outrank generic ones.
    var mergePriority: Int {
        switch self {
        case .email:           return 7
        case .contactInfo:     return 6
        case .accountNumber:   return 5
        case .icPassport:      return 4
        case .referenceNumber: return 3
        case .name:            return 2
        case .address:         return 1
        }
    }
}

struct RedactionItem: Identifiable {
    let id: UUID
    let redactionType: RedactionType
    let pageIndex: Int
    /// Normalized UIKit coords: origin top-left, values 0–1
    let boundingBox: CGRect
    /// Detection confidence 0.0–1.0
    let confidenceScore: Double
    /// Human-readable explanation shown in debug builds
    let detectionReason: String
    /// Auto-set from threshold; user can override by tapping in the preview
    var isEnabled: Bool

    init(
        id: UUID = UUID(),
        redactionType: RedactionType,
        pageIndex: Int,
        boundingBox: CGRect,
        confidenceScore: Double,
        detectionReason: String
    ) {
        self.id = id
        self.redactionType = redactionType
        self.pageIndex = pageIndex
        self.boundingBox = boundingBox
        self.confidenceScore = confidenceScore
        self.detectionReason = detectionReason
        self.isEnabled = confidenceScore >= redactionType.autoEnableThreshold
    }
}
