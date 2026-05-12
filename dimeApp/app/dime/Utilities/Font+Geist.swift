//
//  Font+Geist.swift
//  dime
//

import SwiftUI

extension Font {

    static func geist(_ size: CGFloat, weight: Font.Weight = .regular) -> Font {
        .custom(geistName(weight), size: size)
    }

    static func geist(_ style: Font.TextStyle, weight: Font.Weight = .regular) -> Font {
        .custom(geistName(weight), size: geistStyleSize(style), relativeTo: style)
    }

    private static func geistName(_ weight: Font.Weight) -> String {
        switch weight {
        case .bold, .heavy, .black: return "Geist-Bold"
        case .semibold:             return "Geist-SemiBold"
        case .medium:               return "Geist-Medium"
        default:                    return "Geist-Regular"
        }
    }

    private static func geistStyleSize(_ style: Font.TextStyle) -> CGFloat {
        switch style {
        case .largeTitle:   return 34
        case .title:        return 28
        case .title2:       return 22
        case .title3:       return 20
        case .headline:     return 17
        case .subheadline:  return 15
        case .body:         return 17
        case .callout:      return 16
        case .footnote:     return 13
        case .caption:      return 12
        case .caption2:     return 11
        @unknown default:   return 17
        }
    }
}
