//
//  Font+Satoshi.swift
//  dime
//

import SwiftUI

extension Font {

    static func satoshi(_ size: CGFloat, weight: Font.Weight = .regular) -> Font {
        .custom(satoshiName(weight), size: size)
    }

    static func satoshi(_ style: Font.TextStyle, weight: Font.Weight = .regular) -> Font {
        .custom(satoshiName(weight), size: textStyleSize(style), relativeTo: style)
    }

    private static func satoshiName(_ weight: Font.Weight) -> String {
        switch weight {
        case .black:       return "Satoshi-Black"
        case .bold:        return "Satoshi-Bold"
        case .semibold:    return "Satoshi-Medium"
        case .medium:      return "Satoshi-Medium"
        case .light:       return "Satoshi-Light"
        case .thin:        return "Satoshi-Light"
        case .ultraLight:  return "Satoshi-Light"
        default:           return "Satoshi-Regular"
        }
    }

    private static func textStyleSize(_ style: Font.TextStyle) -> CGFloat {
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
