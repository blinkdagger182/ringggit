//
//  FontExtension.swift
//  dime
//

import UIKit
import SwiftUI

extension UIFont {

    static func getBodyFontSize(dynamicTypeSize: DynamicTypeSize) -> CGFloat {
        switch dynamicTypeSize {
        case .xSmall:
            return 14
        case .small:
            return 15
        case .medium:
            return 16
        case .large:
            return 17
        case .xLarge:
            return 19
        case .xxLarge:
            return 21
        case .xxxLarge:
            return 23
        default:
            return 23
        }
    }

    class func rounded(ofSize size: CGFloat, weight: UIFont.Weight) -> UIFont {
        let name: String
        switch weight {
        case .black, .heavy:
            name = "Satoshi-Black"
        case .bold:
            name = "Satoshi-Bold"
        case .semibold, .medium:
            name = "Satoshi-Medium"
        case .light, .thin, .ultraLight:
            name = "Satoshi-Light"
        default:
            name = "Satoshi-Regular"
        }
        return UIFont(name: name, size: size) ?? UIFont.systemFont(ofSize: size, weight: weight)
    }

    class func roundedSpecial(ofStyle style: UIFont.TextStyle, weight: UIFont.Weight, size: Double) -> UIFont {
        let base = UIFont.rounded(ofSize: size, weight: weight)
        return UIFontMetrics(forTextStyle: style).scaledFont(for: base)
    }

    static func textStyleSize(_ style: UIFont.TextStyle) -> CGFloat {
        UIFont.preferredFont(forTextStyle: style).pointSize
    }
}
