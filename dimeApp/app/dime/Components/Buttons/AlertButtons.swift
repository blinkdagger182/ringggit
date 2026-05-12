//
//  File.swift
//  dime
//
//  Created by Rafael Soh on 4/11/23.
//

import Foundation
import SwiftUI

struct DeleteButton: View {
    let text: String
    let red: Bool

    var body: some View {
        Text(text)
            .font(Font.satoshi(.title3, weight: .semibold))
            .foregroundColor(red ? Color.white : Color.PrimaryText.opacity(0.9))
            .frame(height: 45)
            .frame(maxWidth: .infinity)
            .background(red ? Color.AlertRed : Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 9, style: .continuous))
    }
}
