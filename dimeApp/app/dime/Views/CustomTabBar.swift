//
//  CustomTabBar.swift
//  xpenz
//
//  Created by Rafael Soh on 20/5/22.
//

import Foundation
import SwiftUI

struct CustomTabBar: View {
    @EnvironmentObject var appLockVM: AppLockViewModel
    @Binding var currentTab: String
    var topEdge: CGFloat
    var bottomEdge: CGFloat
    @State private var showAddMenu: Bool = false

    @State var checkingFace: Bool = false

    @FetchRequest(sortDescriptors: []) private var transactions: FetchedResults<Transaction>

    @State var count = 0
    @Binding var counter: Int

    var launchAdd: Bool
    var onAddExpense: () -> Void = {}
    var onScanReceipt: () -> Void = {}
    var onAskKIRA: () -> Void = {}

    @AppStorage("confetti", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var confetti: Bool = false
    @AppStorage("firstTransactionViewLaunch", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var firstLaunch: Bool = true

    @State var animate = false

    private var isZoomed: Bool {
        UIScreen.main.scale != UIScreen.main.nativeScale
    }

    var body: some View {
        ZStack(alignment: .bottom) {
            if showAddMenu {
                ZStack {
                    Color.black.opacity(0.20)
                    Rectangle()
                        .fill(.ultraThinMaterial)
                        .opacity(0.45)
                }
                    .ignoresSafeArea()
                    .onTapGesture {
                        withAnimation(.spring(response: 0.28, dampingFraction: 0.9)) {
                            showAddMenu = false
                        }
                    }
                    .transition(.opacity)

                FloatingActionMenu(
                    onScanReceipt: {
                        closeMenu()
                        onScanReceipt()
                    },
                    onAddExpense: {
                        prepareTransactionCount()
                        closeMenu()
                        onAddExpense()
                    },
                    onAskKIRA: {
                        closeMenu()
                        onAskKIRA()
                    }
                )
                .padding(.bottom, 82 + max(bottomEdge - 10, 0))
                .transition(.move(edge: .bottom).combined(with: .opacity).combined(with: .scale(scale: 0.96, anchor: .bottom)))
            }

            HStack(spacing: 0) {
                TabButton(image: "Home", zoomed: isZoomed, currentTab: $currentTab)

                Spacer()
                    .frame(width: 96)

                TabButton(image: "Activity", zoomed: isZoomed, currentTab: $currentTab)
            }
            .padding(.horizontal, 34)
            .padding(.bottom, max(bottomEdge - 10, 0))
            .padding(.top, 14)
            .background(
                Color.PrimaryBackground
                    .ignoresSafeArea(edges: .bottom)
                    .overlay(alignment: .top) {
                        Rectangle()
                            .fill(Color.Outline)
                            .frame(height: 0.5)
                    }
            )
            .overlay(alignment: .top) {
                floatingAddButton
                    .offset(y: -22)
            }
            .frame(maxWidth: .infinity)
        }
        .onChange(of: launchAdd) { _ in
            prepareTransactionCount()
            onAddExpense()
        }
        .onChange(of: transactions.count) { _ in
            if !transactions.isEmpty {
                self.animate = false
            } else {
                self.animate = true
            }
        }
        .onOpenURL { url in
            guard
                url.host == "newExpense"

            else {
                return
            }

            prepareTransactionCount()
            onAddExpense()
        }
    }

    private var floatingAddButton: some View {
        Button {
            let impactMed = UIImpactFeedbackGenerator(style: .light)
            impactMed.impactOccurred()
            withAnimation(.spring(response: 0.28, dampingFraction: 0.86)) {
                showAddMenu.toggle()
            }
        } label: {
            Image(systemName: "plus")
                .font(Font.satoshi(28, weight: .semibold))
                .foregroundColor(Color.LightIcon)
                .frame(width: 60, height: 60)
                .background(Color.DarkBackground, in: Circle())
                .overlay(Circle().stroke(Color.white.opacity(0.14), lineWidth: 1))
                .shadow(color: Color.black.opacity(0.18), radius: 16, x: 0, y: 8)
                .rotationEffect(.degrees(showAddMenu ? 45 : 0))
        }
        .buttonStyle(BouncyButton(duration: 0.24, scale: 0.92))
        .accessibilityLabel("Open quick actions")
        .onAppear {
            if transactions.isEmpty {
                withAnimation(.easeInOut(duration: 1.9).repeatForever(autoreverses: false)) {
                    self.animate.toggle()
                }
            }
        }
    }

    private func closeMenu() {
        withAnimation(.spring(response: 0.28, dampingFraction: 0.9)) {
            showAddMenu = false
        }
    }

    private func prepareTransactionCount() {
        count = transactions.count
        if firstLaunch {
            firstLaunch = false
        }
    }
}

private struct FloatingActionMenu: View {
    let onScanReceipt: () -> Void
    let onAddExpense: () -> Void
    let onAskKIRA: () -> Void

    var body: some View {
        VStack(spacing: 2) {
            actionRow(title: "Scan receipt", subtitle: "Auto-sort", systemImage: "viewfinder", action: onScanReceipt)
            actionRow(title: "Add expense", subtitle: "Log quickly", systemImage: "plus.circle", action: onAddExpense)
            actionRow(title: "Ask KIRA", subtitle: "Get answers", systemImage: "sparkles", action: onAskKIRA)
        }
        .padding(8)
        .frame(width: 238)
        .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 22, style: .continuous))
        .background(Color.PrimaryBackground.opacity(0.86), in: RoundedRectangle(cornerRadius: 22, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 22, style: .continuous)
                .stroke(Color.Outline.opacity(0.75), lineWidth: 1)
        )
        .shadow(color: Color.black.opacity(0.14), radius: 22, x: 0, y: 12)
    }

    private func actionRow(title: String, subtitle: String, systemImage: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack(spacing: 12) {
                Image(systemName: systemImage)
                    .font(Font.satoshi(16, weight: .semibold))
                    .foregroundColor(Color(hex: "4A5240"))
                    .frame(width: 34, height: 34)
                    .background(Color(hex: "4A5240").opacity(0.10), in: Circle())

                VStack(alignment: .leading, spacing: 1) {
                    Text(title)
                        .font(Font.satoshi(.subheadline, weight: .semibold))
                        .foregroundColor(Color.PrimaryText)
                    Text(subtitle)
                        .font(Font.satoshi(.caption2, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                }

                Spacer()
            }
            .padding(.horizontal, 10)
            .frame(height: 50)
            .contentShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
        }
        .buttonStyle(BouncyButton(duration: 0.2, scale: 0.98))
    }
}

struct MyButtonStyle: ButtonStyle {
    func makeBody(configuration: Self.Configuration) -> some View {
        configuration.label
            .font(Font.satoshi(20, weight: .bold))
            .foregroundColor(Color.LightIcon)
            .frame(width: 65, height: 38)
            .background(configuration.isPressed ? Color.SubtitleText : Color.DarkBackground, in: RoundedRectangle(cornerRadius: 13, style: .continuous))
    }
}

struct BouncyButton: ButtonStyle {
    var duration: Double
    var scale: Double

    public func makeBody(configuration: Self.Configuration) -> some View {
        return configuration.label
            .scaleEffect(configuration.isPressed ? scale : 1)
//            .scaleEffect(configuration.isPressed ? 1.3 : 1)
            .animation(.easeOut(duration: duration), value: configuration.isPressed)
    }
}

struct TabButton: View {
    var image: String
    var zoomed: Bool
    @Binding var currentTab: String

    private var sfSymbol: (inactive: String, active: String) {
        switch image {
        case "Home":     return ("house", "house.fill")
        case "Activity": return ("receipt", "receipt.fill")
        default:         return ("circle", "circle.fill")
        }
    }

    var body: some View {
        Button {
            DispatchQueue.main.async {
                currentTab = image
            }
        } label: {
            VStack(spacing: 4) {
                Image(systemName: currentTab == image ? sfSymbol.active : sfSymbol.inactive)
                    .font(Font.satoshi(21, weight: currentTab == image ? .semibold : .regular))
                    .foregroundColor(currentTab == image ? Color.DarkIcon : Color.GreyIcon)
                    .animation(.easeInOut(duration: 0.2), value: currentTab)

                Text(image)
                    .font(Font.satoshi(11, weight: currentTab == image ? .semibold : .medium))
                    .foregroundColor(currentTab == image ? Color.DarkIcon : Color.GreyIcon)
                    .lineLimit(1)
            }
            .frame(maxWidth: .infinity)
            .frame(height: 56)
        }
        .buttonStyle(BouncyButton(duration: 0.3, scale: 0.75))
        .accessibilityLabel("\(image) tab")
        .accessibilityAddTraits(
            currentTab == image
                ? [.isButton, .isSelected]
                : .isButton
        )
    }
}
