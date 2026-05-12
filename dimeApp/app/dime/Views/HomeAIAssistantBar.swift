//
//  HomeAIAssistantBar.swift
//  dime
//

import SwiftUI

struct HomeAIAssistantBar: View {
    enum PresentationMode {
        case idlePill
        case previewBackdrop
    }

    @ObservedObject var viewModel: HomeAIAssistantViewModel
    let namespace: Namespace.ID
    let topInset: CGFloat
    let pullOffset: CGFloat
    let mode: PresentationMode
    let dragGesture: AnyGesture<DragGesture.Value>?
    let onExpand: () -> Void

    private let revealThreshold: CGFloat = 132

    private var progress: CGFloat {
        min(max(pullOffset / revealThreshold, 0), 1)
    }

    private var previewOpacity: CGFloat {
        min(max((progress - 0.18) / 0.82, 0), 1)
    }

    private var promptText: String {
        progress > 0.82 ? "Release for ✨ AI" : "Pull more for ✨ AI"
    }

    var body: some View {
        ZStack(alignment: .top) {
            switch mode {
            case .idlePill:
                VStack(spacing: 0) {
                    askKIRAPill
                        .padding(.top, max(topInset, 12) + 14)
                        .opacity(max(0, 1.0 - (progress * 0.9)))

                    Spacer()
                }

            case .previewBackdrop:
                assistantBackground
                    .opacity(max(0.24, progress * 0.98))
                    .mask(
                        LinearGradient(
                            colors: [.black, .black, .clear],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                    )
                    .ignoresSafeArea()

                VStack(spacing: 0) {
                    previewHeader
                        .padding(.horizontal, 28)
                        .padding(.top, max(topInset, 12) + 18)
                        .opacity(previewOpacity)

                    Spacer()

                    previewBody
                        .opacity(previewOpacity)
                        .offset(y: (1 - previewOpacity) * 24)

                    Spacer(minLength: 0)
                }

                VStack(spacing: 0) {
                    Spacer(minLength: min(290, 190 + pullOffset * 0.65))

                    Text(promptText)
                        .font(Font.satoshi(.title3, weight: .medium))
                        .foregroundColor(.white.opacity(0.92))
                        .opacity(0.9)
                        .padding(.horizontal, 24)
                }
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }

    @ViewBuilder
    private var askKIRAPill: some View {
        HStack(spacing: 10) {
            Image(systemName: "sparkles")
                .font(Font.satoshi(18, weight: .semibold))

            Text("Ask KIRA")
                .font(Font.satoshi(17, weight: .semibold))
                .tracking(-0.2)
        }
        .foregroundColor(.white.opacity(0.94))
        .padding(.horizontal, 22)
        .padding(.vertical, 13)
        .background(
            Capsule(style: .continuous)
                .fill(.ultraThinMaterial)
                .opacity(0.58)
        )
        .overlay {
            Capsule(style: .continuous)
                .strokeBorder(
                    LinearGradient(
                        colors: [
                            Color.white.opacity(0.62),
                            Color(hex: "C8B94A").opacity(0.70),
                            Color(hex: "4A5240").opacity(0.50)
                        ],
                        startPoint: .topLeading,
                        endPoint: .bottomTrailing
                    ),
                    lineWidth: 1.4
                )
        }
        .shadow(color: Color(hex: "4A5240").opacity(0.20), radius: 18, x: 0, y: 8)
        .contentShape(Rectangle())
        .matchedGeometryEffect(id: "home-ai-surface", in: namespace)
        .onTapGesture(perform: onExpand)
        .modifier(TopHandleGestureModifier(dragGesture: dragGesture))
    }

    private var previewHeader: some View {
        HStack {
            Button(action: onExpand) {
                Image(systemName: "xmark")
                    .font(Font.satoshi(18, weight: .medium))
                    .foregroundColor(.white.opacity(0.84))
                    .frame(width: 36, height: 36)
            }
            .buttonStyle(.plain)
            .opacity(0.45)

            Spacer()

            HStack(spacing: 10) {
                Image(systemName: "sparkles")
                    .font(Font.satoshi(15, weight: .semibold))
                Text("KIRA")
                    .font(Font.satoshi(21, weight: .semibold))

                Text("beta")
                    .font(Font.satoshi(.subheadline, weight: .semibold))
                    .padding(.horizontal, 10)
                    .padding(.vertical, 5)
                    .overlay(
                        Capsule(style: .continuous)
                            .stroke(Color.white.opacity(0.26), lineWidth: 1)
                    )
            }
            .foregroundColor(.white.opacity(0.82))

            Spacer()

            Color.clear
                .frame(width: 36, height: 36)
        }
    }

    private var previewBody: some View {
        VStack(spacing: 18) {
            Text("How can I help you?")
                .font(Font.satoshi(30, weight: .medium))
                .tracking(-0.4)
                .foregroundColor(.white.opacity(0.94))
                .multilineTextAlignment(.center)

            LazyVGrid(columns: [
                GridItem(.flexible(), spacing: 14),
                GridItem(.flexible(), spacing: 14)
            ], spacing: 14) {
                ForEach(viewModel.quickActions) { quickAction in
                    VStack(alignment: .leading, spacing: 10) {
                        Image(systemName: quickAction.systemImage)
                            .font(Font.satoshi(14, weight: .semibold))
                            .foregroundColor(.white.opacity(0.72))

                        Text(quickAction.title)
                            .font(Font.satoshi(.headline, weight: .medium))
                            .foregroundColor(.white.opacity(0.92))
                            .lineLimit(2)
                            .multilineTextAlignment(.leading)

                        Text(quickActionSubtitle(for: quickAction.title))
                            .font(Font.satoshi(.footnote))
                            .foregroundColor(.white.opacity(0.46))
                            .lineLimit(2)
                    }
                    .padding(16)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .fill(Color(hex: "141414").opacity(0.9))
                            .overlay(
                                RoundedRectangle(cornerRadius: 16, style: .continuous)
                                    .stroke(Color(hex: "222222"), lineWidth: 1)
                            )
                    )
                }
            }
            .padding(.horizontal, 28)

            Text("Try: \"How much did I spend on food this week?\"")
                .font(Font.satoshi(.subheadline, weight: .medium))
                .foregroundColor(.white.opacity(0.42))
        }
    }

    private var assistantBackground: some View {
        Color(hex: "0A0A0A")
    }

    private func quickActionSubtitle(for title: String) -> String {
        switch title {
        case "Add transaction":
            return "Quickly log an expense"
        case "Where did my money go?":
            return "Breakdown by category"
        case "Show this month's spending":
            return "Review this month's flow"
        case "Can I afford this?":
            return "Check against current budget"
        default:
            return "Start with a suggested prompt"
        }
    }
}

private struct TopHandleGestureModifier: ViewModifier {
    let dragGesture: AnyGesture<DragGesture.Value>?

    func body(content: Content) -> some View {
        if let dragGesture {
            content.highPriorityGesture(dragGesture)
        } else {
            content
        }
    }
}
