//
//  HomeAIAssistantOverlay.swift
//  dime
//

import SwiftUI
import WebKit

struct HomeAIAssistantOverlay: View {
    @ObservedObject var viewModel: HomeAIAssistantViewModel
    @ObservedObject private var keyboardHeightHelper = KeyboardHeightHelper()

    let namespace: Namespace.ID
    let topInset: CGFloat
    let bottomInset: CGFloat
    let revealProgress: CGFloat
    let isExpanded: Bool
    let onCollapse: () -> Void
    let collapseGesture: AnyGesture<DragGesture.Value>?

    @FocusState private var composerFocused: Bool

    var body: some View {
        GeometryReader { proxy in
            let keyboardOverlap = max(0, keyboardHeightHelper.keyboardHeight - proxy.safeAreaInsets.bottom)
            let baseComposerBottomPadding = max(proxy.safeAreaInsets.bottom, bottomInset) + 12
            let activeComposerBottomPadding = keyboardOverlap > 0 ? 12 : baseComposerBottomPadding

            ZStack(alignment: .bottom) {
                Color(hex: "0A0A0A")
                    .ignoresSafeArea()
                    .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))

                HomeAIAnimatedGradientBackground()
                    .opacity(max(0, revealProgress * 0.92))
                    .mask(
                        LinearGradient(
                            colors: [.black, .black.opacity(0.88), .clear],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                    )
                    .ignoresSafeArea()
                    .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))

                VStack(spacing: 0) {
                    previewHeader
                        .padding(.horizontal, 24)
                        .padding(.top, max(topInset, 12) + 18)
                        .opacity(revealProgress)

                    messageArea(
                        keyboardOverlap: keyboardOverlap,
                        composerBottomPadding: activeComposerBottomPadding
                    )
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                        .opacity(revealProgress)
                        .offset(y: (1 - revealProgress) * 24)
                }
                .frame(width: proxy.size.width, height: proxy.size.height, alignment: .top)

                composer
                    .padding(.horizontal, 20)
                    .padding(.bottom, activeComposerBottomPadding)
                    .opacity(revealProgress)
                    .offset(y: (1 - revealProgress) * 20 - keyboardOverlap)
                    .background(
                        LinearGradient(
                            colors: [
                                Color.black.opacity(0),
                                Color.black.opacity(0.18),
                                Color.black.opacity(0.34)
                            ],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                    )
                    .zIndex(2)
                    .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))

                if isExpanded {
                    collapseHandle
                        .padding(.bottom, activeComposerBottomPadding + 104)
                        .opacity(revealProgress)
                        .offset(y: -keyboardOverlap)
                        .zIndex(1)
                        .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))
                }
            }
            .ignoresSafeArea(.keyboard, edges: .bottom)
            .onChange(of: isExpanded) { expanded in
                guard expanded else { return }
                withAnimation(.spring(response: 0.45, dampingFraction: 0.86)) {
                    composerFocused = viewModel.messages.isEmpty
                }
            }
        }
    }

    private var previewHeader: some View {
        HStack(spacing: 14) {
            Button(action: onCollapse) {
                Image(systemName: "xmark")
                    .font(.system(size: 18, weight: .medium))
                    .foregroundColor(.white.opacity(0.82))
                    .frame(width: 36, height: 36)
            }
            .buttonStyle(.plain)
            .accessibilityLabel("Minimize AI Assistant")

            Spacer()

            HStack(spacing: 10) {
                Image(systemName: "sparkles")
                    .font(.system(size: 15, weight: .semibold))

                Text("Ryt AI")
                    .font(.system(size: 21, weight: .semibold, design: .rounded))

                Text("beta")
                    .font(.system(.subheadline, design: .rounded).weight(.semibold))
                    .padding(.horizontal, 10)
                    .padding(.vertical, 5)
                    .overlay(
                        Capsule(style: .continuous)
                            .stroke(Color.white.opacity(0.24), lineWidth: 1)
                    )
            }
            .foregroundColor(.white.opacity(0.88))

            Spacer()

            Color.clear
                .frame(width: 36, height: 36)
        }
    }

    private func messageArea(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> some View {
        ScrollViewReader { proxy in
            ScrollView(showsIndicators: false) {
                VStack(alignment: .leading, spacing: 16) {
                    if viewModel.messages.isEmpty {
                        emptyState
                    } else {
                        quickActionsSection(title: "Try asking")

                        ForEach(viewModel.messages) { message in
                            messageBubble(message)
                                .id(message.id)
                        }

                        if viewModel.isResponding {
                            typingIndicator
                        }
                    }

                    Color.clear
                        .frame(height: 1)
                        .id("bottom-anchor")
                }
                .padding(.horizontal, 24)
                .padding(.top, 16)
                .padding(.bottom, 120 + composerBottomPadding + keyboardOverlap)
            }
            .onChange(of: viewModel.messages.count) { _ in
                scrollToBottom(proxy, animated: true)
            }
            .onChange(of: viewModel.isResponding) { _ in
                scrollToBottom(proxy, animated: true)
            }
            .onChange(of: keyboardHeightHelper.keyboardHeight) { _ in
                scrollToBottom(proxy, animated: true)
            }
            .onChange(of: composerFocused) { focused in
                guard focused else { return }
                scrollToBottom(proxy, animated: true)
            }
            .onAppear {
                guard !viewModel.messages.isEmpty else { return }
                scrollToBottom(proxy, animated: false)
            }
        }
    }

    private var emptyState: some View {
        VStack(spacing: 18) {
            Text("How can I help you?")
                .font(.system(size: 30, weight: .medium, design: .serif))
                .tracking(-0.4)
                .foregroundColor(.white.opacity(0.96))
                .multilineTextAlignment(.center)
                .padding(.top, 20)

            quickActionsSection(title: "")

            Text("Try: \"How much did I spend on food this week?\"")
                .font(.system(.subheadline, design: .rounded).weight(.medium))
                .foregroundColor(.white.opacity(0.42))
        }
    }

    private func quickActionsSection(title: String) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            if !title.isEmpty {
                Text(title)
                    .font(.system(.footnote, design: .rounded).weight(.medium))
                    .foregroundColor(.white.opacity(0.42))
            }

            LazyVGrid(columns: [
                GridItem(.flexible(), spacing: 14),
                GridItem(.flexible(), spacing: 14)
            ], spacing: 14) {
                ForEach(viewModel.quickActions) { quickAction in
                    Button {
                        viewModel.triggerQuickAction(quickAction)
                    } label: {
                        VStack(alignment: .leading, spacing: 10) {
                            Image(systemName: quickAction.systemImage)
                                .font(.system(size: 14, weight: .semibold))
                                .foregroundColor(.white.opacity(0.72))

                            Text(quickAction.title)
                                .font(.system(.headline, design: .rounded).weight(.medium))
                                .foregroundColor(.white.opacity(0.92))
                                .lineLimit(2)
                                .multilineTextAlignment(.leading)

                            Text(quickActionSubtitle(for: quickAction.title))
                                .font(.system(.footnote, design: .rounded))
                                .foregroundColor(.white.opacity(0.46))
                                .lineLimit(2)
                        }
                        .padding(16)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(
                            RoundedRectangle(cornerRadius: 16, style: .continuous)
                                .fill(Color(hex: "141414").opacity(0.96))
                                .overlay(
                                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                                        .stroke(Color(hex: "222222"), lineWidth: 1)
                                )
                        )
                    }
                    .buttonStyle(.plain)
                }
            }
        }
    }

    private func messageBubble(_ message: HomeAIMessage) -> some View {
        HStack {
            if message.role == .assistant {
                VStack(alignment: .leading, spacing: 6) {
                    Text("Ryt AI")
                        .font(.system(.caption, design: .rounded).weight(.medium))
                        .foregroundColor(.white.opacity(0.38))

                    Text(message.text)
                        .font(.system(.body, design: .rounded))
                        .foregroundColor(.white.opacity(0.92))
                        .multilineTextAlignment(.leading)
                }
                .padding(16)
                .background(
                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                        .fill(Color(hex: "141414").opacity(0.96))
                        .overlay(
                            RoundedRectangle(cornerRadius: 16, style: .continuous)
                                .stroke(Color(hex: "222222"), lineWidth: 1)
                        )
                )

                Spacer(minLength: 42)
            } else {
                Spacer(minLength: 42)

                Text(message.text)
                    .font(.system(.body, design: .rounded).weight(.medium))
                    .foregroundColor(.white.opacity(0.94))
                    .padding(.horizontal, 16)
                    .padding(.vertical, 13)
                    .background(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .fill(Color(hex: "1A1A1A"))
                    )
            }
        }
    }

    private var typingIndicator: some View {
        HStack {
            HStack(spacing: 6) {
                Circle()
                    .fill(Color.white.opacity(0.8))
                    .frame(width: 7, height: 7)
                Circle()
                    .fill(Color.white.opacity(0.6))
                    .frame(width: 7, height: 7)
                Circle()
                    .fill(Color.white.opacity(0.4))
                    .frame(width: 7, height: 7)
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 14)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color(hex: "141414").opacity(0.96))
                    .overlay(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .stroke(Color(hex: "222222"), lineWidth: 1)
                    )
            )

            Spacer(minLength: 42)
        }
    }

    private var composer: some View {
        HStack(alignment: .center, spacing: 10) {
            TextField("Ask anything about your money...", text: $viewModel.draftMessage)
                .font(.system(.body, design: .rounded))
                .foregroundColor(.white.opacity(0.95))
                .frame(maxHeight: .infinity, alignment: .center)
                .focused($composerFocused)
                .submitLabel(.send)
                .onSubmit {
                    viewModel.sendDraftMessage()
                }

            Button {
                viewModel.sendDraftMessage()
            } label: {
                Image(systemName: "arrow.up")
                    .font(.system(size: 16, weight: .semibold))
                    .foregroundColor(viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? .white.opacity(0.34) : .white)
                    .frame(width: 34, height: 34)
                    .background(
                        Circle()
                            .fill(viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? Color.white.opacity(0.08) : Color.white.opacity(0.14))
                    )
            }
            .buttonStyle(.plain)
            .disabled(viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || viewModel.isResponding)
        }
        .frame(height: 56)
        .padding(.horizontal, 18)
        .background(
            RoundedRectangle(cornerRadius: 20, style: .continuous)
                .fill(Color(hex: "1A1A1A"))
                .overlay(
                    RoundedRectangle(cornerRadius: 20, style: .continuous)
                        .stroke(composerFocused ? Color.white.opacity(0.16) : Color.clear, lineWidth: 1)
                )
                .shadow(color: composerFocused ? Color.white.opacity(0.08) : Color.black.opacity(0.12), radius: composerFocused ? 18 : 12, y: 8)
        )
    }

    private var collapseHandle: some View {
        Image(systemName: "chevron.compact.up")
            .font(.system(size: 24, weight: .medium))
            .foregroundColor(.white.opacity(0.42))
            .frame(width: 44, height: 28)
            .contentShape(Rectangle())
    }

    private func scrollToBottom(_ proxy: ScrollViewProxy, animated: Bool) {
        let action = {
            proxy.scrollTo("bottom-anchor", anchor: .bottom)
        }

        DispatchQueue.main.async {
            if animated {
                withAnimation(.easeOut(duration: 0.22)) {
                    action()
                }
            } else {
                action()
            }
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.08) {
            if animated {
                withAnimation(.easeOut(duration: 0.18)) {
                    action()
                }
            } else {
                action()
            }
        }
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

private struct HomeAICollapseGestureModifier: ViewModifier {
    let dragGesture: AnyGesture<DragGesture.Value>?

    func body(content: Content) -> some View {
        if let dragGesture {
            content.highPriorityGesture(dragGesture)
        } else {
            content
        }
    }
}

private struct HomeAIAnimatedGradientBackground: UIViewRepresentable {
    func makeUIView(context: Context) -> WKWebView {
        let webView = WKWebView()
        webView.isOpaque = false
        webView.backgroundColor = .clear
        webView.scrollView.isScrollEnabled = false
        webView.scrollView.bounces = false

        let htmlString = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
            <style>
                * { margin: 0; padding: 0; box-sizing: border-box; }
                body, html {
                    width: 100%;
                    height: 100%;
                    overflow: hidden;
                    background: #0A0A0A;
                }
                .background--custom {
                    background-color: #0A0A0A;
                    width: 100vw;
                    height: 100vh;
                    position: absolute;
                    overflow: hidden;
                    top: 0;
                    left: 0;
                }
                canvas#canvas {
                    position: absolute;
                    width: 100%;
                    height: 60%;
                    transform-origin: 50% 0%;
                    transform: rotate(-18deg) scale(1.9) translateY(-24%);
                    --gradient-color-1: #0f172a;
                    --gradient-color-2: #163d66;
                    --gradient-color-3: #472066;
                    --gradient-color-4: #0b2640;
                    --gradient-speed: 0.000012;
                    filter: blur(26px);
                    opacity: 0.72;
                }
            </style>
        </head>
        <body>
            <div class="background--custom">
                <canvas id="canvas"></canvas>
            </div>
            <script src="https://cdn.jsdelivr.net/gh/greentfrapp/pocoloco@minigl/minigl.js"></script>
            <script>
                var gradient = new Gradient();
                gradient.initGradient("#canvas");

                const canvas = document.getElementById('canvas');
                let time = 0;

                function animatePosition() {
                    time += 0.00042;

                    const moveX = Math.sin(time * 0.7) * 10;
                    const moveY = Math.cos(time * 0.5) * 12;
                    const rotation = Math.sin(time * 0.45) * 20 - 18;

                    canvas.style.transform = `rotate(${rotation}deg) scale(1.9) translateY(-24%) translateX(${moveX}%) translateY(${moveY}%)`;
                    requestAnimationFrame(animatePosition);
                }

                animatePosition();
            </script>
        </body>
        </html>
        """

        webView.loadHTMLString(htmlString, baseURL: nil)
        return webView
    }

    func updateUIView(_ uiView: WKWebView, context: Context) {}
}
