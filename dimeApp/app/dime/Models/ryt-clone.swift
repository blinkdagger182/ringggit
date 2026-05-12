import SwiftUI

// MARK: - Ryt-style Peek Sandbox
// Paste into ContentView.swift in a fresh SwiftUI app. iOS 17+

struct ContentView: View {
    var body: some View {
        RytPeekSandboxView()
    }
}

struct RytPeekSandboxView: View {
    private let collapsedTopInset: CGFloat = 100
    private let revealedHomePeekHeight: CGFloat = 100
    private let homeCornerRadius: CGFloat = 32

    @State private var containerHeight: CGFloat = 852
    @State private var settledProgress: CGFloat = 0
    @State private var homeScrollY: CGFloat = 0
    @State private var gestureCanReveal = false
    @State private var dragSessionStarted = false
    @State private var isHomeAtTop = true
    @State private var lockedCanRevealForThisDrag = false
    @State private var lastKnownHomeScrollY: CGFloat = 0
    @State private var homeContentOffsetY: CGFloat = 0
    @State private var isOrbBreathing = false

    @GestureState private var dragY: CGFloat = 0

    private var revealDistance: CGFloat {
        max(containerHeight - collapsedTopInset - revealedHomePeekHeight, 1)
    }

    private var liveProgress: CGFloat {
        min(max(settledProgress + dragY / revealDistance, 0), 1)
    }

    private var homeSheetY: CGFloat {
        collapsedTopInset + liveDistance
    }

    private var liveDistance: CGFloat {
        liveProgress * revealDistance
    }

    var body: some View {
        GeometryReader { geometry in
            let w = geometry.size.width
            let h = geometry.size.height
            let base = min(w / 430.0, h / 932.0)
            let s = base * 0.92
            let headerTopPadding = 8 * s

            ZStack(alignment: .top) {
                // IMPORTANT:
                // Do NOT add .ignoresSafeArea() here.
                // Only the AI background ignores the safe area.
                // This keeps the top row below the Dynamic Island.
                aiPage

                homeSheet
                    .offset(y: homeSheetY)
                    .simultaneousGesture(homeDragGesture)
                    .animation(
                        .interactiveSpring(response: 0.42, dampingFraction: 0.9, blendDuration: 0.12),
                        value: settledProgress
                    )

                // Floats above homeSheet so collapsedTopInset = 100 doesn't clip it.
                collapsedTopBar(scale: s, topPadding: headerTopPadding)
                    .opacity(1 - Double(liveProgress))
            }
            .background(Color.black)
            .onAppear {
                containerHeight = h
            }
            .onChange(of: h) { _, newValue in
                containerHeight = newValue
            }
        }
    }

    // MARK: - AI Page

    private var aiPage: some View {
        GeometryReader { geo in
            let w = geo.size.width
            let h = geo.size.height

            let base = min(w / 430.0, h / 932.0)
            let s = base * 0.92

            let headerTopPadding = 8 * s

            ZStack(alignment: .top) {
                aiLightBackground
                    .ignoresSafeArea()

                VStack(spacing: 0) {
                    sakuAIHeader(scale: s)
                        .padding(.top, headerTopPadding)

                    categoryPill(scale: s)
                        .padding(.top, 16 * s)

                    heroCopy(scale: s)
                        .frame(width: w - 74 * s)
                        .padding(.top, 48 * s)

                    glowOrb(scale: s)
                        .padding(.top, 34 * s)

                    sakuComposer(scale: s)
                        .frame(width: w - 72 * s)
                        .padding(.top, 44 * s)

                    suggestionsList(scale: s)
                        .frame(width: w - 72 * s)
                        .padding(.top, 24 * s)

                    Spacer(minLength: 0)

                    quickActionBar(scale: s)
                        .frame(width: w - 56 * s)
                        .padding(.bottom, max(22 * s, geo.safeAreaInsets.bottom + 12 * s))
                }
                .frame(width: w, height: h)
                .opacity(Double(liveProgress))
            }
            .onAppear {
                isOrbBreathing = true
            }
        }
    }

    private var aiLightBackground: some View {
        ZStack {
            LinearGradient(
                colors: [
                    Color(red: 0.94, green: 1.00, blue: 1.00),
                    Color.white,
                    Color(red: 0.985, green: 0.965, blue: 1.00),
                    Color(red: 0.93, green: 0.98, blue: 1.00)
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )

            RadialGradient(
                colors: [
                    Color(hex: "4A5240").opacity(0.30),
                    Color(hex: "4A5240").opacity(0.10),
                    .clear
                ],
                center: .bottomLeading,
                startRadius: 20,
                endRadius: 380
            )

            RadialGradient(
                colors: [
                    Color(hex: "C8B94A").opacity(0.22),
                    Color(hex: "C8B94A").opacity(0.08),
                    .clear
                ],
                center: .bottomTrailing,
                startRadius: 20,
                endRadius: 420
            )

            RadialGradient(
                colors: [
                    Color(hex: "4A5240").opacity(0.16),
                    .clear
                ],
                center: .topLeading,
                startRadius: 20,
                endRadius: 300
            )
        }
    }

    private func collapsedTopBar(scale s: CGFloat, topPadding: CGFloat) -> some View {
        HStack {
            Button {
                // Profile action.
            } label: {
                Circle()
                    .fill(
                        LinearGradient(
                            colors: [
                                Color(hex: "4A5240"),
                                Color(hex: "5B8C5A")
                            ],
                            startPoint: .topLeading,
                            endPoint: .bottomTrailing
                        )
                    )
                    .frame(width: 48 * s, height: 48 * s)
                    .overlay(
                        Text("ME")
                            .font(.system(size: 17 * s, weight: .bold, design: .rounded))
                            .foregroundStyle(.white)
                    )
            }
            .buttonStyle(.plain)
            .frame(width: 58 * s, alignment: .leading)

            Spacer(minLength: 0)

            Button {
                withAnimation(.interactiveSpring(response: 0.42, dampingFraction: 0.9, blendDuration: 0.12)) {
                    settledProgress = 1
                }
            } label: {
                HStack(spacing: 8 * s) {
                    Text("✦")
                        .font(.system(size: 22 * s, weight: .semibold))
                        .foregroundStyle(.white)

                    Text("Ask KIRA")
                        .font(.system(size: 18 * s, weight: .bold, design: .rounded))
                        .foregroundStyle(.white)
                }
                .padding(.horizontal, 19 * s)
                .frame(height: 48 * s)
                .background(
                    Capsule()
                        .fill(.white.opacity(0.11))
                )
                .background(
                    Capsule()
                        .fill(
                            LinearGradient(
                                colors: [
                                    .white.opacity(0.22),
                                    .white.opacity(0.07),
                                    .cyan.opacity(0.08)
                                ],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                )
                .overlay {
                    Capsule()
                        .stroke(
                            LinearGradient(
                                colors: [
                                    Color(hex: "4A5240").opacity(0.95),
                                    Color.white.opacity(0.62),
                                    Color(hex: "C8B94A").opacity(0.65)
                                ],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            ),
                            lineWidth: 1.5 * s
                        )
                }
                .shadow(color: Color(hex: "4A5240").opacity(0.24), radius: 10 * s, x: 0, y: 0)
                .shadow(color: Color(hex: "C8B94A").opacity(0.18), radius: 13 * s, x: 0, y: 7 * s)
                .contentShape(Capsule())
            }
            .buttonStyle(.plain)

            Spacer(minLength: 0)

            Button {
                // Notification action.
            } label: {
                Image(systemName: "bell")
                    .font(.system(size: 22 * s, weight: .medium))
                    .foregroundStyle(Color(hex: "4A5240"))
                    .frame(width: 48 * s, height: 48 * s)
                    .overlay(alignment: .topTrailing) {
                        Circle()
                            .fill(Color.red)
                            .frame(width: 9 * s, height: 9 * s)
                            .offset(x: 1 * s, y: 4 * s)
                    }
            }
            .buttonStyle(.plain)
            .frame(width: 58 * s, alignment: .trailing)
        }
        .padding(.horizontal, 30 * s)
        .padding(.top, topPadding)
    }

    private func sakuAIHeader(scale s: CGFloat) -> some View {
        ZStack {
            HStack {
                Button {
                    // Profile action.
                } label: {
                    Circle()
                        .fill(
                            LinearGradient(
                                colors: [
                                    Color(hex: "4A5240").opacity(0.72),
                                    Color(hex: "4A5240").opacity(0.42)
                                ],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                        .frame(width: 48 * s, height: 48 * s)
                        .overlay(
                            Text("ME")
                                .font(.system(size: 17 * s, weight: .bold, design: .rounded))
                                .foregroundStyle(.white)
                        )
                }
                .buttonStyle(.plain)

                Spacer()

                Button {
                    // Notification action.
                } label: {
                    Image(systemName: "bell")
                        .font(.system(size: 23 * s, weight: .medium))
                        .foregroundStyle(Color(hex: "4A5240"))
                        .frame(width: 48 * s, height: 48 * s)
                        .overlay(alignment: .topTrailing) {
                            Circle()
                                .fill(Color.red)
                                .frame(width: 9 * s, height: 9 * s)
                                .offset(x: 2 * s, y: 5 * s)
                        }
                }
                .buttonStyle(.plain)
            }
            .padding(.horizontal, 30 * s)

            HStack(spacing: 9 * s) {
                Text("✦")
                    .font(.system(size: 29 * s, weight: .semibold))
                    .foregroundStyle(Color(hex: "4A5240").opacity(0.75))

                Text("KIRA")
                    .font(.system(size: 25 * s, weight: .bold, design: .rounded))
                    .foregroundStyle(Color(red: 0.03, green: 0.10, blue: 0.25))

                Text("beta")
                    .font(.system(size: 16 * s, weight: .medium, design: .rounded))
                    .foregroundStyle(Color(hex: "4A5240"))
                    .padding(.horizontal, 12 * s)
                    .padding(.vertical, 5 * s)
                    .background(.white.opacity(0.38), in: Capsule())
                    .overlay(
                        Capsule()
                            .stroke(Color(hex: "C8B94A").opacity(0.22), lineWidth: 1.1)
                    )
            }
        }
        .frame(height: 50 * s)
    }

    private func categoryPill(scale s: CGFloat) -> some View {
        HStack(spacing: 7 * s) {
            Text("General")
                .font(.system(size: 18 * s, weight: .semibold, design: .rounded))

            Image(systemName: "chevron.down")
                .font(.system(size: 13 * s, weight: .bold))
        }
        .foregroundStyle(Color(hex: "4A5240"))
        .padding(.horizontal, 22 * s)
        .frame(height: 43 * s)
        .background(.white.opacity(0.68), in: Capsule())
        .overlay(Capsule().stroke(Color(hex: "4A5240").opacity(0.22), lineWidth: 1))
    }

    private func heroCopy(scale s: CGFloat) -> some View {
        VStack(spacing: 18 * s) {
            Text("What can I\nhelp you with today?")
                .font(.system(size: 34 * s, weight: .bold, design: .rounded))
                .multilineTextAlignment(.center)
                .lineSpacing(5 * s)
                .foregroundStyle(
                    LinearGradient(
                        colors: [
                            Color(hex: "4A5240"),
                            Color(hex: "C8B94A")
                        ],
                        startPoint: .leading,
                        endPoint: .trailing
                    )
                )

            Text("Ask anything about your money.\nI’m here to help.")
                .font(.system(size: 17 * s, weight: .medium, design: .rounded))
                .multilineTextAlignment(.center)
                .lineSpacing(4 * s)
                .foregroundStyle(Color(red: 0.43, green: 0.46, blue: 0.58))
        }
    }

    private func glowOrb(scale s: CGFloat) -> some View {
        ZStack {
            Ellipse()
                .fill(Color(hex: "C8B94A").opacity(isOrbBreathing ? 0.12 : 0.06))
                .frame(
                    width: isOrbBreathing ? 170 * s : 140 * s,
                    height: isOrbBreathing ? 28 * s : 20 * s
                )
                .blur(radius: isOrbBreathing ? 13 * s : 8 * s)
                .offset(y: 34 * s)

            Circle()
                .fill(
                    RadialGradient(
                        colors: [
                            Color.white.opacity(0.90),
                            Color(hex: "4A5240").opacity(isOrbBreathing ? 0.64 : 0.48),
                            Color(hex: "C8B94A").opacity(isOrbBreathing ? 0.78 : 0.58),
                            Color.clear
                        ],
                        center: .center,
                        startRadius: 0,
                        endRadius: isOrbBreathing ? 56 * s : 46 * s
                    )
                )
                .frame(
                    width: isOrbBreathing ? 86 * s : 74 * s,
                    height: isOrbBreathing ? 86 * s : 74 * s
                )
                .blur(radius: isOrbBreathing ? 5 * s : 3 * s)
                .scaleEffect(isOrbBreathing ? 1.06 : 0.96)
                .shadow(
                    color: Color(hex: "4A5240").opacity(isOrbBreathing ? 0.28 : 0.14),
                    radius: isOrbBreathing ? 24 * s : 12 * s,
                    x: 0,
                    y: 0
                )
                .shadow(
                    color: Color(hex: "C8B94A").opacity(isOrbBreathing ? 0.32 : 0.16),
                    radius: isOrbBreathing ? 30 * s : 14 * s,
                    x: 0,
                    y: 8 * s
                )

            Text("✦")
                .font(.system(size: isOrbBreathing ? 32 * s : 28 * s, weight: .semibold))
                .foregroundStyle(.white.opacity(isOrbBreathing ? 0.86 : 0.68))
                .scaleEffect(isOrbBreathing ? 1.08 : 0.92)
        }
        .frame(height: 90 * s)
        .animation(
            .easeInOut(duration: 1.75).repeatForever(autoreverses: true),
            value: isOrbBreathing
        )
    }

    private func sakuComposer(scale s: CGFloat) -> some View {
        VStack(alignment: .leading, spacing: 28 * s) {
            Text("Ask KIRA anything...")
                .font(.system(size: 18 * s, weight: .medium, design: .rounded))
                .foregroundStyle(Color(red: 0.45, green: 0.47, blue: 0.60))

            HStack(spacing: 30 * s) {
                Button {
                    // Attach action.
                } label: {
                    Image(systemName: "paperclip")
                }
                .buttonStyle(.plain)

                Button {
                    // Camera action.
                } label: {
                    Image(systemName: "camera")
                }
                .buttonStyle(.plain)

                Spacer()

                Button {
                    // Send / ask action.
                } label: {
                    Circle()
                        .fill(
                            LinearGradient(
                                colors: [
                                    Color(hex: "4A5240").opacity(0.75),
                                    Color(hex: "4A5240")
                                ],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                        .frame(width: 43 * s, height: 43 * s)
                        .overlay(
                            Text("✦")
                                .font(.system(size: 24 * s, weight: .semibold))
                                .foregroundStyle(.white)
                        )
                        .shadow(color: Color(hex: "C8B94A").opacity(0.22), radius: 13 * s, x: 0, y: 7 * s)
                }
                .buttonStyle(.plain)
            }
            .font(.system(size: 23 * s, weight: .medium))
            .foregroundStyle(Color(hex: "4A5240"))
        }
        .padding(.horizontal, 26 * s)
        .padding(.top, 27 * s)
        .padding(.bottom, 20 * s)
        .frame(height: 138 * s)
        .background(.white.opacity(0.78), in: RoundedRectangle(cornerRadius: 27 * s, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 27 * s, style: .continuous)
                .stroke(Color(hex: "4A5240").opacity(0.34), lineWidth: 1)
        )
        .shadow(color: Color(hex: "4A5240").opacity(0.10), radius: 22 * s, x: 0, y: 11 * s)
    }

    private func suggestionsList(scale s: CGFloat) -> some View {
        VStack(alignment: .leading, spacing: 11 * s) {
            Text("Suggested for you")
                .font(.system(size: 16 * s, weight: .medium, design: .rounded))
                .foregroundStyle(Color(hex: "6B6B66"))
                .padding(.leading, 4 * s)
                .padding(.bottom, 4 * s)

            suggestionRow(icon: "chart.line.uptrend.xyaxis", title: "Why did I overspend this week?", tint: Color(hex: "4A5240"), scale: s)
            suggestionRow(icon: "viewfinder", title: "Scan a receipt", tint: Color(hex: "C8B94A"), scale: s)
            suggestionRow(icon: "folder", title: "Create a food budget", tint: Color(hex: "5B8C5A"), scale: s)
            suggestionRow(icon: "arrow.triangle.2.circlepath", title: "Show my subscriptions", tint: Color(hex: "4A5240"), scale: s)
        }
    }

    private func suggestionRow(icon: String, title: String, tint: Color, scale s: CGFloat) -> some View {
        Button {
            // Suggestion action.
        } label: {
            HStack(spacing: 17 * s) {
                Circle()
                    .fill(tint.opacity(0.16))
                    .frame(width: 45 * s, height: 45 * s)
                    .overlay(
                        Image(systemName: icon)
                            .font(.system(size: 20 * s, weight: .semibold))
                            .foregroundStyle(tint.opacity(0.78))
                    )

                Text(title)
                    .font(.system(size: 16 * s, weight: .medium, design: .rounded))
                    .foregroundStyle(Color(red: 0.18, green: 0.20, blue: 0.28))
                    .lineLimit(1)
                    .minimumScaleFactor(0.86)

                Spacer()

                Image(systemName: "chevron.right")
                    .font(.system(size: 15 * s, weight: .semibold))
                    .foregroundStyle(Color(hex: "C8B94A").opacity(0.60))
            }
            .padding(.horizontal, 17 * s)
            .frame(height: 58 * s)
            .background(.white.opacity(0.82), in: Capsule())
            .shadow(color: Color(hex: "C8B94A").opacity(0.035), radius: 13 * s, x: 0, y: 7 * s)
        }
        .buttonStyle(.plain)
    }

    private func quickActionBar(scale s: CGFloat) -> some View {
        HStack(spacing: 0) {
            bottomAction(icon: "plus", title: "Add transaction", scale: s)

            Divider()
                .frame(height: 32 * s)
                .opacity(0.35)

            bottomAction(icon: "chart.pie.fill", title: "Where did my money go?", scale: s)

            Divider()
                .frame(height: 32 * s)
                .opacity(0.35)

            bottomAction(icon: "viewfinder", title: "Show latest transfers", scale: s)
        }
        .padding(.horizontal, 15 * s)
        .frame(height: 66 * s)
        .background(.white.opacity(0.86), in: Capsule())
        .shadow(color: Color.blue.opacity(0.06), radius: 20 * s, x: 0, y: 10 * s)
    }

    private func bottomAction(icon: String, title: String, scale s: CGFloat) -> some View {
        Button {
            // Quick action.
        } label: {
            HStack(spacing: 8 * s) {
                Circle()
                    .fill(.white)
                    .frame(width: 31 * s, height: 31 * s)
                    .overlay(
                        Image(systemName: icon)
                            .font(.system(size: 15 * s, weight: .semibold))
                            .foregroundStyle(Color(hex: "4A5240"))
                    )

                Text(title)
                    .font(.system(size: 13 * s, weight: .semibold, design: .rounded))
                    .foregroundStyle(Color(red: 0.15, green: 0.17, blue: 0.24))
                    .lineLimit(2)
                    .minimumScaleFactor(0.72)
            }
            .frame(maxWidth: .infinity)
        }
        .buttonStyle(.plain)
    }

    // MARK: - Home Sheet

    private var homeSheet: some View {
        VStack(spacing: 0) {
            Image(systemName: "chevron.up")
                .font(.system(size: 28, weight: .semibold))
                .foregroundStyle(.gray.opacity(0.48))
                .frame(height: 58)
                .opacity(liveProgress)

            ScrollView(showsIndicators: false) {
                VStack(spacing: 16) {
                    balanceCard
                    payLaterCard
                    forYouSection
                    linkCard
                    placeholderList
                }
                .opacity(Double(1.0 - min(liveProgress * 1.35, 1.0)))
                .allowsHitTesting(liveProgress < 0.01)
                .padding(.horizontal, 16)
                .padding(.top, 10)
                .padding(.bottom, 140)
            }
            .scrollDisabled(liveProgress > 0.01)
            .onScrollGeometryChange(for: CGFloat.self) { geometry in
                geometry.contentOffset.y
            } action: { _, newValue in
                homeContentOffsetY = newValue
                homeScrollY = -newValue
                lastKnownHomeScrollY = -newValue
                isHomeAtTop = newValue <= 1
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(hex: "F5F2ED"))
        .clipShape(RoundedRectangle(cornerRadius: homeCornerRadius, style: .continuous))
        .shadow(color: .black.opacity(0.12), radius: 18, x: 0, y: -5)
        .ignoresSafeArea(edges: .bottom)
        .contentShape(Rectangle())
        .onTapGesture {
            if liveProgress > 0.85 {
                withAnimation(.interactiveSpring(response: 0.42, dampingFraction: 0.9, blendDuration: 0.12)) {
                    settledProgress = 0
                }
            }
        }
    }

    private var balanceCard: some View {
        VStack(spacing: 22) {
            VStack(spacing: 10) {
                HStack(spacing: 6) {
                    Text("Total balance")
                    Image(systemName: "chevron.down")
                        .font(.system(size: 11, weight: .bold))
                }
                .font(.system(size: 16, weight: .semibold, design: .rounded))
                .foregroundStyle(.gray)

                HStack(spacing: 8) {
                    Text("RM 20.08")
                        .font(.system(size: 34, weight: .bold, design: .rounded))
                    Image(systemName: "eye")
                        .font(.system(size: 18, weight: .bold))
                        .foregroundStyle(.gray)
                }

                HStack(spacing: 6) {
                    Text("Interest earned")
                        .foregroundStyle(.gray)
                    Text("+RM 11.75")
                        .foregroundStyle(.green)
                }
                .font(.system(size: 18, weight: .medium, design: .rounded))
            }
            .padding(.top, 20)

            HStack(spacing: 18) {
                quickAction("viewfinder", "Scan")
                quickAction("plus", "Add money")
                quickAction("arrow.down.left", "Receive")
                quickAction("arrow.up.right", "Transfer")
            }
            .padding(.bottom, 26)
        }
        .frame(maxWidth: .infinity)
        .background(.white, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
    }

    private func quickAction(_ icon: String, _ title: String) -> some View {
        VStack(spacing: 10) {
            Circle()
                .fill(Color(red: 0.0, green: 0.49, blue: 1.0))
                .frame(width: 70, height: 70)
                .overlay(
                    Image(systemName: icon)
                        .font(.system(size: 29, weight: .regular))
                        .foregroundStyle(.white)
                )

            Text(title)
                .font(.system(size: 14, weight: .semibold, design: .rounded))
                .foregroundStyle(.gray)
                .lineLimit(1)
                .minimumScaleFactor(0.75)
        }
        .frame(maxWidth: .infinity)
    }

    private var payLaterCard: some View {
        VStack(spacing: 0) {
            HStack {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Ryt PayLater")
                        .font(.system(size: 22, weight: .bold, design: .rounded))
                        .foregroundStyle(Color(red: 0, green: 0.57, blue: 0.50))

                    Text("Credit up to RM 1,499")
                        .font(.system(size: 19, weight: .regular, design: .rounded))
                        .foregroundStyle(.gray)
                }

                Spacer()

                Text("Get instantly")
                    .font(.system(size: 18, weight: .bold, design: .rounded))
                    .foregroundStyle(.blue)
                    .padding(.horizontal, 18)
                    .padding(.vertical, 12)
                    .overlay(Capsule().stroke(.blue, lineWidth: 1.8))
            }
            .padding(22)
            .background(.white)

            HStack(spacing: 12) {
                Image(systemName: "dollarsign.circle")

                Text("Used responsibly by thousands of users.")
                    .font(.system(size: 17, weight: .regular, design: .rounded))

                Spacer()

                Image(systemName: "chevron.right")
            }
            .padding(.horizontal, 22)
            .frame(height: 52)
            .background(Color.blue.opacity(0.13))
        }
        .clipShape(RoundedRectangle(cornerRadius: 17, style: .continuous))
    }

    private var forYouSection: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("For you")
                    .font(.system(size: 27, weight: .bold, design: .rounded))

                Spacer()

                Capsule()
                    .fill(Color.blue)
                    .frame(width: 36, height: 7)

                Capsule()
                    .fill(Color.gray.opacity(0.18))
                    .frame(width: 36, height: 7)
            }
            .padding(.top, 28)

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 14) {
                    forYouCard(
                        title: "RYT CARD",
                        body: "Enable Ryt PayLater\nfor 1.2% cashback",
                        color: .mint.opacity(0.48)
                    )

                    forYouCard(
                        title: "RYT REFERRAL",
                        body: "RM 99 duit raya\nwaiting for you!",
                        color: .green.opacity(0.23)
                    )

                    forYouCard(
                        title: "RYT CARD",
                        body: "Earn 1.2%\ncashback",
                        color: .cyan.opacity(0.35)
                    )
                }
            }
        }
    }

    private func forYouCard(title: String, body: String, color: Color) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            Text(title)
                .font(.system(size: 14, weight: .bold, design: .rounded))
                .kerning(2)
                .foregroundStyle(Color(red: 0, green: 0.45, blue: 0.43))

            Text(body)
                .font(.system(size: 20, weight: .medium, design: .rounded))
                .lineSpacing(3)

            Spacer()

            Text("Learn more ›")
                .font(.system(size: 18, weight: .bold, design: .rounded))
                .foregroundStyle(.blue)
        }
        .padding(20)
        .frame(width: 176, height: 188)
        .background(color, in: RoundedRectangle(cornerRadius: 17, style: .continuous))
    }

    private var linkCard: some View {
        HStack(spacing: 16) {
            Circle()
                .stroke(.blue.opacity(0.35), lineWidth: 4)
                .frame(width: 58, height: 58)
                .overlay(
                    Image(systemName: "link")
                        .font(.system(size: 22, weight: .bold))
                )

            Text("Link DuitNow ID")
                .font(.system(size: 21, weight: .bold, design: .rounded))

            Spacer()

            Text("Link now")
                .font(.system(size: 18, weight: .bold, design: .rounded))
                .foregroundStyle(.blue)
                .padding(.horizontal, 20)
                .padding(.vertical, 12)
                .overlay(Capsule().stroke(.blue, lineWidth: 1.8))
        }
        .padding(22)
        .background(.white, in: RoundedRectangle(cornerRadius: 17, style: .continuous))
    }

    private var placeholderList: some View {
        VStack(spacing: 12) {
            ForEach(Array(0..<12), id: \.self) { index in
                PlaceholderActivityRow(index: index)
            }
        }
        .padding(.top, 4)
    }

    // MARK: - Gesture

    private var homeDragGesture: some Gesture {
        DragGesture(minimumDistance: 3, coordinateSpace: .global)
            .updating($dragY) { value, state, _ in
                let draggingDown = value.translation.height > 0
                let draggingUp = value.translation.height < 0

                if draggingDown {
                    if settledProgress > 0 || (lockedCanRevealForThisDrag && homeContentOffsetY <= 1) {
                        state = value.translation.height * 0.94
                    }
                } else if draggingUp, settledProgress > 0 {
                    state = value.translation.height
                }
            }
            .onChanged { _ in
                if !dragSessionStarted {
                    dragSessionStarted = true

                    let reallyAtTop = homeContentOffsetY <= 1
                    lockedCanRevealForThisDrag = reallyAtTop || settledProgress > 0
                    gestureCanReveal = lockedCanRevealForThisDrag
                }
            }
            .onEnded { value in
                defer {
                    gestureCanReveal = false
                    lockedCanRevealForThisDrag = false
                    dragSessionStarted = false
                }

                guard settledProgress > 0 || (lockedCanRevealForThisDrag && homeContentOffsetY <= 1) else {
                    return
                }

                let predictedProgress = settledProgress + value.predictedEndTranslation.height / revealDistance
                let currentProgress = settledProgress + value.translation.height / revealDistance
                let shouldOpen = predictedProgress > 0.34 || currentProgress > 0.5 || value.velocity.height > 700

                settledProgress = shouldOpen ? 1 : 0
            }
    }
}

private struct PlaceholderActivityRow: View {
    let index: Int

    private var isExpense: Bool { index % 2 == 0 }
    private var title: String { isExpense ? "Recent transfer" : "Cashback reward" }
    private var subtitle: String { isExpense ? "Today • Ryt Account" : "Yesterday • Promotion" }
    private var amount: String { isExpense ? "-RM 12.00" : "+RM 1.20" }
    private var iconName: String { isExpense ? "creditcard" : "arrow.down.circle" }
    private var iconBackground: Color { isExpense ? Color.blue.opacity(0.15) : Color.green.opacity(0.15) }
    private var amountColor: Color { isExpense ? Color.primary : Color.green }

    var body: some View {
        HStack(spacing: 14) {
            Circle()
                .fill(iconBackground)
                .frame(width: 46, height: 46)
                .overlay(
                    Image(systemName: iconName)
                        .foregroundStyle(.blue)
                )

            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.system(size: 17, weight: .semibold, design: .rounded))

                Text(subtitle)
                    .font(.system(size: 14, weight: .medium, design: .rounded))
                    .foregroundStyle(.gray)
            }

            Spacer()

            Text(amount)
                .font(.system(size: 16, weight: .bold, design: .rounded))
                .foregroundStyle(amountColor)
        }
        .padding(16)
        .background(.white, in: RoundedRectangle(cornerRadius: 17, style: .continuous))
    }
}

private struct ScrollOffsetPreferenceKey: PreferenceKey {
    static var defaultValue: CGFloat = 0

    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) {
        value = nextValue()
    }
}

#Preview {
    ContentView()
}
