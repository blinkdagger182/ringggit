//
//  HomeView.swift
//  xpenz
//
//  Created by Rafael Soh on 20/5/22.
//

import ConfettiSwiftUI
import Foundation
import SwiftUI

class OverallToastPresenter: ObservableObject {
    @Published var showToast: Bool = false
}

enum DeletionType {
    case instant
    case prompt
}

class OverallTransactionManager: ObservableObject {
    @Published var toEdit: Transaction?
    @Published var toDelete: Transaction?
    @Published var showToast: Bool = false
    @Published var showPopup: Bool = false
    @Published var future: Bool = false
}

struct HomeView: View {
    @EnvironmentObject var appLockVM: AppLockViewModel

    @StateObject var toastPresenter = OverallToastPresenter()
    @StateObject var transactionManager = OverallTransactionManager()
    @StateObject private var homeAIAssistantViewModel = HomeAIAssistantViewModel()
    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController

    @Namespace private var homeAINamespace

    @State var currentTab = "Log"

    // ryt-clone drag model
    @State private var settledProgress: CGFloat = 0
    @State private var dragY: CGFloat = 0
    @State private var collapseGestureDragY: CGFloat = 0
    @State private var containerHeight: CGFloat = 852
    @State private var isLogAtTop: Bool = true
    @State private var openAttachmentOnAIExpand: Bool = false
    @State private var dragSessionStarted: Bool = false
    @State private var lockedCanRevealForThisDrag: Bool = false

    var topEdge: CGFloat
    var bottomEdge: CGFloat

    @State var fromURL1: Bool = false
    @State var fromURL2: Bool = false
    @State var fromURL3: Bool = false
    @State var fromURL4: Bool = false

    @State var launchAdd: Bool = false
    @State var launchSearch: Bool = false

    @State var counter = 0

    @EnvironmentObject var tabBarManager: TabBarManager

    @State var showPopup = false

    init(topEdge: CGFloat, bottomEdge: CGFloat) {
        UITabBar.appearance().isHidden = true
        self.topEdge = topEdge
        self.bottomEdge = bottomEdge
    }

    // MARK: - Layout

    private var collapsedTopInset: CGFloat { topEdge + 68 }

    private func peekHeight(for h: CGFloat) -> CGFloat {
        max(72, min(88, h * 0.10))
    }

    private var revealDistance: CGFloat {
        max(containerHeight - collapsedTopInset - peekHeight(for: containerHeight), 1)
    }

    private var liveProgress: CGFloat {
        min(max(settledProgress + (dragY + collapseGestureDragY) / revealDistance, 0), 1)
    }

    private var settleAnimation: Animation {
        .interactiveSpring(response: 0.42, dampingFraction: 0.9, blendDuration: 0.12)
    }

    // MARK: - Body

    var body: some View {
        GeometryReader { proxy in
            let h = proxy.size.height
            let peek = peekHeight(for: h)
            let rd = max(h - collapsedTopInset - peek, 1)
            let isLogTab = currentTab == "Log"
            let sheetY: CGFloat = isLogTab ? (collapsedTopInset + liveProgress * rd) : 0
            let sheetCornerRadius: CGFloat = isLogTab ? 38 : 0

            ZStack(alignment: .top) {
                Color.black.ignoresSafeArea()

                if currentTab == "Log" {
                    aiBackdrop
                        .ignoresSafeArea()

                    HomeAIAssistantOverlay(
                        viewModel: homeAIAssistantViewModel,
                        namespace: homeAINamespace,
                        topInset: topEdge,
                        bottomInset: bottomEdge,
                        revealProgress: liveProgress,
                        isExpanded: homeAIAssistantViewModel.isPresented,
                        homePeekHeight: peek,
                        openAttachmentOnExpand: $openAttachmentOnAIExpand,
                        onCollapse: { collapseAI() },
                        collapseGesture: makeCollapseGesture(rd: rd)
                    )
                    .ignoresSafeArea()
                    .allowsHitTesting(homeAIAssistantViewModel.isPresented)
                }

                // Home sheet — above overlay so peek is always visible.
                // contentShape limits hit area to peek strip when AI is presented,
                // letting touches above peek fall through to the overlay.
                homeSheet(peekHeight: peek)
                    .clipShape(RoundedRectangle(cornerRadius: sheetCornerRadius, style: .continuous))
                    .shadow(color: .black.opacity(isLogTab ? 0.12 : 0), radius: 18, x: 0, y: -5)
                    .ignoresSafeArea(edges: .bottom)
                    .contentShape(HomeSheetHitShape(
                        peekHeight: peek,
                        limitToPeek: homeAIAssistantViewModel.isPresented
                    ))
                    .onTapGesture {
                        guard homeAIAssistantViewModel.isPresented else { return }
                        collapseAI()
                    }
                    .offset(y: sheetY)
                    .animation(.easeOut(duration: 0.22), value: isLogTab)
                    .simultaneousGesture(makeDragGesture(rd: rd))

                // Tab bar pinned to screen bottom, outside offset sheet
                CustomTabBar(
                    currentTab: $currentTab,
                    topEdge: topEdge,
                    bottomEdge: bottomEdge,
                    counter: $counter,
                    launchAdd: launchAdd
                )
                .frame(maxHeight: .infinity, alignment: .bottom)
                .opacity(homeAIAssistantViewModel.isPresented ? 0 : 1)
                .allowsHitTesting(!homeAIAssistantViewModel.isPresented)

                // Floating collapsed header — fades out as AI reveals
                if currentTab == "Log" {
                    collapsedHeader(onReveal: {
                        withAnimation(settleAnimation) { settledProgress = 1 }
                        homeAIAssistantViewModel.expand()
                    })
                    .padding(.top, max(topEdge, proxy.safeAreaInsets.top) + 10)
                    .opacity(max(0, 1.0 - liveProgress * 1.35))
                    .allowsHitTesting(liveProgress < 0.05)
                }

                // Popups above everything
                if showPopup {
                    Color.clear
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .contentShape(Rectangle())
                        .onTapGesture { transactionManager.showPopup = false }
                }

                DeleteTransactionAlert()
                    .offset(y: showPopup ? 0 : 300)
                    .environmentObject(transactionManager)

                if appLockVM.isAppLockEnabled && !appLockVM.isAppUnLocked {
                    AppLockView()
                        .ignoresSafeArea(.all)
                        .onOpenURL { url in
                            if url.host == "newExpense" { fromURL1 = true }
                            else if url.host == "search" { fromURL2 = true }
                            else if url.host == "insights" { fromURL3 = true }
                            else if url.host == "budget" { fromURL4 = true }
                        }
                }
            }
            .onAppear { containerHeight = h }
            .onChange(of: h) { containerHeight = $0 }
        }
        .toast(isPresenting: $toastPresenter.showToast, duration: 4, tapToDismiss: true, offsetY: 12, alert: {
            AlertToast(displayMode: .hud, type: .systemImage("checkmark.circle.fill", Color.IncomeGreen), title: "Image Saved", subTitle: "Check it out in Photos")
        })
        .toast(isPresenting: $transactionManager.showToast, duration: 4, tapToDismiss: true, offsetY: 12, alert: {
            AlertToast(displayMode: .hud, type: .systemImage("arrow.uturn.backward.circle.fill", Color.AlertRed), title: "Log Deleted", subTitle: "Tap to Undo")
        }, onTap: {
            withAnimation(.easeInOut(duration: 0.5)) { moc.rollback() }
            transactionManager.toDelete = nil
        }, completion: {
            dataController.save()
            transactionManager.toDelete = nil
        })
        .onChange(of: transactionManager.showPopup) { newValue in
            withAnimation { showPopup = newValue }
        }
        .onChange(of: currentTab) { _ in
            settledProgress = 0
            homeAIAssistantViewModel.collapse()
        }
        .fullScreenCover(item: $transactionManager.toEdit, onDismiss: {
            transactionManager.toEdit = nil
        }) { transaction in
            TransactionView(toEdit: transaction)
        }
        .confettiCannon(counter: $counter, num: 50, openingAngle: Angle(degrees: 0), closingAngle: Angle(degrees: 360), radius: 200)
        .onAppear {
            homeAIAssistantViewModel.dataController = dataController
            homeAIAssistantViewModel.reloadWorkspaces()

            if appLockVM.isAppLockEnabled && fromURL1 {
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { launchAdd.toggle() }
                fromURL1 = false
            }
            if appLockVM.isAppLockEnabled && fromURL2 {
                currentTab = "Log"
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { launchSearch.toggle() }
                fromURL2 = false
            }
            if appLockVM.isAppLockEnabled && fromURL3 { currentTab = "Insights" }
            if appLockVM.isAppLockEnabled && fromURL4 { currentTab = "Budget" }
        }
        .onOpenURL { url in
            if url.host == "search" { currentTab = "Log" }
            else if url.host == "insights" { currentTab = "Insights" }
            else if url.host == "budget" { currentTab = "Budget" }
            else if url.host == "aioverlay" {
                currentTab = "Log"
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                    withAnimation(settleAnimation) { settledProgress = 1 }
                    homeAIAssistantViewModel.expand()
                }
            }
        }
    }

    // MARK: - Home sheet

    private func homeSheet(peekHeight: CGFloat) -> some View {
        ZStack(alignment: .bottom) {
            TabView(selection: $currentTab) {
                LogView(
                    topEdge: 0,
                    bottomEdge: bottomEdge,
                    launchSearch: launchSearch,
                    isScrollLocked: liveProgress > 0.01,
                    onScrollStateChanged: { isAtTop, _ in isLogAtTop = isAtTop },
                    onAddTransaction: { launchAdd.toggle() },
                    onScanReceipt: {
                        openAttachmentOnAIExpand = true
                        settledProgress = 1
                        homeAIAssistantViewModel.expand()
                    }
                )
                .ignoresSafeArea(.all)
                .tag("Log")

                BudgetView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .tag("Budget")

                InsightsView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .tag("Insights")

                SettingsView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .tag("Settings")
            }
            .allowsHitTesting(showPopup ? false : !homeAIAssistantViewModel.isPresented)
            .environmentObject(toastPresenter)
            .environmentObject(transactionManager)
            .opacity(max(0, 1.0 - Double(liveProgress) * 3.0))

        }
        .background(liveProgress > 0.01 ? Color.white : Color.PrimaryBackground)
    }

    // MARK: - Gestures

    private func makeDragGesture(rd: CGFloat) -> some Gesture {
        DragGesture(minimumDistance: 4, coordinateSpace: .global)
            .onChanged { value in
                guard currentTab == "Log" else { return }

                if !dragSessionStarted {
                    dragSessionStarted = true
                    lockedCanRevealForThisDrag = isLogAtTop || settledProgress > 0
                }

                let t = value.translation.height
                if t > 0 {
                    if settledProgress > 0 || (lockedCanRevealForThisDrag && isLogAtTop) {
                        dragY = t * 0.94
                    }
                } else if t < 0, settledProgress > 0 {
                    dragY = t
                }
            }
            .onEnded { value in
                guard currentTab == "Log" else { return }
                defer {
                    dragSessionStarted = false
                    lockedCanRevealForThisDrag = false
                }

                guard settledProgress > 0 || (lockedCanRevealForThisDrag && isLogAtTop) else {
                    withAnimation(settleAnimation) { dragY = 0 }
                    return
                }

                let finalT = dragY
                let predictedT = value.predictedEndTranslation.height
                let currentP = min(max(settledProgress + finalT / rd, 0), 1)
                let predictedP = min(max(settledProgress + predictedT / rd, 0), 1)
                let vel = predictedT - finalT
                let shouldReveal = predictedP > 0.34 || currentP > 0.5 || vel > 700
                withAnimation(settleAnimation) {
                    settledProgress = shouldReveal ? 1 : 0
                    dragY = 0
                }
                if shouldReveal {
                    homeAIAssistantViewModel.expand()
                } else if homeAIAssistantViewModel.isPresented {
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.27) {
                        homeAIAssistantViewModel.collapse()
                    }
                }
            }
    }

    private func makeCollapseGesture(rd: CGFloat) -> AnyGesture<DragGesture.Value> {
        AnyGesture(
            DragGesture(minimumDistance: 4, coordinateSpace: .global)
                .onChanged { value in
                    collapseGestureDragY = value.translation.height
                }
                .onEnded { value in
                    let finalT = collapseGestureDragY
                    let predictedT = value.predictedEndTranslation.height
                    let currentP = min(max(settledProgress + finalT / rd, 0), 1)
                    let predictedP = min(max(settledProgress + predictedT / rd, 0), 1)
                    let vel = predictedT - finalT
                    let shouldReveal = predictedP > 0.34 || currentP > 0.5 || vel > 700
                    withAnimation(settleAnimation) {
                        settledProgress = shouldReveal ? 1 : 0
                        collapseGestureDragY = 0
                    }
                    if !shouldReveal {
                        DispatchQueue.main.asyncAfter(deadline: .now() + 0.27) {
                            homeAIAssistantViewModel.collapse()
                        }
                    }
                }
        )
    }

    // MARK: - Actions

    private func collapseAI() {
        withAnimation(settleAnimation) { settledProgress = 0 }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.27) {
            homeAIAssistantViewModel.collapse()
        }
    }

    // MARK: - UI

    private var aiBackdrop: some View {
        ZStack {
            Color(hex: "3E4933")
            RadialGradient(colors: [Color(hex: "5C6B4A").opacity(0.60), .clear], center: .topLeading, startRadius: 0, endRadius: 320)
            RadialGradient(colors: [Color(hex: "2E3828").opacity(0.70), .clear], center: .bottomTrailing, startRadius: 0, endRadius: 400)
        }
    }

    private func collapsedHeader(onReveal: @escaping () -> Void) -> some View {
        HStack(alignment: .center) {
            Button { } label: {
                Circle()
                    .fill(Color(hex: "4A5240"))
                    .frame(width: 44, height: 44)
                    .overlay(Text("ME").font(Font.satoshi(14, weight: .bold)).foregroundStyle(.white))
            }
            .buttonStyle(.plain)
            .frame(width: 58, alignment: .leading)

            Spacer(minLength: 0)

            Button { onReveal() } label: {
                HStack(spacing: 9) {
                    Image("kira-ai")
                        .resizable()
                        .scaledToFill()
                        .frame(width: 22, height: 22)
                        .clipShape(RoundedRectangle(cornerRadius: 5, style: .continuous))
                    Text("Ask KIRA")
                        .font(Font.satoshi(17, weight: .semibold))
                        .foregroundStyle(.white)
                }
                .padding(.horizontal, 18)
                .frame(height: 46)
                .background(Capsule().fill(Color.white.opacity(0.10)))
                .background(Capsule().fill(
                    LinearGradient(
                        colors: [Color.white.opacity(0.18), Color.white.opacity(0.05), Color(hex: "3E4933").opacity(0.06)],
                        startPoint: .topLeading, endPoint: .bottomTrailing
                    )
                ))
                .overlay {
                    Capsule().stroke(
                        LinearGradient(
                            colors: [
                                Color.white.opacity(0.90),
                                Color(hex: "C8B94A").opacity(0.72),
                                Color(hex: "5B8C5A").opacity(0.55),
                                Color.white.opacity(0.40)
                            ],
                            startPoint: .topLeading,
                            endPoint: .bottomTrailing
                        ),
                        lineWidth: 1.4
                    )
                }
                .shadow(color: Color.white.opacity(0.14), radius: 12, x: 0, y: 0)
                .shadow(color: Color(hex: "C8B94A").opacity(0.20), radius: 16, x: 0, y: 6)
                .contentShape(Capsule())
            }
            .buttonStyle(.plain)

            Spacer(minLength: 0)

            Button { } label: {
                Image(systemName: "bell")
                    .font(Font.satoshi(20, weight: .medium))
                    .foregroundStyle(Color(hex: "4A5240"))
                    .frame(width: 48, height: 48)
                    .overlay(alignment: .topTrailing) {
                        Circle().fill(Color.AlertRed).frame(width: 8, height: 8).offset(x: 1, y: 4)
                    }
            }
            .buttonStyle(.plain)
            .frame(width: 58, alignment: .trailing)
        }
        .padding(.horizontal, 30)
    }
}

// Hit-test shape for homeSheet: full rect normally, top-peek-only when AI is presented.
// The peek strip is the TOP `peekHeight` pixels of the layout frame, which is visually
// the visible strip at the bottom of the screen (because the sheet is offset down by sheetY).
private struct HomeSheetHitShape: Shape {
    let peekHeight: CGFloat
    let limitToPeek: Bool

    func path(in rect: CGRect) -> Path {
        Path(CGRect(
            x: rect.minX,
            y: rect.minY,
            width: rect.width,
            height: limitToPeek ? peekHeight : rect.height
        ))
    }
}

struct AppLockView: View {
    @EnvironmentObject var appLockVM: AppLockViewModel

    var body: some View {
        VStack(spacing: 15) {
            Image(systemName: "lock.fill")
                .font(Font.satoshi(65))
                .foregroundColor(Color.DarkIcon.opacity(0.7))

            Text("App Locked")
                .font(Font.satoshi(28, weight: .semibold))
                .foregroundColor(Color.PrimaryText)
                .padding(.bottom, 30)

            Button {
                appLockVM.appLockValidation()
            } label: {
                HStack {
                    Image(systemName: "faceid")
                    Text("Unlock App")
                }
                .font(Font.satoshi(20, weight: .medium))
                .foregroundColor(Color.PrimaryText)
                .padding(.horizontal, 40)
                .padding(.vertical, 15)
                .overlay {
                    RoundedRectangle(cornerRadius: 13)
                        .stroke(Color.Outline)
                }
            }

            if appLockVM.enrollmentError {
                Text("Please re-enable Face ID access in the Settings app to unlock application.")
                    .font(Font.satoshi(15, weight: .regular))
                    .foregroundColor(Color.SubtitleText)
                    .multilineTextAlignment(.center)
            }
        }
        .padding(.horizontal, 30)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.PrimaryBackground)
    }
}
