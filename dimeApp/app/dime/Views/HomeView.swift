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
        max(156, min(184, h * 0.19))
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

                // Home sheet — slides down to reveal AI behind it
                homeSheet(peekHeight: peek)
                    .clipShape(RoundedRectangle(cornerRadius: sheetCornerRadius, style: .continuous))
                    .shadow(color: .black.opacity(isLogTab ? 0.12 : 0), radius: 18, x: 0, y: -5)
                    .ignoresSafeArea(edges: .bottom)
                    .offset(y: sheetY)
                    .animation(.easeOut(duration: 0.22), value: isLogTab)
                    .contentShape(Rectangle())
                    .onTapGesture {
                        guard liveProgress > 0.85 else { return }
                        withAnimation(settleAnimation) { settledProgress = 0 }
                        DispatchQueue.main.asyncAfter(deadline: .now() + 0.27) {
                            homeAIAssistantViewModel.collapse()
                        }
                    }
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
                        settledProgress = 1
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
                    settledProgress = 1
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

                InsightsView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .tag("Insights")

                BudgetView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .tag("Budget")

                SettingsView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .tag("Settings")
            }
            .allowsHitTesting(showPopup ? false : !homeAIAssistantViewModel.isPresented)
            .environmentObject(toastPresenter)
            .environmentObject(transactionManager)

        }
        .background(Color.PrimaryBackground)
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
            RadialGradient(colors: [Color.cyan.opacity(0.30), Color.cyan.opacity(0.10), .clear], center: .bottomLeading, startRadius: 20, endRadius: 380)
            RadialGradient(colors: [Color.purple.opacity(0.22), Color.purple.opacity(0.08), .clear], center: .bottomTrailing, startRadius: 20, endRadius: 420)
            RadialGradient(colors: [Color.cyan.opacity(0.16), .clear], center: .topLeading, startRadius: 20, endRadius: 300)
        }
    }

    private func collapsedHeader(onReveal: @escaping () -> Void) -> some View {
        HStack(alignment: .center) {
            Button { } label: {
                Circle()
                    .fill(LinearGradient(
                        colors: [Color(red: 0.36, green: 0.83, blue: 0.91), Color(red: 0.49, green: 0.79, blue: 0.96)],
                        startPoint: .topLeading, endPoint: .bottomTrailing
                    ))
                    .frame(width: 48, height: 48)
                    .overlay(Text("ME").font(.system(size: 17, weight: .bold, design: .rounded)).foregroundStyle(.white))
            }
            .buttonStyle(.plain)
            .frame(width: 58, alignment: .leading)

            Spacer(minLength: 0)

            Button { onReveal() } label: {
                HStack(spacing: 8) {
                    Text("✦").font(.system(size: 22, weight: .semibold)).foregroundStyle(.white)
                    Text("Ask Saku AI").font(.system(size: 18, weight: .bold, design: .rounded)).foregroundStyle(.white)
                }
                .padding(.horizontal, 19)
                .frame(height: 48)
                .background(Capsule().fill(.white.opacity(0.11)))
                .background(Capsule().fill(LinearGradient(
                    colors: [.white.opacity(0.22), .white.opacity(0.07), .cyan.opacity(0.08)],
                    startPoint: .topLeading, endPoint: .bottomTrailing
                )))
                .overlay {
                    Capsule().stroke(LinearGradient(
                        colors: [Color.cyan.opacity(0.95), Color.white.opacity(0.62), Color.purple.opacity(0.65)],
                        startPoint: .topLeading, endPoint: .bottomTrailing
                    ), lineWidth: 1.5)
                }
                .shadow(color: Color.cyan.opacity(0.24), radius: 10, x: 0, y: 0)
                .shadow(color: Color.purple.opacity(0.18), radius: 13, x: 0, y: 7)
                .contentShape(Capsule())
            }
            .buttonStyle(.plain)

            Spacer(minLength: 0)

            Button { } label: {
                Image(systemName: "bell")
                    .font(.system(size: 22, weight: .medium))
                    .foregroundStyle(Color(red: 0.43, green: 0.36, blue: 0.90))
                    .frame(width: 48, height: 48)
                    .overlay(alignment: .topTrailing) {
                        Circle().fill(Color.AlertRed).frame(width: 9, height: 9).offset(x: 1, y: 4)
                    }
            }
            .buttonStyle(.plain)
            .frame(width: 58, alignment: .trailing)
        }
        .padding(.horizontal, 30)
    }
}

struct AppLockView: View {
    @EnvironmentObject var appLockVM: AppLockViewModel

    var body: some View {
        VStack(spacing: 15) {
            Image(systemName: "lock.fill")
                .font(.system(size: 65))
                .foregroundColor(Color.DarkIcon.opacity(0.7))

            Text("App Locked")
                .font(.system(size: 28, weight: .semibold, design: .rounded))
                .foregroundColor(Color.PrimaryText)
                .padding(.bottom, 30)

            Button {
                appLockVM.appLockValidation()
            } label: {
                HStack {
                    Image(systemName: "faceid")
                    Text("Unlock App")
                }
                .font(.system(size: 20, weight: .medium, design: .rounded))
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
                    .font(.system(size: 15, weight: .regular, design: .rounded))
                    .foregroundColor(Color.SubtitleText)
                    .multilineTextAlignment(.center)
            }
        }
        .padding(.horizontal, 30)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.PrimaryBackground)
    }
}
