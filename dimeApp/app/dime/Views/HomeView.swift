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
    @StateObject private var keyboardHeightHelper = KeyboardHeightHelper()
    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController

    @Namespace private var homeAINamespace

    @State var currentTab = "Log"
    @State private var homeAISheetOffset: CGFloat = 0
    @State private var isBarGestureActive: Bool = false
    @State private var isLogAtTop: Bool = true
    @State private var isLogIdle: Bool = true
    @State private var logSurfaceTopY: CGFloat = 0

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

    private let homeAIPullThreshold: CGFloat = 132
    private let homeAIMaxPullDistance: CGFloat = 360

    init(topEdge: CGFloat, bottomEdge: CGFloat) {
        UITabBar.appearance().isHidden = true
        self.topEdge = topEdge
        self.bottomEdge = bottomEdge
    }

    var body: some View {
        GeometryReader { proxy in
            let homeAIPeekHeight = targetHomeAIPeekHeight(for: proxy.size.height)
            let homeAIOpenOffset = targetHomeAIOpenOffset(for: proxy.size.height)
            let homeAIProgress = homeAIRevealProgress(openOffset: homeAIOpenOffset)

            ZStack(alignment: .bottom) {
                if currentTab == "Log" {
                    homeAISurfaceBackdrop
                        .ignoresSafeArea()
                        .zIndex(-1)
                }

                if currentTab == "Log" {
                    HomeAIAssistantOverlay(
                        viewModel: homeAIAssistantViewModel,
                        namespace: homeAINamespace,
                        topInset: topEdge,
                        bottomInset: bottomEdge,
                        revealProgress: homeAIProgress,
                        isExpanded: homeAIAssistantViewModel.isPresented,
                        homePeekHeight: homeAIPeekHeight,
                        onCollapse: { collapseAI() },
                        collapseGesture: homeAIPullGesture(openOffset: homeAIOpenOffset)
                    )
                    .ignoresSafeArea()
                    .allowsHitTesting(homeAIAssistantViewModel.isPresented)
                    .zIndex(0)
                }

                if currentTab == "Log", !homeAIAssistantViewModel.isPresented {
                    homeAIHomeHeader
                        .padding(.horizontal, 20)
                        .padding(.top, max(topEdge, proxy.safeAreaInsets.top) + 10)
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                        .opacity(max(CGFloat(0), CGFloat(1) - (homeAIProgress * CGFloat(1.35))))
                        .allowsHitTesting(false)
                        .zIndex(0.4)
                }

                TabView(selection: $currentTab) {
                    LogView(
                        topEdge: topEdge,
                        bottomEdge: bottomEdge,
                        launchSearch: launchSearch,
                        isScrollLocked: homeAISheetOffset > 0 || homeAIAssistantViewModel.isPresented,
                        onHomeSurfaceTopChanged: { topY in
                            guard !homeAIAssistantViewModel.isPresented, homeAISheetOffset < 1 else { return }
                            if abs(logSurfaceTopY - topY) > 0.5 {
                                logSurfaceTopY = topY
                            }
                        },
                        onScrollStateChanged: { isAtTop, isIdle in
                            isLogAtTop = isAtTop
                            isLogIdle = isIdle
                        },
                        onScrollRevealGesture: { state, t, v in
                            handleScrollRevealGesture(state: state, translationY: t, velocityY: v)
                        }
                    )
                        .ignoresSafeArea(.all)
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
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
                .mask(
                    Group {
                        if currentTab == "Log" {
                            HomeAISurfaceMaskShape(
                                topInset: max(0, logSurfaceTopY),
                                cornerRadius: currentTab == "Log" ? 38 : 0
                            )
                        } else {
                            Rectangle()
                        }
                    }
                )
                .offset(y: currentTab == "Log" ? homeAISheetOffset : 0)
                .simultaneousGesture(homeAIPullGesture(openOffset: homeAIOpenOffset))
                .shadow(
                    color: Color.black.opacity(currentTab == "Log" ? (0.08 + homeAIProgress * 0.08) : 0),
                    radius: currentTab == "Log" ? (12 + homeAIProgress * 20) : 0,
                    y: currentTab == "Log" ? (6 + homeAIProgress * 10) : 0
                )
                .zIndex(1)

                CustomTabBar(currentTab: $currentTab, topEdge: topEdge, bottomEdge: bottomEdge, counter: $counter, launchAdd: launchAdd)
                    .offset(y: (currentTab == "Log" ? homeAISheetOffset : 0) + (tabBarManager.hideTab ? (70 + bottomEdge) : 0))
                    .allowsHitTesting(!homeAIAssistantViewModel.isPresented)
                    .zIndex(1.05)

                if showPopup {
                    Rectangle()
                        .fill(Color.clear)
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .contentShape(Rectangle())
                        .onTapGesture {
                            transactionManager.showPopup = false
                        }
                }

                DeleteTransactionAlert()
                    .offset(y: showPopup ? 0 : 300)
                    .environmentObject(transactionManager)

                if appLockVM.isAppLockEnabled && !appLockVM.isAppUnLocked {
                    AppLockView()
                        .ignoresSafeArea(.all)
                        .onOpenURL { url in
                            if url.host == "newExpense" {
                                fromURL1 = true
                            } else if url.host == "search" {
                                fromURL2 = true
                            } else if url.host == "insights" {
                                fromURL3 = true
                            } else if url.host == "budget" {
                                fromURL4 = true
                            }
                        }
                }
            }
        }
        .toast(isPresenting: $toastPresenter.showToast, duration: 4, tapToDismiss: true, offsetY: 12, alert: {
            AlertToast(displayMode: .hud, type: .systemImage("checkmark.circle.fill", Color.IncomeGreen), title: "Image Saved", subTitle: "Check it out in Photos")
        })
        .toast(isPresenting: $transactionManager.showToast, duration: 4, tapToDismiss: true, offsetY: 12, alert: {
            AlertToast(displayMode: .hud, type: .systemImage("arrow.uturn.backward.circle.fill", Color.AlertRed), title: "Log Deleted", subTitle: "Tap to Undo")
        }, onTap: {
            withAnimation(.easeInOut(duration: 0.5)) {
                moc.rollback()
            }
            transactionManager.toDelete = nil
        }, completion: {
            dataController.save()
            transactionManager.toDelete = nil
        })
        .onChange(of: transactionManager.showPopup) { newValue in
            withAnimation {
                showPopup = newValue
            }
        }
        .onChange(of: currentTab) { newValue in
            withAnimation(homeAISettleAnimation) {
                homeAISheetOffset = 0
            }

            if newValue != "Log" {
                withAnimation(homeAISettleAnimation) {
                    homeAIAssistantViewModel.collapse()
                }
            }
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
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                    launchAdd.toggle()
                }

                fromURL1 = false
            }

            if appLockVM.isAppLockEnabled && fromURL2 {
                currentTab = "Log"

                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                    launchSearch.toggle()
                }

                fromURL2 = false
            }

            if appLockVM.isAppLockEnabled && fromURL3 {
                currentTab = "Insights"
            }

            if appLockVM.isAppLockEnabled && fromURL4 {
                currentTab = "Budget"
            }
        }
        .onOpenURL { url in
            if url.host == "search" {
                currentTab = "Log"
            } else if url.host == "insights" {
                currentTab = "Insights"
            } else if url.host == "budget" {
                currentTab = "Budget"
            } else if url.host == "aioverlay" {
                currentTab = "Log"
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                    withAnimation(homeAISettleAnimation) {
                        homeAIAssistantViewModel.expand()
                        homeAISheetOffset = targetHomeAIOpenOffset(for: UIScreen.main.bounds.height)
                    }
                }
            }
        }
    }

    private func homeAIPullGesture(openOffset: CGFloat) -> AnyGesture<DragGesture.Value> {
        AnyGesture(
            DragGesture(minimumDistance: 4, coordinateSpace: .global)
                .onChanged { value in
                    guard currentTab == "Log", homeAIAssistantViewModel.isPresented else { return }
                    isBarGestureActive = true
                    let isKeyboardPresented = keyboardHeightHelper.keyboardHeight > 0
                    if isKeyboardPresented {
                        if value.translation.height > 8 { UIApplication.shared.endEditing() }
                        return
                    }
                    if value.translation.height < 0 {
                        UIApplication.shared.endEditing()
                        homeAISheetOffset = max(0, openOffset + value.translation.height)
                    } else if value.translation.height > 0 {
                        if value.translation.height > 8 { UIApplication.shared.endEditing() }
                        homeAISheetOffset = min(openOffset + (rubberBandPullDistance(for: value.translation.height) * 0.08), openOffset + 18)
                    }
                }
                .onEnded { value in
                    isBarGestureActive = false
                    guard homeAIAssistantViewModel.isPresented else { return }
                    guard keyboardHeightHelper.keyboardHeight == 0 else {
                        withAnimation(homeAISettleAnimation) { homeAISheetOffset = openOffset }
                        return
                    }
                    let collapseDistance = max(0, openOffset - homeAISheetOffset)
                    let shouldCollapse = value.predictedEndTranslation.height < -180 || collapseDistance > (openOffset * 0.22)
                    if shouldCollapse {
                        collapseAI()
                    } else {
                        withAnimation(homeAISettleAnimation) { homeAISheetOffset = openOffset }
                    }
                }
        )
    }

    private func handleScrollRevealGesture(state: UIGestureRecognizer.State, translationY: CGFloat, velocityY: CGFloat) {
        guard currentTab == "Log", !homeAIAssistantViewModel.isPresented else { return }
        let openOffset = targetHomeAIOpenOffset(for: UIScreen.main.bounds.height)

        switch state {
        case .changed:
            if translationY > 0 {
                guard (isLogAtTop && isLogIdle) || homeAISheetOffset > 0 else { return }
                isBarGestureActive = true
                homeAISheetOffset = min(rubberBandPullDistance(for: translationY), openOffset)
            } else if translationY < 0, homeAISheetOffset > 0 {
                isBarGestureActive = true
                homeAISheetOffset = max(0, homeAISheetOffset + translationY * 0.2)
            }
        case .ended, .cancelled:
            isBarGestureActive = false
            guard homeAISheetOffset > 0 else { return }
            let predicted = max(max(translationY + velocityY * 0.2, translationY), 0)
            let predictedOffset = min(rubberBandPullDistance(for: predicted), openOffset)
            withAnimation(homeAISettleAnimation) {
                if predictedOffset > homeAIPullThreshold {
                    homeAISheetOffset = openOffset
                    homeAIAssistantViewModel.expand()
                } else {
                    homeAISheetOffset = 0
                }
            }
        default:
            break
        }
    }

    private func homeAIRevealProgress(openOffset: CGFloat) -> CGFloat {
        guard openOffset > 0 else { return 0 }
        return min(max(homeAISheetOffset / openOffset, 0), 1)
    }

    private var homeAISettleAnimation: Animation {
        .easeOut(duration: 0.24)
    }

    private var homeAISurfaceBackdrop: some View {
        ZStack {
            LinearGradient(
                colors: [
                    Color(hex: "0B1023"),
                    Color(hex: "11234D"),
                    Color(hex: "3F2368")
                ],
                startPoint: .bottom,
                endPoint: .topTrailing
            )

            RadialGradient(
                colors: [
                    Color(hex: "18B7C7").opacity(0.56),
                    Color.clear
                ],
                center: .topLeading,
                startRadius: 20,
                endRadius: 360
            )

            RadialGradient(
                colors: [
                    Color(hex: "B24CFF").opacity(0.34),
                    Color.clear
                ],
                center: .topTrailing,
                startRadius: 20,
                endRadius: 340
            )
        }
    }

    private var homeAIHomeHeader: some View {
        HStack(alignment: .center) {
            ZStack {
                Circle()
                    .fill(Color.white.opacity(0.18))
                    .frame(width: 46, height: 46)

                Text("ME")
                    .font(.system(size: 17, weight: .bold, design: .rounded))
                    .foregroundColor(.white)
            }

            Spacer()

            HStack(spacing: 8) {
                Image(systemName: "sparkles")
                    .font(.system(size: 16, weight: .semibold))

                Text("Saku AI")
                    .font(.system(size: 18, weight: .semibold, design: .rounded))

                Text("beta")
                    .font(.system(size: 12, weight: .semibold, design: .rounded))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .overlay(
                        Capsule(style: .continuous)
                            .stroke(Color.white.opacity(0.45), lineWidth: 1)
                    )
            }
            .foregroundColor(.white.opacity(0.94))

            Spacer()

            ZStack(alignment: .topTrailing) {
                Circle()
                    .fill(Color.white.opacity(0.16))
                    .frame(width: 46, height: 46)

                Image(systemName: "bell")
                    .font(.system(size: 20, weight: .medium))
                    .foregroundColor(.white)
                    .frame(width: 46, height: 46)

                Circle()
                    .fill(Color.AlertRed)
                    .frame(width: 10, height: 10)
                    .offset(x: -4, y: 3)
            }
        }
    }

    private func targetHomeAIOpenOffset(for height: CGFloat) -> CGFloat {
        let baseSurfaceTop = max(0, logSurfaceTopY)
        guard baseSurfaceTop > 0 else {
            return max(0, height - targetHomeAIPeekHeight(for: height))
        }

        return max(0, height - targetHomeAIPeekHeight(for: height) - baseSurfaceTop)
    }

    private func targetHomeAIPeekHeight(for height: CGFloat) -> CGFloat {
        max(156, min(184, height * 0.19))
    }

    private func collapseAI() {
        withAnimation(homeAISettleAnimation) {
            homeAISheetOffset = 0
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.27) {
            homeAIAssistantViewModel.collapse()
        }
    }

    private func rubberBandPullDistance(for translation: CGFloat) -> CGFloat {
        guard translation > 0 else { return 0 }

        let normalized = translation / homeAIMaxPullDistance
        let resistance = 1 - (1 / ((normalized * 0.85) + 1))
        return min(resistance * homeAIMaxPullDistance * 1.55, homeAIMaxPullDistance)
    }

}

private struct HomeAISurfaceMaskShape: Shape {
    let topInset: CGFloat
    let cornerRadius: CGFloat

    func path(in rect: CGRect) -> Path {
        let visibleTop = min(max(0, topInset), rect.maxY)
        let visibleRect = CGRect(
            x: rect.minX,
            y: visibleTop,
            width: rect.width,
            height: max(0, rect.maxY - visibleTop)
        )

        guard cornerRadius > 0 else { return Path(visibleRect) }
        guard !visibleRect.isEmpty else { return Path() }

        let path = UIBezierPath(
            roundedRect: visibleRect,
            byRoundingCorners: [.topLeft, .topRight],
            cornerRadii: CGSize(width: cornerRadius, height: cornerRadius)
        )
        return Path(path.cgPath)
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
