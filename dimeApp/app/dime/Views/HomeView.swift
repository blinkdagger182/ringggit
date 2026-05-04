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
            let homeAIOpenOffset = targetHomeAIOpenOffset(for: proxy.size.height)

            ZStack(alignment: .bottom) {
                if currentTab == "Log", homeAIAssistantViewModel.isPresented || homeAISheetOffset > 0.5 {
                    HomeAIAssistantOverlay(
                        viewModel: homeAIAssistantViewModel,
                        namespace: homeAINamespace,
                        topInset: topEdge,
                        bottomInset: bottomEdge,
                        revealProgress: homeAIRevealProgress(openOffset: homeAIOpenOffset),
                        isExpanded: homeAIAssistantViewModel.isPresented,
                        onCollapse: {
                        withAnimation(.spring(response: 0.42, dampingFraction: 0.86)) {
                            homeAISheetOffset = 0
                            homeAIAssistantViewModel.collapse()
                        }
                        },
                        collapseGesture: homeAIPullGesture(openOffset: homeAIOpenOffset)
                    )
                    .ignoresSafeArea()
                    .zIndex(0)
                }

                TabView(selection: $currentTab) {
                    LogView(topEdge: topEdge, bottomEdge: bottomEdge, launchSearch: launchSearch)
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
                .offset(y: currentTab == "Log" ? homeAISheetOffset : 0)
                .mask(
                    RoundedRectangle(
                        cornerRadius: currentTab == "Log" ? min(34, homeAIRevealProgress(openOffset: homeAIOpenOffset) * 34) : 0,
                        style: .continuous
                    )
                    .padding(.top, currentTab == "Log" ? 0 : -1200)
                )
                .overlay(alignment: .top) {
                    if currentTab == "Log" && homeAISheetOffset > 6 && !homeAIAssistantViewModel.isPresented {
                        VStack(spacing: 10) {
                            Image(systemName: "chevron.compact.up")
                                .font(.system(size: 22, weight: .medium))
                                .foregroundColor(Color.PrimaryText.opacity(0.14))

                            Text(homeAISheetOffset > homeAIPullThreshold * 0.82 ? "Release for ✨ AI" : "Pull more for ✨ AI")
                                .font(.system(.title3, design: .rounded).weight(.medium))
                                .foregroundColor(.white.opacity(0.94))
                        }
                        .padding(.top, max(48, homeAISheetOffset - 108))
                    }
                }
                .zIndex(1)

                CustomTabBar(currentTab: $currentTab, topEdge: topEdge, bottomEdge: bottomEdge, counter: $counter, launchAdd: launchAdd)
                    .offset(y: (currentTab == "Log" ? homeAISheetOffset : 0) + (tabBarManager.hideTab ? (70 + bottomEdge) : 0))
                    .allowsHitTesting(!homeAIAssistantViewModel.isPresented)
                    .zIndex(1.05)

                if currentTab == "Log" {
                    Color.clear
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                        .overlay(alignment: .top) {
                            Rectangle()
                                .fill(Color.clear)
                                .frame(height: topEdge + 56)
                                .contentShape(Rectangle())
                                .highPriorityGesture(homeAIPullGesture(openOffset: homeAIOpenOffset))
                        }
                        .ignoresSafeArea(.container, edges: .top)
                        .offset(y: homeAISheetOffset)
                        .zIndex(1.1)
                }

                if currentTab == "Log", !homeAIAssistantViewModel.isPresented {
                    HomeAIAssistantBar(
                        viewModel: homeAIAssistantViewModel,
                        namespace: homeAINamespace,
                        topInset: topEdge,
                        pullOffset: homeAISheetOffset,
                        mode: .idlePill,
                        dragGesture: homeAIPullGesture(openOffset: homeAIOpenOffset)
                    ) {
                        withAnimation(.spring(response: 0.45, dampingFraction: 0.86)) {
                            homeAIAssistantViewModel.expand()
                            homeAISheetOffset = homeAIOpenOffset
                        }
                    }
                    .allowsHitTesting(true)
                    .padding(.horizontal, 20)
                    .zIndex(1.15)
                }

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
            withAnimation(.spring(response: 0.35, dampingFraction: 0.88)) {
                homeAISheetOffset = 0
            }

            if newValue != "Log" {
                withAnimation(.spring(response: 0.42, dampingFraction: 0.88)) {
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
            }
        }
    }

    private func homeAIPullGesture(openOffset: CGFloat) -> AnyGesture<DragGesture.Value> {
        AnyGesture(
            DragGesture(minimumDistance: 4, coordinateSpace: .global)
                .onChanged { value in
                    guard currentTab == "Log" else { return }

                    if homeAIAssistantViewModel.isPresented {
                        guard value.startLocation.y <= homeAISheetOffset + topEdge + 72 else { return }
                        let isKeyboardPresented = keyboardHeightHelper.keyboardHeight > 0

                        if isKeyboardPresented {
                            if value.translation.height > 8 {
                                UIApplication.shared.endEditing()
                            }
                            return
                        }

                        if value.translation.height < 0 {
                            UIApplication.shared.endEditing()
                            homeAISheetOffset = max(0, openOffset - collapseRubberBandDistance(for: -value.translation.height))
                        } else {
                            if value.translation.height > 8 {
                                UIApplication.shared.endEditing()
                            }
                            homeAISheetOffset = min(openOffset + (rubberBandPullDistance(for: value.translation.height) * 0.08), openOffset + 18)
                        }
                        return
                    }

                    guard value.startLocation.y <= topEdge + 72 else { return }
                    guard value.translation.height > 0 else {
                        if homeAISheetOffset > 0 {
                            homeAISheetOffset = max(0, homeAISheetOffset + (value.translation.height * 0.2))
                        }
                        return
                    }

                    homeAISheetOffset = min(rubberBandPullDistance(for: value.translation.height), openOffset)
                }
                .onEnded { value in
                    withAnimation(.spring(response: 0.42, dampingFraction: 0.82)) {
                        if homeAIAssistantViewModel.isPresented {
                            guard keyboardHeightHelper.keyboardHeight == 0 else {
                                homeAISheetOffset = openOffset
                                return
                            }

                            let collapseDistance = max(0, openOffset - homeAISheetOffset)
                            let shouldCollapse = value.predictedEndTranslation.height < -180 || collapseDistance > 180

                            if shouldCollapse {
                                homeAISheetOffset = 0
                                homeAIAssistantViewModel.collapse()
                            } else {
                                homeAISheetOffset = openOffset
                            }
                        } else {
                            let predictedOffset = min(rubberBandPullDistance(for: max(value.predictedEndTranslation.height, value.translation.height)), openOffset)
                            let shouldExpand = predictedOffset > homeAIPullThreshold

                            if shouldExpand {
                                homeAISheetOffset = openOffset
                                homeAIAssistantViewModel.expand()
                            } else {
                                homeAISheetOffset = 0
                            }
                        }
                    }
                }
        )
    }

    private func homeAIRevealProgress(openOffset: CGFloat) -> CGFloat {
        guard openOffset > 0 else { return 0 }
        return min(max(homeAISheetOffset / openOffset, 0), 1)
    }

    private func targetHomeAIOpenOffset(for height: CGFloat) -> CGFloat {
        height + 28
    }

    private func rubberBandPullDistance(for translation: CGFloat) -> CGFloat {
        guard translation > 0 else { return 0 }

        let normalized = translation / homeAIMaxPullDistance
        let resistance = 1 - (1 / ((normalized * 0.85) + 1))
        return min(resistance * homeAIMaxPullDistance * 1.55, homeAIMaxPullDistance)
    }

    private func collapseRubberBandDistance(for translation: CGFloat) -> CGFloat {
        guard translation > 0 else { return 0 }

        let normalized = translation / homeAIMaxPullDistance
        let resistance = 1 - (1 / ((normalized * 0.92) + 1))
        return min(resistance * homeAIMaxPullDistance * 1.2, homeAIMaxPullDistance)
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
