//
//  HomeView.swift
//  xpenz
//
//  Created by Rafael Soh on 20/5/22.
//

import ConfettiSwiftUI
import Foundation
import SwiftUI
import UIKit

class OverallToastPresenter: ObservableObject {
    @Published var showToast: Bool = false
}

enum DeletionType {
    case instant
    case prompt
}

enum HomeSheet: Identifiable {
    case profile
    case notifications
    case reports
    case plan

    var id: String {
        switch self {
        case .profile: return "profile"
        case .notifications: return "notifications"
        case .reports: return "reports"
        case .plan: return "plan"
        }
    }
}

struct ScannedReceiptPrefill {
    let amount: Double?
    let note: String
    let date: Date?
    let attachmentReference: TransactionAttachmentReference?
    let sourceText: String?
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

    @State var currentTab = "Home"

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
    @State private var activeSheet: HomeSheet?
    @State private var showAddTransaction = false
    @State private var showReceiptScanner = false
    @State private var pendingReceiptPrefill: ScannedReceiptPrefill?
    @State private var transactionCountBeforeAdd = 0
    @State private var aiCaptionIndex = 0

    @State var counter = 0

    @EnvironmentObject var tabBarManager: TabBarManager

    @State var showPopup = false

    private let aiCaptions = [
        "Ask KIRA",
        "Where did my money go?",
        "Split a receipt",
        "Can I afford this?",
        "Summarize this month"
    ]

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
            let isHomeTab = currentTab == "Home"
            let sheetY: CGFloat = isHomeTab ? (collapsedTopInset + liveProgress * rd) : 0
            let sheetCornerRadius: CGFloat = isHomeTab ? 38 : 0

            ZStack(alignment: .top) {
                Color.black.ignoresSafeArea()

                if currentTab == "Home" {
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
                    .shadow(color: .black.opacity(isHomeTab ? 0.12 : 0), radius: 18, x: 0, y: -5)
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
                    .animation(.easeOut(duration: 0.22), value: isHomeTab)
                    .simultaneousGesture(makeDragGesture(rd: rd))

                // Tab bar pinned to screen bottom, outside offset sheet
                CustomTabBar(
                    currentTab: $currentTab,
                    topEdge: topEdge,
                    bottomEdge: bottomEdge,
                    counter: $counter,
                    launchAdd: launchAdd,
                    onAddExpense: { presentAddTransaction() },
                    onScanReceipt: { showReceiptScanner = true },
                    onAskKIRA: { revealAskKIRA() }
                )
                .frame(maxHeight: .infinity, alignment: .bottom)
                .opacity(homeAIAssistantViewModel.isPresented ? 0 : 1)
                .allowsHitTesting(!homeAIAssistantViewModel.isPresented)

                // Floating collapsed header — fades out as AI reveals
                if currentTab == "Home" {
                    collapsedHeader(onReveal: {
                        revealAskKIRA()
                    }, onProfile: {
                        activeSheet = .profile
                    }, onNotifications: {
                        activeSheet = .notifications
                    })
                    .padding(.top, max(topEdge, proxy.safeAreaInsets.top) + 10)
                    .opacity(max(0, 1.0 - liveProgress * 1.35))
                    .allowsHitTesting(liveProgress < 0.05)
                    .onAppear {
                        startAICaptionRotation()
                    }
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
        .sheet(item: $activeSheet) { sheet in
            switch sheet {
            case .profile:
                ProfileView()
            case .notifications:
                NotificationsSheetView()
            case .reports:
                InsightsView()
            case .plan:
                PlanView()
            }
        }
        .fullScreenCover(isPresented: $showReceiptScanner) {
            HomeScanReceiptFlowView { prefill in
                showReceiptScanner = false
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.22) {
                    presentAddTransaction(prefill: prefill)
                }
            }
        }
        .fullScreenCover(isPresented: $showAddTransaction, onDismiss: {
            handleAddTransactionDismiss()
        }) {
            TransactionView(prefill: pendingReceiptPrefill)
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
                currentTab = "Activity"
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { launchSearch.toggle() }
                fromURL2 = false
            }
            if appLockVM.isAppLockEnabled && fromURL3 { activeSheet = .reports }
            if appLockVM.isAppLockEnabled && fromURL4 { activeSheet = .plan }
        }
        .onOpenURL { url in
            if url.host == "search" { currentTab = "Activity" }
            else if url.host == "insights" { activeSheet = .reports }
            else if url.host == "budget" { activeSheet = .plan }
            else if url.host == "aioverlay" {
                currentTab = "Home"
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                    revealAskKIRA()
                }
            }
        }
    }

    // MARK: - Home sheet

    private func homeSheet(peekHeight: CGFloat) -> some View {
        ZStack(alignment: .bottom) {
            TabView(selection: $currentTab) {
                HomeDashboardView(
                    bottomEdge: bottomEdge,
                    sheetTopInset: collapsedTopInset,
                    isScrollLocked: liveProgress > 0.01,
                    onScrollStateChanged: { isAtTop, _ in isLogAtTop = isAtTop },
                    onAskKIRA: { revealAskKIRA() },
                    onScanReceipt: { showReceiptScanner = true },
                    onAddTransaction: { presentAddTransaction() },
                    onOpenPlan: { activeSheet = .plan },
                    onViewActivity: { currentTab = "Activity" },
                    onReports: { activeSheet = .reports }
                )
                .ignoresSafeArea(.all)
                .tag("Home")

                ActivityView(
                    bottomEdge: bottomEdge,
                    launchSearch: launchSearch
                )
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .tag("Activity")
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
                guard currentTab == "Home" else { return }

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
                guard currentTab == "Home" else { return }
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

    private func revealAskKIRA() {
        currentTab = "Home"
        withAnimation(settleAnimation) { settledProgress = 1 }
        homeAIAssistantViewModel.expand()
    }

    private func presentAddTransaction(prefill: ScannedReceiptPrefill? = nil) {
        pendingReceiptPrefill = prefill
        transactionCountBeforeAdd = dataController.results(for: Transaction.fetchRequest()).count
        showAddTransaction = true
    }

    private func handleAddTransactionDismiss() {
        let currentCount = dataController.results(for: Transaction.fetchRequest()).count
        if currentCount != transactionCountBeforeAdd {
            counter += 1
        }
        pendingReceiptPrefill = nil
    }

    private func startAICaptionRotation() {
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.8) {
            guard currentTab == "Home", !homeAIAssistantViewModel.isPresented else { return }
            withAnimation(.easeInOut(duration: 0.32)) {
                aiCaptionIndex = (aiCaptionIndex + 1) % aiCaptions.count
            }
            startAICaptionRotation()
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

    private func collapsedHeader(onReveal: @escaping () -> Void, onProfile: @escaping () -> Void, onNotifications: @escaping () -> Void) -> some View {
        HStack(alignment: .center) {
            Button { onProfile() } label: {
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
                    Text(aiCaptions[aiCaptionIndex])
                        .id(aiCaptionIndex)
                        .font(Font.satoshi(16, weight: .semibold))
                        .foregroundStyle(.white)
                        .lineLimit(1)
                        .minimumScaleFactor(0.72)
                        .transition(.asymmetric(
                            insertion: .move(edge: .bottom).combined(with: .opacity),
                            removal: .move(edge: .top).combined(with: .opacity)
                        ))
                }
                .padding(.horizontal, 16)
                .frame(height: 46)
                .frame(maxWidth: 218)
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
            .buttonStyle(BouncyButton(duration: 0.22, scale: 0.96))

            Spacer(minLength: 0)

            Button { onNotifications() } label: {
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

private enum ActivityFilter: String, CaseIterable {
    case all = "All"
    case expenses = "Expenses"
    case income = "Income"
    case receipts = "Receipts"
}

struct ActivityView: View {
    var bottomEdge: CGFloat
    var launchSearch: Bool

    @FetchRequest(sortDescriptors: [
        SortDescriptor(\.date, order: .reverse)
    ]) private var transactions: FetchedResults<Transaction>

    @AppStorage("showCents", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var showCents: Bool = true
    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var currency: String = Locale.current.currencyCode!
    @AppStorage("showExpenseOrIncomeSign", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var showExpenseOrIncomeSign: Bool = true
    @AppStorage("swapTimeLabel", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var swapTimeLabel: Bool = false

    @State private var searchText = ""
    @State private var filter: ActivityFilter = .all
    @FocusState private var searchFocused: Bool

    private var currencySymbol: String {
        Locale.current.localizedCurrencySymbol(forCurrencyCode: currency) ?? "RM"
    }

    private var filteredTransactions: [Transaction] {
        transactions.filter { transaction in
            let matchesFilter: Bool
            switch filter {
            case .all:
                matchesFilter = true
            case .expenses:
                matchesFilter = !transaction.income
            case .income:
                matchesFilter = transaction.income
            case .receipts:
                matchesFilter = transaction.reference?.isEmpty == false
            }

            guard matchesFilter else { return false }
            guard !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return true }

            let query = searchText.lowercased()
            return transaction.wrappedNote.lowercased().contains(query)
                || transaction.wrappedCategoryName.lowercased().contains(query)
                || (transaction.bucket?.name ?? "").lowercased().contains(query)
        }
    }

    private var groupedTransactions: [(title: String, transactions: [Transaction])] {
        let calendar = Calendar.current
        let today = calendar.startOfDay(for: Date.now)
        let yesterday = calendar.date(byAdding: .day, value: -1, to: today) ?? today
        let weekStart = calendar.dateInterval(of: .weekOfYear, for: Date.now)?.start ?? today

        let todayItems = filteredTransactions.filter { calendar.isDate($0.wrappedDate, inSameDayAs: today) }
        let yesterdayItems = filteredTransactions.filter { calendar.isDate($0.wrappedDate, inSameDayAs: yesterday) }
        let weekItems = filteredTransactions.filter {
            $0.wrappedDate >= weekStart
                && !calendar.isDate($0.wrappedDate, inSameDayAs: today)
                && !calendar.isDate($0.wrappedDate, inSameDayAs: yesterday)
        }
        let olderItems = filteredTransactions.filter { $0.wrappedDate < weekStart }

        return [
            ("Today", todayItems),
            ("Yesterday", yesterdayItems),
            ("This week", weekItems),
            ("Older", olderItems)
        ].filter { !$0.transactions.isEmpty }
    }

    var body: some View {
        VStack(spacing: 0) {
            VStack(alignment: .leading, spacing: 14) {
                Text("Activity")
                    .font(Font.satoshi(.largeTitle, weight: .bold))
                    .foregroundColor(Color.PrimaryText)
                    .accessibility(addTraits: .isHeader)

                searchBar

                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(ActivityFilter.allCases, id: \.self) { item in
                            Button {
                                withAnimation(.easeInOut(duration: 0.18)) {
                                    filter = item
                                }
                            } label: {
                                Text(item.rawValue)
                                    .font(Font.satoshi(.subheadline, weight: .semibold))
                                    .foregroundColor(filter == item ? Color.LightIcon : Color.PrimaryText)
                                    .padding(.horizontal, 14)
                                    .frame(height: 36)
                                    .background(filter == item ? Color.DarkBackground : Color.SecondaryBackground, in: Capsule())
                                    .overlay(Capsule().stroke(Color.Outline.opacity(filter == item ? 0 : 0.7), lineWidth: 1))
                            }
                            .buttonStyle(.plain)
                        }
                    }
                }
            }
            .padding(.horizontal, 20)
            .padding(.top, 18)
            .padding(.bottom, 12)

            ScrollView(showsIndicators: false) {
                if filteredTransactions.isEmpty {
                    EmptyStateView(
                        systemImage: "tray",
                        title: transactions.isEmpty ? "No activity yet." : "No matching activity.",
                        message: transactions.isEmpty ? "Scan a receipt or add a transaction." : "Try a different search or filter."
                    )
                    .padding(.horizontal, 20)
                    .padding(.top, 60)
                } else {
                    LazyVStack(spacing: 18) {
                        ForEach(groupedTransactions, id: \.title) { group in
                            VStack(spacing: 0) {
                                HStack {
                                    Text(group.title)
                                        .font(Font.satoshi(.callout, weight: .semibold))
                                        .foregroundColor(Color.SubtitleText)
                                    Spacer()
                                    Text(totalText(for: group.transactions))
                                        .font(Font.satoshi(.callout, weight: .semibold))
                                        .foregroundColor(Color.SubtitleText)
                                }
                                .padding(.horizontal, 10)
                                .padding(.bottom, 8)

                                VStack(spacing: 0) {
                                    ForEach(group.transactions, id: \.id) { transaction in
                                        SingleTransactionView(
                                            transaction: transaction,
                                            showCents: showCents,
                                            currencySymbol: currencySymbol,
                                            currency: currency,
                                            swapTimeLabel: swapTimeLabel,
                                            future: false,
                                            showExpenseOrIncomeSign: showExpenseOrIncomeSign
                                        )
                                    }
                                }
                                .padding(.horizontal, 4)
                                .padding(.vertical, 6)
                                .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
                            }
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.bottom, 96 + bottomEdge)
                }
            }
        }
        .background(Color.PrimaryBackground.ignoresSafeArea())
        .onChange(of: launchSearch) { _ in
            searchFocused = true
        }
    }

    private var searchBar: some View {
        HStack(spacing: 9) {
            Image(systemName: "magnifyingglass")
                .font(Font.satoshi(15, weight: .semibold))
                .foregroundColor(Color.SubtitleText)

            TextField("Search transactions", text: $searchText)
                .font(Font.satoshi(.body, weight: .medium))
                .foregroundColor(Color.PrimaryText)
                .focused($searchFocused)

            if !searchText.isEmpty {
                Button {
                    searchText = ""
                } label: {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundColor(Color.SubtitleText)
                }
            }
        }
        .padding(.horizontal, 14)
        .frame(height: 46)
        .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .stroke(Color.Outline.opacity(0.7), lineWidth: 1)
        )
    }

    private func totalText(for transactions: [Transaction]) -> String {
        let total = transactions.reduce(0.0) { partial, transaction in
            partial + (transaction.income ? transaction.wrappedAmount : -transaction.wrappedAmount)
        }
        let value = showCents ? String(format: "%.2f", abs(total)) : String(format: "%.0f", abs(total))
        return "\(total >= 0 ? "+" : "-")\(currencySymbol)\(value)"
    }
}

struct NotificationsSheetView: View {
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationView {
            VStack(spacing: 14) {
                Spacer()

                Image(systemName: "bell.badge")
                    .font(Font.satoshi(34, weight: .medium))
                    .foregroundColor(Color(hex: "4A5240"))
                    .frame(width: 72, height: 72)
                    .background(Color.SecondaryBackground, in: Circle())

                Text("You're all caught up.")
                    .font(Font.satoshi(.title3, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)

                Text("KIRA will let you know when something needs your attention.")
                    .font(Font.satoshi(.subheadline, weight: .medium))
                    .foregroundColor(Color.SubtitleText)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 32)

                Spacer()
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color.PrimaryBackground.ignoresSafeArea())
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                        .foregroundColor(Color.PrimaryText)
                }
            }
        }
        .navigationViewStyle(.stack)
    }
}

struct HomeScanReceiptFlowView: View {
    @Environment(\.dismiss) private var dismiss
    @StateObject private var redactionVM = RedactionFlowViewModel()
    @State private var showDocumentScanner = false
    @State private var hasOpenedScanner = false

    let onComplete: (ScannedReceiptPrefill) -> Void

    var body: some View {
        ZStack(alignment: .topLeading) {
            KiraReceiptLoadingView(
                title: "Opening scanner",
                subtitle: "Scan your receipt, then KIRA will fill the expense form.",
                status: "Ask. Scan. Understand."
            )

            Button { dismiss() } label: {
                Image(systemName: "xmark")
                    .font(Font.satoshi(.callout, weight: .bold))
                    .foregroundColor(.white.opacity(0.86))
                    .frame(width: 40, height: 40)
                    .background(.white.opacity(0.12), in: Circle())
                    .overlay(
                        Circle()
                            .stroke(.white.opacity(0.18), lineWidth: 1)
                    )
            }
            .buttonStyle(.plain)
            .padding(.top, 18)
            .padding(.leading, 20)
        }
        .background(Color(hex: "4A5240").ignoresSafeArea())
        .onAppear {
            guard !hasOpenedScanner else { return }
            hasOpenedScanner = true
            DispatchQueue.main.async {
                showDocumentScanner = true
            }
        }
        .fullScreenCover(isPresented: $showDocumentScanner) {
            DocumentScannerView(onCancel: {
                dismiss()
            }) { pages in
                guard !pages.isEmpty else {
                    dismiss()
                    return
                }
                redactionVM.startRedaction(pages: pages, filename: "Scanned Receipt", sourceText: nil)
            }
            .ignoresSafeArea()
        }
        .fullScreenCover(isPresented: $redactionVM.showFlow) {
            RedactionPreviewView(viewModel: redactionVM) { redactedPages in
                let sourceText = redactionVM.sourceText ?? redactionVM.ocrText
                let attachment = AttachmentItem(
                    pdfPages: redactedPages,
                    filename: redactionVM.filename,
                    sourceText: sourceText
                )
                let reference = TransactionAttachmentStore.saveAIAttachment(attachment)
                let prefill = ReceiptPrefillParser.makePrefill(
                    sourceText: sourceText,
                    attachmentReference: reference
                )
                onComplete(prefill)
                dismiss()
            }
        }
    }
}

struct KiraReceiptLoadingView: View {
    let title: String
    let subtitle: String
    let status: String

    @State private var animate = false

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [
                    Color(hex: "313B2D"),
                    Color(hex: "4A5240"),
                    Color(hex: "5B6B4F")
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            VStack(spacing: 28) {
                Spacer(minLength: 80)

                ZStack {
                    ForEach(0..<3, id: \.self) { index in
                        Circle()
                            .stroke(.white.opacity(0.10), lineWidth: 1.5)
                            .frame(width: 118 + CGFloat(index * 34), height: 118 + CGFloat(index * 34))
                            .scaleEffect(animate ? 1.08 : 0.92)
                            .opacity(animate ? 0.20 : 0.70)
                            .animation(
                                .easeInOut(duration: 1.7)
                                    .repeatForever(autoreverses: true)
                                    .delay(Double(index) * 0.16),
                                value: animate
                            )
                    }

                    RoundedRectangle(cornerRadius: 34, style: .continuous)
                        .fill(.white.opacity(0.13))
                        .frame(width: 118, height: 118)
                        .overlay(
                            RoundedRectangle(cornerRadius: 34, style: .continuous)
                                .stroke(
                                    LinearGradient(
                                        colors: [.white.opacity(0.46), .white.opacity(0.12)],
                                        startPoint: .topLeading,
                                        endPoint: .bottomTrailing
                                    ),
                                    lineWidth: 1.2
                                )
                        )
                        .shadow(color: .black.opacity(0.20), radius: 28, x: 0, y: 18)
                        .scaleEffect(animate ? 1.02 : 0.98)

                    Image("kira-ai")
                        .resizable()
                        .scaledToFit()
                        .frame(width: 72, height: 72)
                        .shadow(color: .black.opacity(0.18), radius: 10, x: 0, y: 6)
                }

                VStack(spacing: 10) {
                    Text(title)
                        .font(Font.satoshi(.title2, weight: .bold))
                        .foregroundColor(.white)
                        .multilineTextAlignment(.center)

                    Text(subtitle)
                        .font(Font.satoshi(.body, weight: .medium))
                        .foregroundColor(.white.opacity(0.68))
                        .multilineTextAlignment(.center)
                        .lineSpacing(2)
                        .padding(.horizontal, 26)
                }

                HStack(spacing: 8) {
                    ForEach(0..<3, id: \.self) { index in
                        Circle()
                            .fill(.white.opacity(0.78))
                            .frame(width: 6, height: 6)
                            .scaleEffect(animate ? 1.0 : 0.55)
                            .opacity(animate ? 1 : 0.35)
                            .animation(
                                .easeInOut(duration: 0.72)
                                    .repeatForever(autoreverses: true)
                                    .delay(Double(index) * 0.18),
                                value: animate
                            )
                    }
                }

                Text(status)
                    .font(Font.satoshi(.footnote, weight: .semibold))
                    .foregroundColor(.white.opacity(0.56))
                    .padding(.horizontal, 14)
                    .frame(height: 32)
                    .background(.white.opacity(0.10), in: Capsule())
                    .overlay(Capsule().stroke(.white.opacity(0.14), lineWidth: 1))

                Spacer(minLength: 90)
            }
            .padding(.horizontal, 24)
        }
        .onAppear {
            withAnimation {
                animate = true
            }
        }
    }
}

private enum ReceiptPrefillParser {
    static func makePrefill(sourceText: String?, attachmentReference: TransactionAttachmentReference?) -> ScannedReceiptPrefill {
        let text = sourceText ?? ""
        return ScannedReceiptPrefill(
            amount: extractAmount(from: text),
            note: extractMerchant(from: text) ?? "Scanned receipt",
            date: extractDate(from: text),
            attachmentReference: attachmentReference,
            sourceText: sourceText
        )
    }

    private static func extractAmount(from text: String) -> Double? {
        let lines = normalizedLines(from: text)
        let priorityKeywords = ["grand total", "total", "amount due", "balance due", "paid"]
        let priorityLines = lines.filter { line in
            priorityKeywords.contains { line.lowercased().contains($0) }
        }

        if let priorityAmount = priorityLines.compactMap({ amounts(in: $0).last }).last {
            return priorityAmount
        }

        return lines.flatMap { amounts(in: $0) }.max()
    }

    private static func amounts(in text: String) -> [Double] {
        let pattern = #"(?i)(?:rm|myr)?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})|[0-9]+(?:\.[0-9]{2}))"#
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return [] }
        let range = NSRange(text.startIndex..<text.endIndex, in: text)

        return regex.matches(in: text, range: range).compactMap { match in
            guard let amountRange = Range(match.range(at: 1), in: text) else { return nil }
            let value = text[amountRange].replacingOccurrences(of: ",", with: "")
            return Double(value)
        }
    }

    private static func extractMerchant(from text: String) -> String? {
        let ignoredFragments = [
            "receipt", "invoice", "tax", "sst", "cashier", "total", "subtotal",
            "amount", "date", "time", "qty", "quantity", "change", "paid"
        ]

        return normalizedLines(from: text).first { line in
            let lowercased = line.lowercased()
            guard line.count >= 2, line.count <= 48 else { return false }
            guard !ignoredFragments.contains(where: { lowercased.contains($0) }) else { return false }
            return line.rangeOfCharacter(from: .letters) != nil
        }
    }

    private static func extractDate(from text: String) -> Date? {
        let patterns = [
            (#"\b\d{1,2}/\d{1,2}/\d{2,4}\b"#, ["d/M/yyyy", "dd/MM/yyyy", "d/M/yy", "dd/MM/yy"]),
            (#"\b\d{1,2}-\d{1,2}-\d{2,4}\b"#, ["d-M-yyyy", "dd-MM-yyyy", "d-M-yy", "dd-MM-yy"]),
            (#"\b\d{4}-\d{1,2}-\d{1,2}\b"#, ["yyyy-M-d", "yyyy-MM-dd"]),
            (#"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4}\b"#, ["d MMM yyyy", "dd MMM yyyy", "d MMMM yyyy", "dd MMMM yyyy"])
        ]

        for (pattern, formats) in patterns {
            guard let regex = try? NSRegularExpression(pattern: pattern) else { continue }
            let range = NSRange(text.startIndex..<text.endIndex, in: text)
            guard let match = regex.firstMatch(in: text, range: range),
                  let matchRange = Range(match.range, in: text)
            else { continue }

            let candidate = String(text[matchRange])
            if let date = parseDate(candidate, formats: formats) {
                return date
            }
        }

        return nil
    }

    private static func parseDate(_ value: String, formats: [String]) -> Date? {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")

        for format in formats {
            formatter.dateFormat = format
            if let date = formatter.date(from: value) {
                return date
            }
        }

        return nil
    }

    private static func normalizedLines(from text: String) -> [String] {
        text.components(separatedBy: .newlines)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }
}

struct ProfileView: View {
    @Environment(\.dismiss) private var dismiss
    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var currency: String = Locale.current.currencyCode!
    @AppStorage("colourScheme", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var colourScheme: Int = 0
    @AppStorage("numberEntryType", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var numberEntryType: Int = 2
    @AppStorage("showNotifications", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var showNotifications: Bool = false

    private let background = Color(hex: "4B5545")
    private let foreground = Color.white.opacity(0.86)
    private let muted = Color.white.opacity(0.58)

    private var appearanceValue: String {
        if colourScheme == 1 { return "Light" }
        if colourScheme == 2 { return "Dark" }
        return "System"
    }

    private var defaultEntryValue: String {
        numberEntryType == 1 ? "Type 1" : "Expense"
    }

    var body: some View {
        NavigationView {
            ZStack {
                background
                    .ignoresSafeArea()

                ScrollView(showsIndicators: false) {
                    VStack(spacing: 26) {
                        header
                            .padding(.top, 18)

                        profileSummary

                        KIRAProCard()

                        ProfileSettingsSection(title: "Account") {
                            ProfileSettingsRow(icon: "person", title: "Personal details", destination: ProfilePlaceholderDetail(title: "Personal details"))
                            ProfileSettingsRow(icon: "lock.shield", title: "Security & privacy", destination: ProfilePlaceholderDetail(title: "Security & privacy"))
                            ProfileSettingsRow(icon: "sparkles", title: "Subscription", value: "KIRA Pro", destination: ProfilePlaceholderDetail(title: "Subscription"))
                        }

                        ProfileSettingsSection(title: "Manage") {
                            ProfileSettingsRow(icon: "square.grid.2x2", title: "Categories", destination: SettingsCategoryView())
                            ProfileSettingsRow(icon: "target", title: "Budgets", destination: PlanView())
                            ProfileSettingsRow(icon: "flag", title: "Goals", destination: PlanView())
                            ProfileSettingsRow(icon: "arrow.clockwise", title: "Recurring expenses", destination: ProfilePlaceholderDetail(title: "Recurring expenses"))
                            ProfileSettingsRow(icon: "wand.and.stars", title: "Receipt rules", destination: ProfilePlaceholderDetail(title: "Receipt rules"))
                        }

                        ProfileSettingsSection(title: "AI & Automation") {
                            ProfileSettingsRow(icon: "brain.head.profile", title: "Ask KIRA memory", destination: ProfilePlaceholderDetail(title: "Ask KIRA memory"))
                            ProfileSettingsRow(icon: "tag", title: "Auto-categorization", destination: ProfilePlaceholderDetail(title: "Auto-categorization"))
                            ProfileSettingsRow(icon: "doc.text.magnifyingglass", title: "Monthly summaries", destination: ProfilePlaceholderDetail(title: "Monthly summaries"))
                            ProfileSettingsRow(icon: "lightbulb", title: "Smart suggestions", destination: ProfilePlaceholderDetail(title: "Smart suggestions"))
                        }

                        ProfileSettingsSection(title: "App") {
                            ProfileSettingsRow(icon: "bell", title: "Notifications", value: showNotifications ? "On" : "Off", destination: SettingsNotificationsView())
                            ProfileSettingsRow(icon: "coloncurrencysign.square", title: "Currency", value: currency, destination: SettingsCurrencyView())
                            ProfileSettingsRow(icon: "circle.righthalf.filled", title: "Appearance", value: appearanceValue, destination: SettingsAppearanceView())
                            ProfileSettingsRow(icon: "keyboard", title: "Default entry type", value: defaultEntryValue, destination: SettingsNumberEntryView())
                        }

                        ProfileSettingsSection(title: "Data") {
                            ProfileSettingsRow(icon: "icloud", title: "Backup & sync", destination: SettingsCloudView())
                            ProfileSettingsRow(icon: "square.and.arrow.up", title: "Export data", destination: SettingsExportPlaceholderView())
                            ProfileSettingsRow(icon: "trash", title: "Erase data", isDestructive: true, destination: SettingsEraseView())
                        }

                        ProfileSettingsSection(title: "Support") {
                            ProfileSettingsRow(icon: "questionmark.circle", title: "Help & support", destination: ProfilePlaceholderDetail(title: "Help & support"))
                            ProfileSettingsRow(icon: "bubble.left.and.text.bubble.right", title: "Feature request", destination: ProfilePlaceholderDetail(title: "Feature request"))
                            ProfileSettingsRow(icon: "exclamationmark.bubble", title: "Report a bug", destination: ProfilePlaceholderDetail(title: "Report a bug"))
                            ProfileSettingsRow(icon: "star", title: "Rate KIRA", destination: ProfilePlaceholderDetail(title: "Rate KIRA"))
                            ProfileSettingsRow(icon: "square.and.arrow.up", title: "Share with friends", destination: ProfilePlaceholderDetail(title: "Share with friends"))
                        }

                        ProfileSettingsSection(title: "About") {
                            ProfileSettingsRow(icon: "info.circle", title: "About KIRA", destination: ProfilePlaceholderDetail(title: "About KIRA"))
                            ProfileSettingsRow(icon: "hand.raised", title: "Privacy policy", destination: ProfilePlaceholderDetail(title: "Privacy policy"))
                            ProfileSettingsRow(icon: "doc.plaintext", title: "Terms of use", destination: ProfilePlaceholderDetail(title: "Terms of use"))
                            ProfileSettingsRow(icon: "number", title: "Version", value: UIApplication.appVersion ?? "1.0", destination: ProfilePlaceholderDetail(title: "Version"))
                        }

                        Text("Made by Risk Creatives")
                            .font(Font.satoshi(.caption, weight: .medium))
                            .foregroundColor(muted)
                            .padding(.top, 2)

                        Button {
                        } label: {
                            Text("Log out")
                                .font(Font.satoshi(.body, weight: .semibold))
                                .foregroundColor(Color(hex: "FFB29F"))
                                .frame(maxWidth: .infinity)
                                .frame(height: 52)
                                .background(Color.white.opacity(0.06), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
                                .overlay(
                                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                                        .stroke(Color.white.opacity(0.12), lineWidth: 1)
                                )
                        }
                        .buttonStyle(.plain)
                        .padding(.bottom, 28)
                    }
                    .padding(.horizontal, 18)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
            }
            .navigationBarHidden(true)
            .navigationBarTitle("")
        }
        .navigationViewStyle(.stack)
    }

    private var header: some View {
        HStack {
            Button {
                dismiss()
            } label: {
                Image(systemName: "chevron.left")
                    .font(Font.satoshi(.footnote, weight: .semibold))
                    .foregroundColor(foreground)
                    .frame(width: 44, height: 44)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)

            Spacer()

            NavigationLink(destination: SettingsAppearanceView()) {
                Image(systemName: "gearshape")
                    .font(Font.satoshi(.footnote, weight: .semibold))
                    .foregroundColor(foreground)
                    .frame(width: 44, height: 44)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
        }
        .padding(.horizontal, 10)
    }

    private var profileSummary: some View {
        VStack(spacing: 10) {
            Image("kira-ai")
                .resizable()
                .scaledToFill()
                .frame(width: 82, height: 82)
                .clipShape(Circle())
                .overlay(Circle().stroke(Color.white.opacity(0.08), lineWidth: 1))

            VStack(spacing: 2) {
                Text("Adam")
                    .font(Font.satoshi(.title3, weight: .semibold))
                    .foregroundColor(.white)

                HStack(spacing: 5) {
                    Image(systemName: "sparkles")
                        .font(Font.satoshi(9, weight: .bold))
                        .foregroundColor(Color(hex: "FFD85C"))

                    Text("Premium Member")
                        .font(Font.satoshi(.caption2, weight: .semibold))
                        .foregroundColor(Color(hex: "FFD85C"))
                }
            }
        }
    }
}

private struct KIRAProCard: View {
    var body: some View {
        HStack(alignment: .center, spacing: 14) {
            Image(systemName: "sparkles")
                .font(Font.satoshi(18, weight: .bold))
                .foregroundColor(Color(hex: "FFD85C"))
                .frame(width: 42, height: 42)
                .background(Color.white.opacity(0.10), in: Circle())

            VStack(alignment: .leading, spacing: 3) {
                Text("KIRA Pro")
                    .font(Font.satoshi(.headline, weight: .semibold))
                    .foregroundColor(.white)
                Text("Unlock AI reports, receipt scans, and smart budgets")
                    .font(Font.satoshi(.caption, weight: .medium))
                    .foregroundColor(Color.white.opacity(0.62))
                    .fixedSize(horizontal: false, vertical: true)
            }

            Spacer(minLength: 8)

            NavigationLink(destination: ProfilePlaceholderDetail(title: "KIRA Pro")) {
                Text("Upgrade")
                    .font(Font.satoshi(.caption, weight: .semibold))
                    .foregroundColor(Color(hex: "3E4933"))
                    .padding(.horizontal, 14)
                    .frame(height: 34)
                    .background(Color(hex: "F2E7B8"), in: Capsule())
            }
            .buttonStyle(.plain)
        }
        .padding(16)
        .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 24, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 24, style: .continuous)
                .stroke(Color.white.opacity(0.12), lineWidth: 1)
        )
    }
}

private struct ProfileSettingsSection<Content: View>: View {
    let title: String
    @ViewBuilder let content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 9) {
            Text(title)
                .font(Font.satoshi(.caption, weight: .semibold))
                .foregroundColor(Color.white.opacity(0.48))
                .padding(.horizontal, 6)

            VStack(spacing: 0) {
                content
            }
            .background(Color.white.opacity(0.075), in: RoundedRectangle(cornerRadius: 26, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 26, style: .continuous)
                    .stroke(Color.white.opacity(0.10), lineWidth: 1)
            )
        }
    }
}

private struct ProfileSettingsRow<Destination: View>: View {
    let icon: String
    let title: String
    var value: String? = nil
    var isDestructive: Bool = false
    let destination: Destination
    var showsDivider: Bool = true

    private let foreground = Color.white.opacity(0.82)
    private let muted = Color.white.opacity(0.52)
    private let divider = Color.white.opacity(0.12)

    var body: some View {
        NavigationLink(destination: destination) {
            VStack(spacing: 0) {
                HStack(spacing: 12) {
                    Image(systemName: icon)
                        .font(Font.satoshi(18, weight: .medium))
                        .foregroundColor(isDestructive ? Color(hex: "FFB29F") : muted)
                        .frame(width: 34, height: 34)
                        .background(Color.white.opacity(0.07), in: Circle())

                    Text(title)
                        .font(Font.satoshi(.subheadline, weight: .medium))
                        .foregroundColor(isDestructive ? Color(hex: "FFB29F") : foreground)

                    Spacer()

                    if let value {
                        Text(value)
                            .font(Font.satoshi(.caption, weight: .medium))
                            .foregroundColor(muted)
                            .lineLimit(1)
                    }

                    Image(systemName: "chevron.right")
                        .font(Font.satoshi(9, weight: .bold))
                        .foregroundColor(muted)
                }
                .frame(height: 66)
                .padding(.horizontal, 14)

                if showsDivider {
                    Rectangle()
                        .fill(divider)
                        .frame(height: 1)
                        .padding(.leading, 62)
                }
            }
        }
        .buttonStyle(.plain)
    }
}

private struct ProfilePlaceholderDetail: View {
    let title: String

    var body: some View {
        VStack(spacing: 12) {
            Image(systemName: "sparkles")
                .font(Font.satoshi(28, weight: .semibold))
                .foregroundColor(Color(hex: "4B5545"))
                .frame(width: 64, height: 64)
                .background(Color.SecondaryBackground, in: Circle())
            Text(title)
                .font(Font.satoshi(.title3, weight: .semibold))
                .foregroundColor(Color.PrimaryText)
            Text("This setting will be connected to KIRA's full account experience.")
                .font(Font.satoshi(.subheadline, weight: .medium))
                .foregroundColor(Color.SubtitleText)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 26)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.PrimaryBackground.ignoresSafeArea())
        .navigationTitle(title)
        .navigationBarTitleDisplayMode(.inline)
    }
}

private struct SettingsExportPlaceholderView: View {
    var body: some View {
        ProfilePlaceholderDetail(title: "Export data")
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
