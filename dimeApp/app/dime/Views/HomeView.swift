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

enum HomeSheet: Identifiable {
    case profile
    case notifications
    case reports
    case scanReceipt

    var id: String {
        switch self {
        case .profile: return "profile"
        case .notifications: return "notifications"
        case .reports: return "reports"
        case .scanReceipt: return "scanReceipt"
        }
    }
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
    @State private var transactionCountBeforeAdd = 0

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
                    onAddIncome: { presentAddTransaction() },
                    onScanReceipt: { activeSheet = .scanReceipt },
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
            case .scanReceipt:
                ScanReceiptSheetView(onAskKIRA: {
                    activeSheet = nil
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) {
                        openAttachmentOnAIExpand = true
                        revealAskKIRA()
                    }
                })
            }
        }
        .fullScreenCover(isPresented: $showAddTransaction, onDismiss: {
            handleAddTransactionDismiss()
        }) {
            TransactionView(toEdit: nil)
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
            if appLockVM.isAppLockEnabled && fromURL4 { currentTab = "Plan" }
        }
        .onOpenURL { url in
            if url.host == "search" { currentTab = "Activity" }
            else if url.host == "insights" { activeSheet = .reports }
            else if url.host == "budget" { currentTab = "Plan" }
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
                    isScrollLocked: liveProgress > 0.01,
                    onScrollStateChanged: { isAtTop, _ in isLogAtTop = isAtTop },
                    onAskKIRA: { revealAskKIRA() },
                    onScanReceipt: { activeSheet = .scanReceipt },
                    onAddTransaction: { presentAddTransaction() },
                    onViewActivity: { currentTab = "Activity" },
                    onReports: { activeSheet = .reports }
                )
                .ignoresSafeArea(.all)
                .tag("Home")

                PlanView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .tag("Plan")

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

    private func presentAddTransaction() {
        transactionCountBeforeAdd = dataController.results(for: Transaction.fetchRequest()).count
        showAddTransaction = true
    }

    private func handleAddTransactionDismiss() {
        let currentCount = dataController.results(for: Transaction.fetchRequest()).count
        if currentCount != transactionCountBeforeAdd {
            counter += 1
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

struct ScanReceiptSheetView: View {
    @Environment(\.dismiss) private var dismiss
    var onAskKIRA: () -> Void

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Button { dismiss() } label: {
                    Image(systemName: "xmark")
                        .font(Font.satoshi(.callout, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                        .frame(width: 42, height: 42)
                        .background(Color.SecondaryBackground, in: Circle())
                }

                Spacer()

                Text("Scan receipt")
                    .font(Font.satoshi(.headline, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)

                Spacer()

                Color.clear.frame(width: 42, height: 42)
            }
            .padding(.horizontal, 20)
            .padding(.top, 18)

            Spacer()

            VStack(spacing: 16) {
                Image(systemName: "viewfinder")
                    .font(Font.satoshi(40, weight: .medium))
                    .foregroundColor(Color(hex: "4A5240"))
                    .frame(width: 88, height: 88)
                    .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 24, style: .continuous))

                VStack(spacing: 6) {
                    Text("Receipt scanning is moving here.")
                        .font(Font.satoshi(.title3, weight: .semibold))
                        .foregroundColor(Color.PrimaryText)
                    Text("Use Ask KIRA for receipt help while the scan review flow is wired into this sheet.")
                        .font(Font.satoshi(.subheadline, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                        .multilineTextAlignment(.center)
                }

                Button {
                    onAskKIRA()
                } label: {
                    Label("Ask KIRA", systemImage: "sparkles")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(Color.LightIcon)
                        .frame(maxWidth: .infinity)
                        .frame(height: 52)
                        .background(Color.DarkBackground, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
                }
                .padding(.top, 8)
            }
            .padding(.horizontal, 24)

            Spacer()
        }
        .background(Color.PrimaryBackground.ignoresSafeArea())
    }
}

struct ProfileView: View {
    private let background = Color(hex: "4B5545")
    private let foreground = Color.white.opacity(0.86)

    var body: some View {
        NavigationView {
            ZStack {
                background
                    .ignoresSafeArea()

                VStack(spacing: 0) {
                    header
                        .padding(.top, 18)

                    profileSummary
                        .padding(.top, 18)
                        .padding(.bottom, 28)

                    VStack(spacing: 0) {
                        ProfileNavigationRow(
                            icon: "person",
                            title: "Personal Details",
                            destination: SettingsView()
                        )

                        ProfileNavigationRow(
                            icon: "shield",
                            title: "Security & Privacy",
                            destination: SettingsView()
                        )

                        ProfileNavigationRow(
                            icon: "bell",
                            title: "Notifications",
                            destination: SettingsNotificationsView()
                        )

                        ProfileNavigationRow(
                            icon: "gearshape",
                            title: "Preferences",
                            destination: SettingsView()
                        )

                        ProfileNavigationRow(
                            icon: "questionmark.circle",
                            title: "Help & Support",
                            destination: SettingsView()
                        )

                        ProfileNavigationRow(
                            icon: "info.circle",
                            title: "About KIRA",
                            destination: SettingsView(),
                            showsDivider: false
                        )
                    }
                    .padding(.horizontal, 14)

                    Button {
                    } label: {
                        Text("Log out")
                            .font(Font.satoshi(.caption, weight: .semibold))
                            .foregroundColor(Color(hex: "FFB29F"))
                            .frame(maxWidth: .infinity)
                            .frame(height: 50)
                            .background(Color.white.opacity(0.08), in: RoundedRectangle(cornerRadius: 10, style: .continuous))
                            .overlay(
                                RoundedRectangle(cornerRadius: 10, style: .continuous)
                                    .stroke(Color.white.opacity(0.10), lineWidth: 1)
                            )
                    }
                    .buttonStyle(.plain)
                    .padding(.horizontal, 14)
                    .padding(.top, 34)

                    Spacer(minLength: 0)
                }
                .padding(.horizontal, 0)
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
            } label: {
                Image(systemName: "chevron.left")
                    .font(Font.satoshi(.footnote, weight: .semibold))
                    .foregroundColor(foreground)
                    .frame(width: 44, height: 44)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)

            Spacer()

            NavigationLink(destination: SettingsView()) {
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
        VStack(spacing: 8) {
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

                    Image(systemName: "chevron.down")
                        .font(Font.satoshi(7, weight: .bold))
                        .foregroundColor(Color(hex: "FFD85C").opacity(0.8))
                }
            }
        }
    }
}

private struct ProfileNavigationRow<Destination: View>: View {
    let icon: String
    let title: String
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
                        .font(Font.satoshi(.caption, weight: .medium))
                        .foregroundColor(muted)
                        .frame(width: 16, height: 16)

                    Text(title)
                        .font(Font.satoshi(.caption, weight: .medium))
                        .foregroundColor(foreground)

                    Spacer()

                    Image(systemName: "chevron.right")
                        .font(Font.satoshi(9, weight: .bold))
                        .foregroundColor(muted)
                }
                .frame(height: 48)

                if showsDivider {
                    Rectangle()
                        .fill(divider)
                        .frame(height: 1)
                        .padding(.leading, 28)
                }
            }
        }
        .buttonStyle(.plain)
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
