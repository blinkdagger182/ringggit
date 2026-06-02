//
//  BudgetView.swift
//  xpenz
//
//  Created by Rafael Soh on 20/5/22.
//

import CoreData
import Foundation
import Popovers
import SwiftUI

private enum PlanSegment: String, CaseIterable {
    case budgets = "Budgets"
    case goals = "Goals"
}

struct PlanView: View {
    @EnvironmentObject private var dataController: DataController
    @EnvironmentObject private var tabBarManager: TabBarManager

    @FetchRequest(sortDescriptors: [SortDescriptor(\.dateCreated)]) private var budgets: FetchedResults<Budget>
    @FetchRequest(sortDescriptors: []) private var mainBudget: FetchedResults<MainBudget>
    @FetchRequest(sortDescriptors: [SortDescriptor(\.dateCreated)]) private var buckets: FetchedResults<Bucket>

    @State private var selectedSegment: PlanSegment = .budgets
    @State private var newBudget = false
    @State private var newGoal = false
    @State private var budgetToEdit: Budget?
    @State private var budgetToDelete: Budget?
    @State private var goalToEdit: Bucket?
    @State private var goalToDelete: Bucket?

    @AppStorage("budgetViewStyle", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var budgetRows: Bool = false

    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                header

                Picker("Plan", selection: $selectedSegment) {
                    ForEach(PlanSegment.allCases, id: \.self) { segment in
                        Text(segment.rawValue).tag(segment)
                    }
                }
                .pickerStyle(.segmented)
                .padding(.horizontal, 20)
                .padding(.bottom, 14)

                ScrollView(showsIndicators: false) {
                    Group {
                        switch selectedSegment {
                        case .budgets:
                            budgetsContent
                        case .goals:
                            goalsContent
                        }
                    }
                    .padding(.bottom, 96)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
            .background(Color.PrimaryBackground.ignoresSafeArea())
            .navigationBarHidden(true)
            .onAppear {
                dataController.updateBudgetDates()
            }
            .sheet(isPresented: $newBudget) {
                BrandNewBudgetView(overallBudgetCreated: !mainBudget.isEmpty)
            }
            .sheet(item: $budgetToEdit, onDismiss: {
                budgetToEdit = nil
            }) { budget in
                BrandNewBudgetView(overallBudgetCreated: !mainBudget.isEmpty, toEditBudget: budget)
            }
            .fullScreenCover(isPresented: $newGoal) {
                BucketEditorSheet()
            }
            .fullScreenCover(item: $goalToEdit, onDismiss: {
                goalToEdit = nil
            }) { goal in
                BucketEditorSheet(bucket: goal)
            }
            .fullScreenCover(item: $budgetToDelete, onDismiss: {
                budgetToDelete = nil
            }) { budget in
                DeleteBudgetAlert(toDelete: budget)
            }
            .fullScreenCover(item: $goalToDelete, onDismiss: {
                goalToDelete = nil
            }) { goal in
                DeleteBucketAlert(bucket: goal)
            }
        }
        .navigationViewStyle(.stack)
    }

    private var header: some View {
        HStack(alignment: .center) {
            VStack(alignment: .leading, spacing: 3) {
                Text("Plan")
                    .font(Font.satoshi(.largeTitle, weight: .bold))
                    .foregroundColor(Color.PrimaryText)
                    .accessibility(addTraits: .isHeader)
                Text("Budgets and goals in one place.")
                    .font(Font.satoshi(.subheadline, weight: .medium))
                    .foregroundColor(Color.SubtitleText)
            }

            Spacer()

            Button {
                if selectedSegment == .budgets {
                    newBudget = true
                } else {
                    newGoal = true
                }
            } label: {
                Image(systemName: "plus")
                    .font(Font.satoshi(16, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                    .frame(width: 42, height: 42)
                    .background(Color.SecondaryBackground, in: Circle())
                    .overlay(Circle().stroke(Color.Outline.opacity(0.7), lineWidth: 1))
            }
            .buttonStyle(.plain)
        }
        .padding(.horizontal, 20)
        .padding(.top, 18)
        .padding(.bottom, 16)
    }

    private var budgetsContent: some View {
        VStack(alignment: .leading, spacing: 14) {
            if mainBudget.isEmpty && budgets.isEmpty {
                EmptyStateView(
                    systemImage: "wallet.pass",
                    title: "Create your first budget.",
                    message: "Keep spending on track with a simple limit."
                )
                .padding(.horizontal, 20)

                primaryPlanButton(title: "Create budget") {
                    newBudget = true
                }
                .padding(.horizontal, 20)
            } else {
                if let first = mainBudget.first {
                    NavigationLink(destination: DetailedMainBudgetView(budget: first)) {
                        MainBudgetView(budget: first, solo: true)
                    }
                    .buttonStyle(.plain)
                    .padding(.horizontal, 20)
                }

                VStack(alignment: .leading, spacing: 10) {
                    Text("Category budgets")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(Color.PrimaryText)
                        .padding(.horizontal, 20)

                    if budgets.isEmpty {
                        EmptyStateView(
                            systemImage: "square.grid.2x2",
                            title: "No category budgets yet.",
                            message: "Create one for food, shopping, bills, or anything else."
                        )
                        .padding(.horizontal, 20)
                    } else {
                        LazyVGrid(columns: [GridItem(.flexible(), spacing: 12), GridItem(.flexible(), spacing: 12)], spacing: 12) {
                            ForEach(budgets, id: \.self) { budget in
                                NavigationLink(destination: DetailedBudgetView(budget: budget)) {
                                    SingleBudgetView(
                                        budget: budget,
                                        toDelete: $budgetToDelete,
                                        toEdit: $budgetToEdit,
                                        budgetRows: budgetRows
                                    )
                                }
                                .buttonStyle(.plain)
                            }
                        }
                        .padding(.horizontal, 20)
                    }
                }
            }
        }
    }

    private var goalsContent: some View {
        VStack(alignment: .leading, spacing: 14) {
            if buckets.isEmpty {
                EmptyStateView(
                    systemImage: "flag",
                    title: "Create a goal.",
                    message: "Use goals for trips, savings, or future spending."
                )
                .padding(.horizontal, 20)

                primaryPlanButton(title: "Create goal") {
                    newGoal = true
                }
                .padding(.horizontal, 20)
            } else {
                VStack(spacing: 10) {
                    ForEach(buckets, id: \.self) { goal in
                        NavigationLink(destination: DetailedBucketView(bucket: goal)) {
                            GoalSummaryRow(goal: goal, onEdit: {
                                goalToEdit = goal
                            }, onDelete: {
                                goalToDelete = goal
                            })
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.horizontal, 20)
            }
        }
    }

    private func primaryPlanButton(title: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Text(title)
                .font(Font.satoshi(.body, weight: .semibold))
                .foregroundColor(Color.LightIcon)
                .frame(maxWidth: .infinity)
                .frame(height: 52)
                .background(Color.DarkBackground, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        }
    }
}

private struct GoalSummaryRow: View {
    let goal: Bucket
    let onEdit: () -> Void
    let onDelete: () -> Void

    @EnvironmentObject private var dataController: DataController
    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) private var currency: String = Locale.current.currencyCode!

    private var currencySymbol: String {
        Locale.current.localizedCurrencySymbol(forCurrencyCode: currency) ?? "RM"
    }

    private var transactions: [Transaction] {
        let request: NSFetchRequest<Transaction> = Transaction.fetchRequest()
        request.predicate = NSPredicate(format: "bucket == %@", goal)
        request.sortDescriptors = [NSSortDescriptor(key: "date", ascending: false)]
        return (try? dataController.container.viewContext.fetch(request)) ?? []
    }

    private var totalAmount: Double {
        transactions.reduce(0) { $0 + $1.wrappedAmount }
    }

    private var timeframeSummary: String? {
        guard let timeframe = goal.timeframe else { return nil }

        switch timeframe {
        case BucketTimeframeOption.thisMonth.rawValue:
            return "This month"
        case BucketTimeframeOption.thisYear.rawValue:
            return "This year"
        case BucketTimeframeOption.customRange.rawValue:
            guard let start = goal.startDate, let end = goal.endDate else { return "Custom range" }
            let formatter = DateFormatter()
            formatter.dateFormat = "d MMM"
            return "\(formatter.string(from: start)) - \(formatter.string(from: end))"
        default:
            return nil
        }
    }

    var body: some View {
        HStack(alignment: .center, spacing: 12) {
            Text(goal.emoji ?? "🏷️")
                .font(Font.satoshi(24))
                .frame(width: 44, height: 44)
                .background(Color(hex: goal.colour ?? "4A5240").opacity(0.15), in: RoundedRectangle(cornerRadius: 14, style: .continuous))

            VStack(alignment: .leading, spacing: 4) {
                Text(goal.name ?? "Untitled Goal")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)

                HStack(spacing: 6) {
                    Text("\(transactions.count) \(transactions.count == 1 ? "transaction" : "transactions")")
                    if let timeframeSummary {
                        Text("•")
                        Text(timeframeSummary)
                    }
                }
                .font(Font.satoshi(.caption, weight: .medium))
                .foregroundColor(Color.SubtitleText)
                .lineLimit(1)
            }

            Spacer()

            Text("\(currencySymbol)\(totalAmount, specifier: "%.2f")")
                .font(Font.satoshi(.title3, weight: .semibold))
                .foregroundColor(Color.PrimaryText)
                .lineLimit(1)
                .minimumScaleFactor(0.7)
        }
        .padding(14)
        .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(Color.Outline.opacity(0.7), lineWidth: 1)
        )
        .contextMenu {
            Button(action: onEdit) {
                Label("Edit Goal", systemImage: "pencil")
            }

            Button(role: .destructive, action: onDelete) {
                Label("Delete Goal", systemImage: "trash")
            }
        }
    }
}

struct BudgetView: View {
    @FetchRequest(sortDescriptors: []) private var categories: FetchedResults<Category>
    @FetchRequest(sortDescriptors: []) private var budgets: FetchedResults<Budget>
    @FetchRequest(sortDescriptors: []) private var mainBudget: FetchedResults<MainBudget>
    @FetchRequest(sortDescriptors: [NSSortDescriptor(key: "dateCreated", ascending: true)]) private var buckets: FetchedResults<Bucket>

    var body: some View {
        if categories.isEmpty && budgets.isEmpty && mainBudget.isEmpty && buckets.isEmpty {
            VStack(spacing: 5) {
                Image("category-3")
                    .resizable()
                    .frame(width: 75, height: 75)
                    .padding(.bottom, 20)

                Text("Budget Your Finances")
                    .font(Font.satoshi(.title2, weight: .medium))
//                    .font(Font.satoshi(23.5, weight: .medium))
                    .multilineTextAlignment(.center)
                    .foregroundColor(Color.PrimaryText.opacity(0.8))

                Text("Link budgets to categories and set appropriate expenditure goals")
                    .font(Font.satoshi(.body, weight: .medium))
//                    .font(Font.satoshi(18, weight: .medium))
                    .multilineTextAlignment(.center)
                    .foregroundColor(Color.SubtitleText.opacity(0.7))
            }
            .padding(.horizontal, 30)
            .frame(height: 250, alignment: .top)
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .ignoresSafeArea(.all)
            .background(
                ZStack {
                    Color.PrimaryBackground
                    HomeAIAnimatedGradientBackground()
                        .opacity(0.9)
                        .mask(
                            LinearGradient(
                                colors: [.clear, .black, .black],
                                startPoint: .top,
                                endPoint: .bottom
                            )
                        )
                }
            )

        } else {
            ActualBudgetView()
        }
    }
}

struct ActualBudgetView: View {
    @Environment(\.colorScheme) var colorScheme
    @State private var showInfo = false

    @FetchRequest(sortDescriptors: [
        SortDescriptor(\.dateCreated)
    ]) private var budgets: FetchedResults<Budget>
    @FetchRequest(sortDescriptors: []) private var mainBudget: FetchedResults<MainBudget>
    @FetchRequest(sortDescriptors: [SortDescriptor(\.dateCreated)]) private var buckets: FetchedResults<Bucket>
    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController
    @EnvironmentObject var tabBarManager: TabBarManager

    @State var newBudget = false
    @State private var newBucket = false
    @State private var bucketToEdit: Bucket?
    @State private var bucketToDelete: Bucket?

    @State private var showMenu = false

    @State private var toDelete: Budget?
    @State private var toEdit: Budget?

    let layout = [
        GridItem(.flexible(), spacing: 15),
        GridItem(.flexible(), spacing: 15)
    ]

    @AppStorage("budgetViewStyle", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var budgetRows: Bool = false

    @Namespace var animation

    var didSave = NotificationCenter.default.publisher(for: .NSManagedObjectContextDidSave) // the publisher
    @AppStorage("UUID") var refreshID = UUID().uuidString

    @State var date = Date.now

    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                HStack(alignment: .center) {
                    Text("BUDGETS")
                        .font(.system(size: 36, weight: .black))
                        .tracking(1.5)
                        .foregroundColor(Color.PrimaryText)
                        .accessibility(addTraits: .isHeader)

                    Spacer()

                    Button {
                        newBudget = true
                    } label: {
                        Image(systemName: "plus")
                            .font(Font.satoshi(16, weight: .semibold))
                            .foregroundColor(Color.PrimaryText)
                            .frame(width: 40, height: 40)
                            .background(Color.SecondaryBackground, in: Circle())
                    }
                }
                .frame(maxWidth: .infinity)
                .padding(.top, 20)
                .padding(.horizontal, 20)
                .padding(.bottom, 16)

                if !budgets.isEmpty || !mainBudget.isEmpty || !buckets.isEmpty {
                    ScrollView(showsIndicators: false) {
                        VStack {
                            if let first = mainBudget.first {
                                NavigationLink(destination: DetailedMainBudgetView(budget: first)
                                    .onAppear {
                                        withAnimation(.easeOut.speed(2)) {
                                            tabBarManager.navigationHideTab()
                                        }
                                    }
                                    .onDisappear {
                                        withAnimation(.easeOut.speed(2)) {
                                            tabBarManager.navigationShowTab()
                                        }
                                    }

                                ) {
                                    if budgets.count == 0 {
                                        MainBudgetView(budget: first, solo: true)
                                            .padding(.horizontal, 25)
                                            .padding(.bottom, 15)
                                            .id(refreshID)
                                    } else {
                                        MainBudgetView(budget: first, solo: false)
                                            .padding(.horizontal, 25)
                                            .padding(.bottom, 15)
                                            .id(refreshID)
                                    }
                                }
                            }

                            if budgetRows {
                                VStack(spacing: 10) {
                                    ForEach(budgets, id: \.self) { budget in
                                        NavigationLink(destination: DetailedBudgetView(budget: budget)
                                            .onAppear {
                                                withAnimation(.easeOut.speed(2)) {
                                                    tabBarManager.navigationHideTab()
                                                }
                                            }
                                            .onDisappear {
                                                withAnimation(.easeOut.speed(2)) {
                                                    tabBarManager.navigationShowTab()
                                                }
                                            }

                                        ) {
                                            SingleBudgetView(budget: budget, toDelete: $toDelete, toEdit: $toEdit, budgetRows: budgetRows)
                                        }
                                    }
                                }

                            } else {
                                LazyVGrid(columns: layout, spacing: 15) {
                                    ForEach(budgets, id: \.self) { budget in
                                        NavigationLink(destination: DetailedBudgetView(budget: budget)
                                            .onAppear {
                                                withAnimation(.easeOut.speed(2)) {
                                                    tabBarManager.navigationHideTab()
                                                }
                                            }
                                            .onDisappear {
                                                withAnimation(.easeOut.speed(2)) {
                                                    tabBarManager.navigationShowTab()
                                                }
                                            }

                                        ) {
                                            SingleBudgetView(budget: budget, toDelete: $toDelete, toEdit: $toEdit, budgetRows: budgetRows)
                                        }
                                    }
                                }
                                .padding(.horizontal, 25)
                                .padding(5)
                            }

                            VStack(alignment: .leading, spacing: 10) {
                                HStack {
                                    Text("GOALS")
                                        .font(.system(size: 18, weight: .black))
                                        .tracking(1.2)
                                        .foregroundColor(Color.PrimaryText)

                                    Spacer()

                                    Button {
                                        newBucket = true
                                    } label: {
                                        Image(systemName: "plus")
                                            .font(Font.satoshi(14, weight: .semibold))
                                            .foregroundColor(Color.PrimaryText)
                                            .frame(width: 32, height: 32)
                                            .background(Color.SecondaryBackground, in: Circle())
                                    }
                                }
                                .padding(.horizontal, 20)
                                .padding(.top, 8)

                                if buckets.isEmpty {
                                    Text("Create goals for trips, savings, or future spending.")
                                        .font(Font.satoshi(.subheadline, weight: .medium))
                                        .foregroundColor(Color.SubtitleText)
                                        .padding(.horizontal, 20)
                                } else {
                                    VStack(spacing: 10) {
                                        ForEach(buckets, id: \.self) { bucket in
                                            NavigationLink(destination: DetailedBucketView(bucket: bucket)) {
                                                BucketSummaryRow(bucket: bucket, onEdit: {
                                                    bucketToEdit = bucket
                                                }, onDelete: {
                                                    bucketToDelete = bucket
                                                })
                                            }
                                            .buttonStyle(.plain)
                                        }
                                    }
                                    .padding(.horizontal, 20)
                                }
                            }
                            .padding(.top, 8)
                        }
                        .padding(.bottom, 70)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .id(refreshID)
                } else {
                    VStack(spacing: 5) {
                        Spacer()
                        Text("🙈")
                            .font(Font.satoshi(.largeTitle))
//                            .font(Font.satoshi(45))
                            .padding(.bottom, 9)

                        Text("No Budgets Found")
                            .font(Font.satoshi(.title2, weight: .medium))
//                            .font(Font.satoshi(23.5, weight: .medium))
                            .multilineTextAlignment(.center)
                            .foregroundColor(Color.PrimaryText.opacity(0.8))

                        Text("Add your first budget today!")
                            .font(Font.satoshi(.body, weight: .medium))
//                            .font(Font.satoshi(18, weight: .medium))
                            .multilineTextAlignment(.center)
                            .foregroundColor(Color.SubtitleText.opacity(0.7))

                        Spacer()
                        Spacer()
                    }
                    .padding(20)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
            .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
            .navigationBarTitle("")
            .navigationBarHidden(true)
            .background(
                ZStack {
                    Color.PrimaryBackground
                    HomeAIAnimatedGradientBackground()
                        .opacity(0.9)
                        .mask(
                            LinearGradient(
                                colors: [.clear, .black, .black],
                                startPoint: .top,
                                endPoint: .bottom
                            )
                        )
                }
                .ignoresSafeArea()
            )
            .sheet(item: $toEdit, onDismiss: {
                toEdit = nil
            }) { budget in
                BrandNewBudgetView(overallBudgetCreated: !mainBudget.isEmpty, toEditBudget: budget)
            }
            .onAppear {
                dataController.updateBudgetDates()
            }
            .sheet(isPresented: $newBudget) {
                BrandNewBudgetView(overallBudgetCreated: !mainBudget.isEmpty)
            }
            .fullScreenCover(isPresented: $newBucket) {
                BucketEditorSheet()
            }
            .fullScreenCover(item: $bucketToEdit, onDismiss: {
                bucketToEdit = nil
            }) { bucket in
                BucketEditorSheet(bucket: bucket)
            }
            .fullScreenCover(item: $toDelete, onDismiss: {
                toDelete = nil
            }) { budget in
                DeleteBudgetAlert(toDelete: budget)
            }
            .fullScreenCover(item: $bucketToDelete, onDismiss: {
                bucketToDelete = nil
            }) { bucket in
                DeleteBucketAlert(bucket: bucket)
            }
            .onReceive(self.didSave) { _ in // the listener
                withAnimation {
                    refreshID = UUID().uuidString
                }
            }
        }
    }
}

struct BucketSummaryRow: View {
    let bucket: Bucket
    let onEdit: () -> Void
    let onDelete: () -> Void

    @EnvironmentObject var dataController: DataController

    private var transactions: [Transaction] {
        let request: NSFetchRequest<Transaction> = Transaction.fetchRequest()
        request.predicate = NSPredicate(format: "bucket == %@", bucket)
        request.sortDescriptors = [NSSortDescriptor(key: "date", ascending: false)]
        return (try? dataController.container.viewContext.fetch(request)) ?? []
    }

    private var totalAmount: Double {
        transactions.reduce(0) { $0 + $1.wrappedAmount }
    }

    private var timeframeSummary: String? {
        guard let timeframe = bucket.timeframe else { return nil }

        switch timeframe {
        case BucketTimeframeOption.thisMonth.rawValue:
            return "This month"
        case BucketTimeframeOption.thisYear.rawValue:
            return "This year"
        case BucketTimeframeOption.customRange.rawValue:
            guard let start = bucket.startDate, let end = bucket.endDate else { return "Custom range" }
            let formatter = DateFormatter()
            formatter.dateFormat = "d MMM"
            return "\(formatter.string(from: start)) - \(formatter.string(from: end))"
        default:
            return nil
        }
    }

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Text(bucket.emoji ?? "🏷️")
                .font(Font.satoshi(24))
                .frame(width: 42, height: 42)
                .background(Color(hex: bucket.colour ?? "4A5240").opacity(0.15), in: RoundedRectangle(cornerRadius: 14, style: .continuous))

            VStack(alignment: .leading, spacing: 3) {
                Text(bucket.name ?? "Untitled Goal")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)

                HStack(spacing: 6) {
                    Text("\(transactions.count) \(transactions.count == 1 ? "transaction" : "transactions")")
                        .font(Font.satoshi(.caption, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                        .lineLimit(1)
                        .fixedSize(horizontal: true, vertical: false)

                    if let timeframeSummary {
                        Circle()
                            .fill(Color.SubtitleText.opacity(0.4))
                            .frame(width: 3, height: 3)

                        Text(timeframeSummary)
                            .font(Font.satoshi(.caption, weight: .medium))
                            .foregroundColor(Color.SubtitleText)
                            .lineLimit(1)
                    }
                }
            }

            Spacer()

            Text(String(format: "RM %.2f", totalAmount))
                .font(Font.satoshi(.title3, weight: .semibold))
                .foregroundColor(Color.PrimaryText)
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 14)
        .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        .contextMenu {
            Button {
                onEdit()
            } label: {
                Label("Edit Goal", systemImage: "pencil")
            }

            Button(role: .destructive) {
                onDelete()
            } label: {
                Label("Delete Goal", systemImage: "trash")
            }
        }
    }
}

private struct BudgetLimitMarker: View {
    let tint: Color

    var body: some View {
        Capsule(style: .continuous)
            .fill(Color.PrimaryBackground.opacity(0.96))
            .frame(width: 7, height: 18)
            .overlay(
                Capsule(style: .continuous)
                    .stroke(tint.opacity(0.65), lineWidth: 1.4)
            )
            .shadow(color: Color.black.opacity(0.08), radius: 4, y: 1)
    }
}

private struct NewBudgetSpentBar: View {
    let color: Color
    let percent: Double
    let totalWidth: CGFloat
    let height: CGFloat

    @State private var animWidth: CGFloat = 0
    @AppStorage("animated", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var animated: Bool = true

    var body: some View {
        ZStack(alignment: .leading) {
            RoundedRectangle(cornerRadius: height / 2, style: .continuous)
                .fill(color.opacity(0.12))
                .frame(width: totalWidth, height: height)
            RoundedRectangle(cornerRadius: height / 2, style: .continuous)
                .fill(color)
                .frame(width: max(animWidth, percent > 0 ? height : 0), height: height)
        }
        .frame(width: totalWidth, height: height, alignment: .leading)
        .onAppear {
            let target = totalWidth * CGFloat(min(max(percent, 0), 1))
            if !animated { animWidth = target }
            else { withAnimation(.easeInOut(duration: 0.7)) { animWidth = target } }
        }
    }
}

private struct OverBudgetHorizontalBar: View {
    let fillColor: Color
    let boundaryPercent: Double
    let totalWidth: CGFloat
    let height: CGFloat

    @State private var redOffset: CGFloat = 0
    @AppStorage("animated", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var animated: Bool = true

    var body: some View {
        let greenWidth = totalWidth * min(max(boundaryPercent, 0), 1)

        return ZStack(alignment: .leading) {
            RoundedRectangle(cornerRadius: height / 2, style: .continuous)
                .fill(fillColor)
                .frame(width: greenWidth, height: height)

            ZStack {
                Color.BudgetRed
                Canvas { context, size in
                    let stripeW: CGFloat = 3
                    let gap: CGFloat = 6
                    var x: CGFloat = -size.height
                    while x < size.width + size.height {
                        let path = Path { p in
                            p.move(to: CGPoint(x: x, y: 0))
                            p.addLine(to: CGPoint(x: x + size.height, y: size.height))
                            p.addLine(to: CGPoint(x: x + size.height + stripeW, y: size.height))
                            p.addLine(to: CGPoint(x: x + stripeW, y: 0))
                        }
                        context.fill(path, with: .color(.white.opacity(0.22)))
                        x += gap + stripeW
                    }
                }
            }
            .frame(width: totalWidth, height: height)
            .offset(x: redOffset)

            BudgetLimitMarker(tint: fillColor)
                .offset(x: min(max(greenWidth - 3.5, 0), max(totalWidth - 7, 0)))
        }
        .frame(width: totalWidth, height: height, alignment: .leading)
        .clipShape(RoundedRectangle(cornerRadius: height / 2, style: .continuous))
        .onAppear {
            if !animated { redOffset = greenWidth }
            else { withAnimation(.easeInOut(duration: 0.7)) { redOffset = greenWidth } }
        }
    }
}

enum BucketTimeframeOption: String, CaseIterable {
    case none
    case thisMonth
    case thisYear
    case customRange

    var title: String {
        switch self {
        case .none: return "No time"
        case .thisMonth: return "This month"
        case .thisYear: return "This year"
        case .customRange: return "Custom range"
        }
    }

    var subtitle: String {
        switch self {
        case .none: return "Keep this bucket available without date limits."
        case .thisMonth: return "Focus it on the current month."
        case .thisYear: return "Tie it to the current year."
        case .customRange: return "Pick your own start and end dates."
        }
    }
}

struct BucketNamePreset: Hashable {
    let emoji: String
    let title: String

    var displayName: String {
        "\(emoji) \(title)"
    }
}

struct BucketEditorSheet: View {
    @Environment(\.dismiss) var dismiss
    @Environment(\.managedObjectContext) var moc
    @Environment(\.colorScheme) var colorScheme

    let bucket: Bucket?

    @State private var progress = 1
    @State private var name: String
    @State private var timeframe: BucketTimeframeOption
    @State private var startDate: Date
    @State private var endDate: Date
    @State private var emoji: String
    @State private var colour: String
    @State private var selectedColor: Color
    @State private var showEmojiPicker = false

    private let presets: [BucketNamePreset] = [
        .init(emoji: "🌤️", title: "Weekend Reset"),
        .init(emoji: "✈️", title: "July Europe Trip"),
        .init(emoji: "🏠", title: "House Renovation"),
        .init(emoji: "💍", title: "Wedding"),
        .init(emoji: "🧾", title: "Tax Claim"),
        .init(emoji: "👨‍👩‍👧", title: "Family Expenses")
    ]

    private let swatches = [
        "7B8FF8", "4ECDC4", "FF8A65", "F06292",
        "F7C65B", "7CC576", "8E7DF2", "5B8DEF"
    ]
    private let emojiChoices = [
        "🏷️", "✈️", "🧾", "🏠", "💍", "🎉", "🛠️", "🚗",
        "🎓", "💼", "👨‍👩‍👧", "🍜", "📦", "📚", "🏖️", "🩺",
        "🎁", "🪑", "💻", "📸", "🏕️", "🚲", "🎟️", "🛒"
    ]

    init(bucket: Bucket? = nil) {
        self.bucket = bucket
        _name = State(initialValue: bucket?.name ?? "")
        _timeframe = State(initialValue: BucketTimeframeOption(rawValue: bucket?.timeframe ?? "") ?? .none)
        _startDate = State(initialValue: bucket?.startDate ?? Date.now)
        _endDate = State(initialValue: bucket?.endDate ?? Date.now)
        _emoji = State(initialValue: bucket?.emoji ?? "🏷️")
        _colour = State(initialValue: bucket?.colour ?? "4A5240")
        _selectedColor = State(initialValue: Color(hex: bucket?.colour ?? "4A5240"))
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Button {
                    if progress == 1 {
                        dismiss()
                    } else {
                        withAnimation(.interactiveSpring(response: 0.6, dampingFraction: 0.8, blendDuration: 0.8)) {
                            progress -= 1
                        }
                    }
                } label: {
                    Image(systemName: progress == 1 ? "xmark" : "chevron.left")
                        .font(Font.satoshi(.callout, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                        .padding(8)
                        .background(Color.SecondaryBackground, in: Circle())
                }

                Spacer()

                BucketCreationProgressView(currentStep: progress, totalSteps: 3)
            }
            .padding(.bottom, 50)

            VStack(alignment: .leading, spacing: 5) {
                Text(progress == 1 ? "Name your bucket" : progress == 2 ? "Choose a time frame" : "Style your bucket")
                    .foregroundColor(.PrimaryText)
                    .font(Font.satoshi(.title2, weight: .semibold))
                    .frame(maxWidth: .infinity, alignment: .leading)

                Text(progress == 1 ? "Goals work best when they feel specific and memorable." : progress == 2 ? "You can leave it open-ended or tie it to a date range." : "Pick a tag color and emoji that will stand out across the app.")
                    .foregroundColor(.SubtitleText)
                    .font(Font.satoshi(.body, weight: .medium))
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(height: 130, alignment: .top)

            Group {
                if progress == 1 {
                    bucketNameStep
                } else if progress == 2 {
                    bucketTimeStep
                } else {
                    bucketStyleStep
                }
            }

            Spacer()

            Button {
                if progress < 3 {
                    withAnimation(.interactiveSpring(response: 0.6, dampingFraction: 0.8, blendDuration: 0.8)) {
                        progress += 1
                    }
                } else {
                    saveBucket()
                }
            } label: {
                Text(progress == 3 ? (bucket == nil ? "Create Goal" : "Save Goal") : "Continue")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundColor(.white)
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 14)
                    .background(Color.DarkBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
            .disabled(!canContinue)
            .opacity(canContinue ? 1 : 0.45)
        }
        .padding(.horizontal, 30)
        .padding(.top, 20)
        .padding(.bottom, 16)
        .background(
            ZStack {
                Color.PrimaryBackground
                HomeAIAnimatedGradientBackground()
                    .opacity(0.9)
            }
            .ignoresSafeArea()
        )
        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
        .sheet(isPresented: $showEmojiPicker) {
            if #available(iOS 16.0, *) {
                BucketEmojiPickerSheet(selectedEmoji: $emoji, emojis: emojiChoices)
                    .presentationDetents([.height(360)])
                    .presentationDragIndicator(.visible)
            } else {
                BucketEmojiPickerSheet(selectedEmoji: $emoji, emojis: emojiChoices)
            }
        }
    }

    private var canContinue: Bool {
        switch progress {
        case 1:
            return !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        case 2:
            return timeframe != .customRange || startDate <= endDate
        default:
            return !emoji.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        }
    }

    private var bucketNameStep: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack(spacing: 12) {
                Button {
                    showEmojiPicker = true
                } label: {
                    Text(displayEmoji)
                        .font(Font.satoshi(26))
                        .frame(width: 52, height: 52)
                        .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                        .overlay(
                            RoundedRectangle(cornerRadius: 14, style: .continuous)
                                .stroke(Color.Outline.opacity(0.8), lineWidth: 1)
                        )
                }
                .buttonStyle(.plain)

                TextField("Goal name", text: $name)
                    .font(Font.satoshi(.body, weight: .medium))
                    .padding(12)
                    .frame(maxWidth: .infinity, minHeight: 52)
                    .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            }

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 10) {
                ForEach(presets, id: \.self) { preset in
                    Button {
                        name = preset.title
                        emoji = preset.emoji
                    } label: {
                        Text(preset.displayName)
                            .font(Font.satoshi(.subheadline, weight: .medium))
                            .foregroundColor(Color.PrimaryText)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(.horizontal, 12)
                            .padding(.vertical, 12)
                            .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }
                    .buttonStyle(.plain)
                }
            }
        }
    }

    private var bucketTimeStep: some View {
        VStack(alignment: .leading, spacing: 16) {
            VStack(alignment: .leading, spacing: 0) {
                ForEach(BucketTimeframeOption.allCases, id: \.self) { option in
                    bucketTimeOptionRow(option)
                }
            }
            .modifier(PickerStyle(colorScheme: colorScheme))

            if timeframe == .customRange {
                VStack(spacing: 12) {
                    DatePicker("Start", selection: $startDate, displayedComponents: .date)
                        .datePickerStyle(.compact)
                    DatePicker("End", selection: $endDate, in: startDate..., displayedComponents: .date)
                        .datePickerStyle(.compact)
                }
                .padding(14)
                .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
        }
    }

    @ViewBuilder
    private func bucketTimeOptionRow(_ option: BucketTimeframeOption) -> some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack {
                Text(option.title)
                Spacer()

                if option == timeframe {
                    Image(systemName: "checkmark")
                        .font(Font.satoshi(14, weight: .medium))
                        .foregroundColor(Color.PrimaryText)
                }
            }
            .font(Font.satoshi(.title3, weight: .medium))
            .foregroundColor(Color.PrimaryText)

            Text(option.subtitle)
                .font(Font.satoshi(.subheadline, weight: .medium))
                .foregroundColor(Color.SubtitleText)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(8)
        .background {
            if option == timeframe {
                RoundedRectangle(cornerRadius: 6, style: .continuous)
                    .fill(Color.SecondaryBackground)
            }
        }
        .contentShape(Rectangle())
        .onTapGesture {
            withAnimation(.easeIn(duration: 0.15)) {
                timeframe = option
                if option == .thisMonth {
                    let range = currentMonthRange()
                    startDate = range.start
                    endDate = range.end
                } else if option == .thisYear {
                    let range = currentYearRange()
                    startDate = range.start
                    endDate = range.end
                }
            }
        }
    }

    private var bucketStyleStep: some View {
        VStack(alignment: .leading, spacing: 18) {
            HStack(spacing: 12) {
                Button {
                    showEmojiPicker = true
                } label: {
                    Text(displayEmoji)
                        .font(Font.satoshi(24))
                        .frame(width: 58, height: 58)
                    .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                    .overlay(
                        RoundedRectangle(cornerRadius: 14, style: .continuous)
                            .stroke(Color.Outline.opacity(0.8), lineWidth: 1)
                    )
                }
                .buttonStyle(.plain)

                ColorPicker(selection: $selectedColor, supportsOpacity: false) {
                    HStack(spacing: 10) {
                        RoundedRectangle(cornerRadius: 10, style: .continuous)
                            .fill(selectedColor)
                            .frame(width: 32, height: 32)
                            .overlay(
                                RoundedRectangle(cornerRadius: 10, style: .continuous)
                                    .stroke(Color.PrimaryBackground.opacity(0.7), lineWidth: 1.5)
                            )

                        VStack(alignment: .leading, spacing: 2) {
                            Text("Goal color")
                                .font(Font.satoshi(.subheadline, weight: .semibold))
                                .foregroundColor(Color.PrimaryText)

                            Text(normalizedColourHex)
                                .font(Font.satoshi(.caption, weight: .medium))
                                .foregroundColor(Color.SubtitleText)
                        }

                        Spacer()
                    }
                    .padding(12)
                    .frame(maxWidth: .infinity, minHeight: 58)
                    .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                }
            }
            .onChange(of: selectedColor) { newValue in
                colour = normalizedHexString(from: newValue)
            }

            LazyVGrid(columns: Array(repeating: GridItem(.flexible(), spacing: 12), count: 4), spacing: 12) {
                ForEach(swatches, id: \.self) { swatch in
                    Button {
                        colour = swatch
                        selectedColor = Color(hex: swatch)
                    } label: {
                        Circle()
                            .fill(Color(hex: swatch))
                            .frame(width: 36, height: 36)
                            .overlay(
                                Circle()
                                    .stroke(Color.PrimaryBackground, lineWidth: colour == swatch ? 3 : 0)
                            )
                            .overlay(
                                Circle()
                                    .stroke(Color(hex: swatch).opacity(0.4), lineWidth: 1.5)
                            )
                    }
                    .buttonStyle(.plain)
                }
            }

            HStack(spacing: 12) {
                Text(displayEmoji)
                    .font(Font.satoshi(28))
                    .frame(width: 48, height: 48)
                    .background(selectedColor.opacity(0.18), in: RoundedRectangle(cornerRadius: 16, style: .continuous))

                VStack(alignment: .leading, spacing: 4) {
                    Text(name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "Untitled Goal" : name)
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(Color.PrimaryText)

                    Text(timeframe.title)
                        .font(Font.satoshi(.caption, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                }

                Spacer()
            }
            .padding(14)
            .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .stroke(Color.Outline.opacity(0.8), lineWidth: 1)
            )
        }
    }

    private func saveBucket() {
        let target = bucket ?? Bucket(context: moc)
        if target.id == nil {
            target.id = UUID()
            target.dateCreated = .now
        }

        target.name = name.trimmingCharacters(in: .whitespacesAndNewlines)
        target.emoji = displayEmoji
        target.colour = normalizedColourHex
        target.timeframe = timeframe == .none ? nil : timeframe.rawValue

        switch timeframe {
        case .none:
            target.startDate = nil
            target.endDate = nil
        case .thisMonth:
            let range = currentMonthRange()
            target.startDate = range.start
            target.endDate = range.end
        case .thisYear:
            let range = currentYearRange()
            target.startDate = range.start
            target.endDate = range.end
        case .customRange:
            target.startDate = startDate
            target.endDate = endDate
        }

        try? moc.save()
        dismiss()
    }

    private var normalizedColourHex: String {
        let trimmed = colour.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return "4A5240"
        }

        return trimmed.trimmingCharacters(in: CharacterSet.alphanumerics.inverted).uppercased()
    }

    private func normalizedHexString(from color: Color) -> String {
        color.toHex()?
            .trimmingCharacters(in: CharacterSet.alphanumerics.inverted)
            .uppercased() ?? "4A5240"
    }

    private var displayEmoji: String {
        let trimmed = emoji.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "🏷️" : trimmed
    }

    private func currentMonthRange() -> (start: Date, end: Date) {
        let calendar = Calendar.current
        let start = calendar.date(from: calendar.dateComponents([.year, .month], from: Date.now)) ?? Date.now
        let end = calendar.date(byAdding: DateComponents(month: 1, day: -1), to: start) ?? Date.now
        return (start, end)
    }

    private func currentYearRange() -> (start: Date, end: Date) {
        let calendar = Calendar.current
        let start = calendar.date(from: calendar.dateComponents([.year], from: Date.now)) ?? Date.now
        let end = calendar.date(byAdding: DateComponents(year: 1, day: -1), to: start) ?? Date.now
        return (start, end)
    }
}

private struct BucketEmojiPickerSheet: View {
    @Binding var selectedEmoji: String
    let emojis: [String]
    @Environment(\.dismiss) private var dismiss

    private let columns = Array(repeating: GridItem(.flexible(), spacing: 12), count: 6)

    var body: some View {
        NavigationView {
            ScrollView(showsIndicators: false) {
                LazyVGrid(columns: columns, spacing: 12) {
                    ForEach(emojis, id: \.self) { emoji in
                        Button {
                            selectedEmoji = emoji
                            dismiss()
                        } label: {
                            Text(emoji)
                                .font(Font.satoshi(28))
                                .frame(height: 48)
                                .frame(maxWidth: .infinity)
                                .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(20)
            }
            .background(Color.PrimaryBackground.ignoresSafeArea())
            .navigationTitle("Choose Emoji")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Close") { dismiss() }
                }
            }
        }
    }
}

private struct BucketCreationProgressView: View {
    let currentStep: Int
    let totalSteps: Int

    var body: some View {
        CustomCapsuleProgress(
            percent: Double(currentStep) / Double(totalSteps + 1),
            width: 4,
            topStroke: Color.DarkBackground,
            bottomStroke: Color.SecondaryBackground
        )
        .frame(width: 60, height: 22)
    }
}

struct DeleteBucketAlert: View {
    @Environment(\.dismiss) var dismiss
    @Environment(\.managedObjectContext) var moc
    let bucket: Bucket
    var onDelete: () -> Void = {}

    var body: some View {
        ZStack(alignment: .bottom) {
            Color.clear
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .contentShape(Rectangle())
                .onTapGesture { dismiss() }

            VStack(alignment: .leading, spacing: 12) {
                Text("Delete Goal?")
                    .font(Font.satoshi(.title3, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)

                Text("Transactions will be kept, but this bucket will be removed from them.")
                    .font(Font.satoshi(.body, weight: .medium))
                    .foregroundColor(Color.SubtitleText)

                Button {
                    let request: NSFetchRequest<Transaction> = Transaction.fetchRequest()
                    request.predicate = NSPredicate(format: "bucket == %@", bucket)
                    let linkedTransactions = (try? moc.fetch(request)) ?? []
                    linkedTransactions.forEach { $0.bucket = nil }
                    moc.delete(bucket)
                    try? moc.save()
                    onDelete()
                    dismiss()
                } label: {
                    Text("Delete Goal")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(.white)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 13)
                        .background(Color.AlertRed, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
                }

                Button("Cancel") {
                    dismiss()
                }
                .font(Font.satoshi(.body, weight: .semibold))
                .foregroundColor(Color.PrimaryText)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 13)
                .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
            }
            .padding(16)
            .background(Color.PrimaryBackground, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
            .padding(.horizontal, 16)
            .padding(.bottom, 18)
        }
        .background(BackgroundBlurView())
    }
}

struct MainBudgetView: View {
    let budget: MainBudget
    @FetchRequest<Transaction> private var transactions: FetchedResults<Transaction>

    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController

    @State var toEdit: MainBudget?
    @State var toDelete: MainBudget?
    @State var totalSpent: Double = 0

    var soloBudget: Bool

    var budgetAmount: Double {
        if budget.isFault {
            return 0.0
        }

        return budget.amount
    }

    var budgetType: String {
        if budget.isFault {
            return ""
        }

        switch budget.type {
        case 1:
            return String(localized: "today")
        case 2:
            return String(localized: "this week")
        case 3:
            return String(localized: "this month")
        case 4:
            return String(localized: "this year")
        default:
            return "this week"
        }
    }

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    var percentageOfDays: Double {
        let calendar = Calendar.current

        if budget.isFault {
            return 0.0
        }

        if budget.type == 1 {
            let components = calendar.dateComponents([.minute], from: budget.wrappedDate, to: Date.now)
            return Double(components.minute ?? 0) / 1440
        }

        let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
        let numberOfDays = components1.day ?? 0

        let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
        let numberOfDaysPast = components2.day ?? 0

        return Double(numberOfDaysPast) / Double(numberOfDays)
    }

    var targetPercent: Double {
        let calendar = Calendar.current

        if budget.isFault {
            return 0.0
        }

        let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
        let numberOfDays = components1.day ?? 0

        let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
        let numberOfDaysPast = (components2.day ?? 0) + 1

        return Double(numberOfDays - numberOfDaysPast) / Double(numberOfDays)
    }

    var triangleOffset: (x: Double, y: Double) {
        var x = 0.0
        var y = -5.0

        let radius = (width / 2) - 5

        if targetPercent > 0.5 {
            let angle = (CGFloat.pi) * (1 - targetPercent)
            y += (radius - (radius * sin(angle)))
            x += (radius * cos(angle))
        } else if targetPercent < 0.5 {
            let angle = (CGFloat.pi) * targetPercent
            y += (radius - (radius * sin(angle)))
            x -= (radius * cos(angle))
        }

        return (x, y)
    }

    var triangleRotation: Double {
        if targetPercent > 0.5 {
            return ((targetPercent - 0.5) / 0.5) * 90
        } else if targetPercent < 0.5 {
            return -((0.5 - targetPercent) / 0.5) * 90
        } else {
            return 0
        }
    }

    var difference: Double {
        return abs(budgetAmount - totalSpent)
    }

    var isOverBudget: Bool {
        budgetAmount > 0 && totalSpent > budgetAmount
    }

    var overBudgetBoundaryPercent: Double {
        guard totalSpent > 0 else { return 1 }
        return min(max(budgetAmount / totalSpent, 0), 1)
    }

    var percentString: String {
        if !budget.isFault {
            return "\(Int(round(100 - (totalSpent / budgetAmount) * 100)))%"
        } else {
            return ""
        }
    }

    var percentString1: String {
        if !budget.isFault {
            return "\(Int(round((totalSpent / budgetAmount) * 100)))%"
        } else {
            return ""
        }
    }

    var width: CGFloat {
        if soloBudget {
            return UIScreen.main.bounds.width - 90
        } else {
            return 250
        }
    }

    @Environment(\.colorScheme) var colorScheme

    var budgetBoundaryOffset: (x: Double, y: Double) {
        offsetOnCurvedBudgetGraph(for: overBudgetBoundaryPercent)
    }

    var budgetBoundaryRotation: Double {
        rotationOnCurvedBudgetGraph(for: overBudgetBoundaryPercent)
    }

    private func offsetOnCurvedBudgetGraph(for percent: Double) -> (x: Double, y: Double) {
        let clamped = min(max(percent, 0), 1)
        let radius = Double(width / 2) - 5

        if clamped > 0.5 {
            let angle = Double.pi * (1 - clamped)
            return (radius * cos(angle), (radius - (radius * sin(angle))) - 6)
        } else if clamped < 0.5 {
            let angle = Double.pi * clamped
            return (-(radius * cos(angle)), (radius - (radius * sin(angle))) - 6)
        } else {
            return (0, -6)
        }
    }

    private func rotationOnCurvedBudgetGraph(for percent: Double) -> Double {
        if percent > 0.5 {
            return ((percent - 0.5) / 0.5) * 90
        } else if percent < 0.5 {
            return -((0.5 - percent) / 0.5) * 90
        } else {
            return 0
        }
    }

    var thisPeriodLabel: String {
        switch budget.type {
        case 1: return "TODAY"
        case 2: return "THIS WEEK"
        case 3: return "THIS MONTH"
        case 4: return "THIS YEAR"
        default: return "THIS WEEK"
        }
    }

    var spentPeriodLabel: String {
        switch budget.type {
        case 1: return "SPENT TODAY"
        case 2: return "SPENT THIS WEEK"
        case 3: return "SPENT THIS MONTH"
        case 4: return "SPENT THIS YEAR"
        default: return "SPENT THIS WEEK"
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(spacing: 4) {
                Text(thisPeriodLabel)
                    .font(Font.satoshi(.caption, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
                Text("•")
                    .font(Font.satoshi(.caption, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
                Text("\(percentString1) OF BUDGET")
                    .font(Font.satoshi(.caption, weight: .semibold))
                    .foregroundColor(isOverBudget ? Color("BudgetRed") : Color.SubtitleText)
                Spacer()
            }
            .padding(.bottom, 10)

            GeometryReader { geo in
                if isOverBudget {
                    OverBudgetHorizontalBar(
                        fillColor: Color.IncomeGreen,
                        boundaryPercent: overBudgetBoundaryPercent,
                        totalWidth: geo.size.width,
                        height: 8
                    )
                } else {
                    NewBudgetSpentBar(
                        color: Color.IncomeGreen,
                        percent: budgetAmount > 0 ? totalSpent / budgetAmount : 0,
                        totalWidth: geo.size.width,
                        height: 8
                    )
                }
            }
            .frame(height: 8)
            .padding(.bottom, 12)

            Text(spentPeriodLabel)
                .font(Font.satoshi(.caption2, weight: .semibold))
                .foregroundColor(Color.SubtitleText)
                .padding(.bottom, 2)

            HStack(alignment: .lastTextBaseline, spacing: 2) {
                Text(currencySymbol)
                    .font(Font.satoshi(.subheadline, weight: .medium))
                    .foregroundColor(Color.SubtitleText)
                Text("\(totalSpent, specifier: "%.2f")")
                    .font(.system(size: 32, weight: .semibold, design: .default))
                    .monospacedDigit()
                    .foregroundColor(Color.PrimaryText)
            }
            .minimumScaleFactor(0.5)
            .lineLimit(1)

            Rectangle()
                .fill(Color.Outline.opacity(0.5))
                .frame(height: 0.5)
                .padding(.vertical, 12)

            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text("BUDGET")
                        .font(Font.satoshi(.caption2, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                    Text("\(currencySymbol)\(budgetAmount, specifier: "%.2f")")
                        .font(Font.satoshi(.subheadline, weight: .semibold))
                        .foregroundColor(Color.PrimaryText)
                        .minimumScaleFactor(0.6)
                        .lineLimit(1)
                }
                Spacer()
                VStack(alignment: .trailing, spacing: 2) {
                    Text(isOverBudget ? "OVER BUDGET" : "REMAINING")
                        .font(Font.satoshi(.caption2, weight: .semibold))
                        .foregroundColor(isOverBudget ? Color("BudgetRed") : Color.SubtitleText)
                    Text("\(currencySymbol)\(difference, specifier: "%.2f")")
                        .font(Font.satoshi(.subheadline, weight: .semibold))
                        .foregroundColor(isOverBudget ? Color("BudgetRed") : Color.PrimaryText)
                        .minimumScaleFactor(0.6)
                        .lineLimit(1)
                }
            }
        }
        .padding(20)
        .frame(maxWidth: .infinity)
        .background(
            colorScheme == .dark ? Color.Outline.opacity(0.2) : Color.SecondaryBackground,
            in: RoundedRectangle(cornerRadius: 20, style: .continuous)
        )
        .contentShape(RoundedRectangle(cornerRadius: 20))
        .contextMenu {
            Button {
                toEdit = budget
            } label: {
                Label("Edit", systemImage: "pencil")
            }
            Button {
                toDelete = budget
            } label: {
                Label("Delete", systemImage: "xmark.bin")
            }
        }
        .onAppear {
            if budget.isFault {
                return
            }

            var holdingTotal = 0.0
            transactions.forEach { transaction in
                holdingTotal += transaction.wrappedAmount
            }

            totalSpent = holdingTotal
        }
        .sheet(item: $toEdit, onDismiss: {
            toEdit = nil
        }) { budget in
            BrandNewBudgetView(overallBudgetCreated: true, toEditMainBudget: budget)
        }
        .fullScreenCover(item: $toDelete, onDismiss: {
            toDelete = nil
        }) { budget in
            DeleteMainBudgetAlert(toDelete: budget)
        }
    }

    init(budget: MainBudget, solo: Bool) {
        self.budget = budget
        soloBudget = solo

        let startPredicate = NSPredicate(format: "%K >= %@", #keyPath(Transaction.date), budget.wrappedDate as CVarArg)
        let endPredicate = NSPredicate(format: "%K <= %@", #keyPath(Transaction.date), Date.now as CVarArg)
        let incomePredicate = NSPredicate(format: "income = %d", false)

        let andPredicate = NSCompoundPredicate(type: .and, subpredicates: [startPredicate, endPredicate, incomePredicate])

        _transactions = FetchRequest<Transaction>(sortDescriptors: [], predicate: andPredicate)
    }
}

struct SingleBudgetView: View {
    let budget: Budget
    @FetchRequest<Transaction> private var transactions: FetchedResults<Transaction>

    @Binding var toDelete: Budget?
    @Binding var toEdit: Budget?

    @State var totalSpent: Double = 0

    var budgetRows: Bool

    var width: CGFloat {
        ((UIScreen.main.bounds.width - 75) / 2) - 30
    }

    var rowWidth: CGFloat {
        (UIScreen.main.bounds.width - 80) / 2
    }

    var budgetAmount: Double {
        if budget.isFault {
            return 0.0
        }

        return budget.amount
    }

    var budgetType: String {
        if budget.isFault {
            return ""
        }

        switch budget.type {
        case 1:
            return String(localized: "today")
        case 2:
            return String(localized: "this week")
        case 3:
            return String(localized: "this month")
        case 4:
            return String(localized: "this year")
        default:
            return "this week"
        }
    }

    var timeLeft: String {
        let calendar = Calendar.current

        if budget.isFault {
            return ""
        }

        if budgetRows {
            if budget.type == 1 {
                let components = calendar.dateComponents([.hour], from: budget.wrappedDate, to: Date.now)

                return String(localized: "\(24 - components.hour!)h left")
            } else if budget.type == 2 {
                let components = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)

                return String(localized: "\(7 - (components.day ?? 0))d left")
            } else {
                let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
                let numberOfDays = components1.day ?? 0

                let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
                let numberOfDaysPast = components2.day ?? 0

                let daysLeftNumber = Int(numberOfDays - numberOfDaysPast)
                return String(localized: "\(daysLeftNumber)d left"
                )
            }
        } else {
            if budget.type == 1 {
                let components = calendar.dateComponents([.hour], from: budget.wrappedDate, to: Date.now)

                return String(localized: "\(24 - (components.hour ?? 0)) hours left")
            } else if budget.type == 2 {
                let components = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)

                return String(localized: "\(7 - (components.day ?? 0)) days left")
            } else {
                let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
                let numberOfDays = components1.day ?? 0

                let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
                let numberOfDaysPast = components2.day ?? 0

                let daysLeftNumber = Int(numberOfDays - numberOfDaysPast)
                return String(localized: "\(daysLeftNumber) days left")
            }
        }
    }

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    var difference: Double {
        return abs(budgetAmount - totalSpent)
    }

    var percentString: String {
        if !budget.isFault {
            return "\(Int(round(100 - (totalSpent / budgetAmount) * 100)))%"
        } else {
            return ""
        }
    }

    var percentString1: String {
        if !budget.isFault {
            return "\(Int(round((totalSpent / budgetAmount) * 100)))%"
        } else {
            return ""
        }
    }

    var isOverBudget: Bool {
        budgetAmount > 0 && totalSpent > budgetAmount
    }

    var overBudgetBoundaryPercent: Double {
        guard totalSpent > 0 else { return 1 }
        return min(max(budgetAmount / totalSpent, 0), 1)
    }

    var targetPercent: Double {
        let calendar = Calendar.current

        if budget.isFault {
            return 0.0
        }

        let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
        let numberOfDays = components1.day ?? 0

        let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
        let numberOfDaysPast = components2.day ?? 0 + 1

        return Double(numberOfDays - numberOfDaysPast) / Double(numberOfDays)
    }

    var thisPeriodLabel: String {
        switch budget.type {
        case 1: return "TODAY"
        case 2: return "THIS WEEK"
        case 3: return "THIS MONTH"
        case 4: return "THIS YEAR"
        default: return "THIS WEEK"
        }
    }

    var spentPeriodLabel: String {
        switch budget.type {
        case 1: return "SPENT TODAY"
        case 2: return "SPENT THIS WEEK"
        case 3: return "SPENT THIS MONTH"
        case 4: return "SPENT THIS YEAR"
        default: return "SPENT THIS WEEK"
        }
    }

    @Environment(\.colorScheme) var colorScheme

    // swipe to delete
    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController
    @State private var offset: CGFloat = 0
    @State private var deleted: Bool = false

    var deletePopup: Bool {
        return abs(offset) > UIScreen.main.bounds.width * 0.15
    }

    var deleteConfirm: Bool {
        return abs(offset) > UIScreen.main.bounds.width * 0.50
    }

    @GestureState var isDragging = false

    var body: some View {
        Group {
            if budgetRows {
                ZStack(alignment: .trailing) {
                    Image(systemName: "xmark")
                        .font(Font.satoshi(.footnote, weight: .bold))
//                        .font(Font.satoshi(13, weight: .bold))
                        .foregroundColor(deleteConfirm ? Color.AlertRed : Color.SubtitleText)
                        .padding(5)
                        .background(deleteConfirm ? Color.AlertRed.opacity(0.23) : Color.SecondaryBackground, in: Circle())
                        .scaleEffect(deleteConfirm ? 1.1 : 1)
                        .contentShape(Circle())
                        .opacity(deleted ? 0 : 1)
                        .padding(.horizontal, 10)
                        .offset(x: 80)
                        .offset(x: max(-80, offset))

                    HStack {
                        HStack(spacing: 10) {
                            ZStack {
                                RoundedRectangle(cornerRadius: 8, style: .continuous)
                                    .fill(.white)

                                RoundedRectangle(cornerRadius: 8, style: .continuous)
                                    .fill(Color(hex: budget.wrappedColour).opacity(0.3))
                            }
                            .frame(width: 40, height: 40)
                            .overlay {
                                Text(budget.wrappedEmoji)
                                    .font(Font.satoshi(20))
                            }

                            VStack(alignment: .leading, spacing: -0.5) {
                                Text(budget.wrappedName)
                                    .font(Font.satoshi(.body, weight: .semibold))
//                                    .font(Font.satoshi(18, weight: .semibold))
                                    .lineLimit(1)
                                    .foregroundColor(Color.PrimaryText)

                                Text("\(timeLeft) • \(percentString1) spent")
                                    .font(Font.satoshi(.footnote, weight: .medium))
//                                    .font(Font.satoshi(13, weight: .medium))
                                    .lineLimit(1)
                                    .foregroundColor(Color.SubtitleText)
                            }
                        }

                        Spacer()

                        VStack(alignment: .trailing, spacing: -4) {
                            BudgetDollarView(amount: difference, red: totalSpent >= budgetAmount, scale: 1, size: 80)

                            Text("\(budgetAmount >= totalSpent ? "left" : "over") \(budgetType)")
                                .font(Font.satoshi(.caption2, weight: .medium))
                                .foregroundColor(Color.SubtitleText)
                        }
                    }
                    .padding(10)
                    .background(colorScheme == .dark ? Color.Outline.opacity(0.2) : Color.Outline.opacity(0.35), in: RoundedRectangle(cornerRadius: 13, style: .continuous))
                    .contentShape(RoundedRectangle(cornerRadius: 13))
                    .contextMenu {
                        Button {
                            toEdit = budget

                        } label: {
                            Label("Edit", systemImage: "pencil")
                        }
                        Button {
                            toDelete = budget
                        } label: {
                            Label("Delete", systemImage: "xmark.bin")
                        }
                    }
                    .offset(x: offset)
                }
                .padding(.horizontal, 30)
                .onChange(of: deletePopup) { _ in
                    if deletePopup {
                        UIImpactFeedbackGenerator(style: .light).impactOccurred()
                    }
                }
                .onChange(of: deleteConfirm) { _ in
                    if deleteConfirm {
                        UIImpactFeedbackGenerator(style: .heavy).impactOccurred()
                    }
                }
                .animation(.default, value: deletePopup)
                .simultaneousGesture(
                    DragGesture()
                        .updating($isDragging, body: { _, state, _ in
                            state = true
                        })
                        .onChanged { value in
                            if value.translation.width < 0 {
                                offset = value.translation.width
                            }
                        }.onEnded { _ in
                            if deleteConfirm {
                                deleted = true
                                withAnimation(.easeInOut(duration: 0.3)) {
                                    offset -= UIScreen.main.bounds.width
                                }

                                DispatchQueue.main.asyncAfter(deadline: .now() + 0.4) {
                                    withAnimation {
                                        moc.delete(budget)
                                        dataController.save()
                                    }
                                }

                            } else if deletePopup {
                                withAnimation(.easeInOut(duration: 0.3)) {
                                    offset = 0
                                }

                                toDelete = budget
                            } else {
                                withAnimation(.easeInOut(duration: 0.3)) {
                                    offset = 0
                                }
                            }
                        }
                )
                .onChange(of: isDragging) { _ in
                    if !isDragging && !deleted {
                        withAnimation(.easeInOut(duration: 0.3)) {
                            offset = 0
                        }
                    }
                }
            } else {
                VStack(alignment: .leading, spacing: 0) {
                    HStack(spacing: 8) {
                        ZStack {
                            RoundedRectangle(cornerRadius: 8, style: .continuous)
                                .fill(.white)
                            RoundedRectangle(cornerRadius: 8, style: .continuous)
                                .fill(Color(hex: budget.wrappedColour).opacity(0.3))
                        }
                        .frame(width: 32, height: 32)
                        .overlay {
                            Text(budget.wrappedEmoji)
                                .font(Font.satoshi(16))
                        }

                        VStack(alignment: .leading, spacing: 1) {
                            Text(budget.wrappedName.uppercased())
                                .font(Font.satoshi(.caption, weight: .black))
                                .foregroundColor(Color.PrimaryText)
                                .lineLimit(1)

                            Text(timeLeft)
                                .font(Font.satoshi(.caption2, weight: .medium))
                                .foregroundColor(Color.SubtitleText)
                        }
                    }
                    .padding(.bottom, 10)

                    Rectangle()
                        .fill(Color.Outline.opacity(0.5))
                        .frame(height: 0.5)
                        .padding(.bottom, 10)

                    Text(spentPeriodLabel)
                        .font(Font.satoshi(.caption2, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                        .padding(.bottom, 2)

                    HStack(alignment: .lastTextBaseline, spacing: 2) {
                        Text(currencySymbol)
                            .font(Font.satoshi(.caption, weight: .medium))
                            .foregroundColor(Color.SubtitleText)
                        Text("\(totalSpent, specifier: "%.2f")")
                            .font(Font.satoshi(.title3, weight: .semibold))
                            .foregroundColor(isOverBudget ? Color("BudgetRed") : Color.PrimaryText)
                            .minimumScaleFactor(0.5)
                            .lineLimit(1)
                    }
                    .padding(.bottom, 8)

                    GeometryReader { geo in
                        if isOverBudget {
                            OverBudgetHorizontalBar(
                                fillColor: Color.IncomeGreen,
                                boundaryPercent: overBudgetBoundaryPercent,
                                totalWidth: geo.size.width,
                                height: 6
                            )
                        } else {
                            NewBudgetSpentBar(
                                color: Color(hex: budget.wrappedColour),
                                percent: budgetAmount > 0 ? totalSpent / budgetAmount : 0,
                                totalWidth: geo.size.width,
                                height: 6
                            )
                        }
                    }
                    .frame(height: 6)
                    .padding(.bottom, 8)

                    Text("\(percentString1) OF \(thisPeriodLabel) BUDGET")
                        .font(Font.satoshi(.caption2, weight: .semibold))
                        .foregroundColor(isOverBudget ? Color("BudgetRed") : Color.SubtitleText)
                        .lineLimit(1)
                }
                .padding(14)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(
                    colorScheme == .dark ? Color.Outline.opacity(0.2) : Color.Outline.opacity(0.35),
                    in: RoundedRectangle(cornerRadius: 13, style: .continuous)
                )
                .contentShape(RoundedRectangle(cornerRadius: 13))
                .contextMenu {
                    Button {
                        toEdit = budget
                    } label: {
                        Label("Edit", systemImage: "pencil")
                    }
                    Button {
                        toDelete = budget
                    } label: {
                        Label("Delete", systemImage: "xmark.bin")
                    }
                }
            }
        }
        .onAppear {
            if budget.isFault {
                return
            }

            var holdingTotal = 0.0
            transactions.forEach { transaction in
                holdingTotal += transaction.wrappedAmount
            }

            totalSpent = holdingTotal
        }
    }

    init(budget: Budget, toDelete: Binding<Budget?>?, toEdit: Binding<Budget?>?, budgetRows: Bool) {
        self.budget = budget
        self.budgetRows = budgetRows
        _toDelete = toDelete ?? Binding.constant(nil)
        _toEdit = toEdit ?? Binding.constant(nil)

        let date = budget.startDate ?? Date.now

        let startPredicate = NSPredicate(format: "%K >= %@", #keyPath(Transaction.date), date as CVarArg)
        let endPredicate = NSPredicate(format: "%K <= %@", #keyPath(Transaction.date), Date.now as CVarArg)
        let incomePredicate = NSPredicate(format: "income = %d", false)

        let andPredicate: NSCompoundPredicate

        if let category = budget.category {
            let categoryPredicate = NSPredicate(format: "%K == %@", #keyPath(Transaction.category), category)
            andPredicate = NSCompoundPredicate(type: .and, subpredicates: [startPredicate, endPredicate, categoryPredicate, incomePredicate])

            _transactions = FetchRequest<Transaction>(sortDescriptors: [], predicate: andPredicate)
        } else {
            andPredicate = NSCompoundPredicate(type: .and, subpredicates: [startPredicate, endPredicate, incomePredicate])

            _transactions = FetchRequest<Transaction>(sortDescriptors: [], predicate: andPredicate)
        }
    }
}

struct AnimatedBudgetBarGraph: View {
    var color: Color
    var percent: Double

    @AppStorage("animated", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var animated: Bool = true
    @State var showBar: Bool = false

    var body: some View {
        GeometryReader { proxy in
            ZStack(alignment: .bottom) {
                RoundedRectangle(cornerRadius: 4, style: .continuous)
                    .fill(color.opacity(0.3))
                    .frame(height: proxy.size.height)

                if percent > 0 {
                    VStack(spacing: 0) {
                        Spacer(minLength: 0)

                        RoundedRectangle(cornerRadius: 4, style: .continuous)
                            .fill(color.opacity(0.73))
                            .frame(height: showBar ? nil : 0, alignment: .bottom)
                    }
                    .frame(height: proxy.size.height * percent)
                }
            }
        }
        .onAppear {
            DispatchQueue.main.asyncAfter(deadline: .now()) {
                if !animated {
                    showBar = true
                } else {
                    withAnimation(.interactiveSpring(response: 0.6, dampingFraction: 0.8, blendDuration: 0.8)) {
                        showBar = true
                    }
                }
            }
        }
    }
}

struct BudgetDollarView: View {
    @AppStorage("showCents", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var showCents: Bool = true

    var amount: Double
    var red: Bool
    var scale: Int
    var size: CGFloat

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    var dynamicTypeSizes: (symbol: Font.TextStyle, amount: Font.TextStyle) {
        if scale == 1 {
            return (.callout, .title3)
        } else if scale == 2 {
            return (.body, .title2)
        } else {
            return (.title2, .largeTitle)
        }
    }

    var body: some View {
        HStack(alignment: .lastTextBaseline, spacing: 1.3) {
            Group {
                Text(currencySymbol)
                    .font(Font.satoshi(dynamicTypeSizes.symbol, weight: .medium))
                    .foregroundColor(red ? Color("BudgetRed") : Color.SubtitleText) +

                Text("\(amount, specifier: showCents && amount < 100 ? "%.2f" : "%.0f")")
                    .font(Font.satoshi(dynamicTypeSizes.amount, weight: .medium))
                    .foregroundColor(red ? Color("BudgetRed") : Color.PrimaryText)
            }
        }
        .minimumScaleFactor(0.5)
        .lineLimit(1)
    }
}

struct DetailedBudgetDollarView: View {
    var amount: Double
    @AppStorage("showCents", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var showCents: Bool = true

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    var body: some View {
        HStack(alignment: .lastTextBaseline, spacing: 1.3) {
            Group {
                Text(currencySymbol)
                    .font(Font.satoshi(.title2, weight: .medium))
                    .foregroundColor(Color.SubtitleText) +

                Text("\(amount, specifier: showCents && amount < 100 ? "%.2f" : "%.0f")")
                    .font(Font.satoshi(.largeTitle, weight: .medium))
                    .foregroundColor(Color.PrimaryText)
            }
        }
        .minimumScaleFactor(0.5)
        .lineLimit(1)
    }
}

struct DetailedBudgetDifferenceDollarView: View {
    var amount: Double
    var red: Bool

    @AppStorage("showCents", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var showCents: Bool = true

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    var body: some View {
        HStack(alignment: .lastTextBaseline, spacing: 1.3) {
            Group {
                Text(currencySymbol)
                    .font(Font.satoshi(.title2, weight: .medium))
                    .foregroundColor(red ? Color("BudgetRed") : Color.SubtitleText) +

                Text("\(amount, specifier: showCents && amount < 100 ? "%.2f" : "%.0f")")
                    .font(Font.satoshi(.largeTitle, weight: .medium))
                    .foregroundColor(red ? Color("BudgetRed") : Color.PrimaryText)
            }
        }
        .minimumScaleFactor(0.5)
        .lineLimit(1)
    }
}

struct DeleteBudgetAlert: View {
    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController
    @Environment(\.dismiss) var dismiss
    @Environment(\.presentationMode) var presentationMode: Binding<PresentationMode>
    let toDelete: Budget
    @Environment(\.colorScheme) var systemColorScheme

    @AppStorage("bottomEdge", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var bottomEdge: Double = 15

    @State private var offset: CGFloat = 0

    var body: some View {
        ZStack(alignment: .bottom) {
            Color.clear
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .contentShape(Rectangle())
                .onTapGesture {
                    dismiss()
                }

            VStack(alignment: .leading, spacing: 1.5) {
                Text("Delete the '\(toDelete.category?.wrappedName ?? "")' budget?")
                    .font(Font.satoshi(.title2, weight: .medium))
                    .foregroundColor(.PrimaryText)

                Text("This action cannot be undone.")
                    .font(Font.satoshi(.title3, weight: .medium))
                    .foregroundColor(.SubtitleText)
                    .padding(.bottom, 25)

                Button {
                    dismiss()
                    self.presentationMode.wrappedValue.dismiss()

                    withAnimation {
                        moc.delete(toDelete)
                        dataController.save()
                    }

                } label: {
                    DeleteButton(text: "Delete", red: true)
                }
                .padding(.bottom, 8)

                Button {
                    withAnimation(.easeOut(duration: 0.7)) {
                        dismiss()
                    }

                } label: {
                    DeleteButton(text: "Cancel", red: false)
                }
            }
            .padding(13)
            .background(RoundedRectangle(cornerRadius: 13).fill(Color.PrimaryBackground).shadow(color: systemColorScheme == .dark ? Color.clear : Color.gray.opacity(0.25), radius: 6))
            .overlay(RoundedRectangle(cornerRadius: 13).stroke(systemColorScheme == .dark ? Color.gray.opacity(0.1) : Color.clear, lineWidth: 1.3))
            .offset(y: offset)
            .gesture(
                DragGesture()
                    .onChanged { gesture in
                        if gesture.translation.height < 0 {
                            offset = gesture.translation.height / 3
                        } else {
                            offset = gesture.translation.height
                        }
                    }
                    .onEnded { value in
                        if value.translation.height > 20 {
                            dismiss()
                        } else {
                            withAnimation {
                                offset = 0
                            }
                        }
                    }
            )
            .padding(.horizontal, 17)
            .padding(.bottom, bottomEdge == 0 ? 13 : bottomEdge)
        }
        .edgesIgnoringSafeArea(.all)
        .background(BackgroundBlurView())
    }
}

struct DeleteMainBudgetAlert: View {
    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController
    @Environment(\.dismiss) var dismiss
    let toDelete: MainBudget
    @Environment(\.colorScheme) var systemColorScheme

    @AppStorage("bottomEdge", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var bottomEdge: Double = 15

    @State private var offset: CGFloat = 0

    var body: some View {
        ZStack(alignment: .bottom) {
            Color.clear
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .contentShape(Rectangle())
                .onTapGesture {
                    dismiss()
                }

            VStack(alignment: .leading, spacing: 1.5) {
                Text("Delete your overall budget?")
                    .font(Font.satoshi(.title2, weight: .medium))
                    .foregroundColor(.PrimaryText)

                Text("This action cannot be undone.")
                    .font(Font.satoshi(.title3, weight: .medium))
                    .foregroundColor(.SubtitleText)
                    .padding(.bottom, 25)

                Button {
                    dismiss()

                    withAnimation {
                        moc.delete(toDelete)
                        dataController.save()
                    }

                } label: {
                    DeleteButton(text: "Delete", red: true)
                }
                .padding(.bottom, 8)

                Button {
                    withAnimation(.easeOut(duration: 0.7)) {
                        dismiss()
                    }

                } label: {
                    DeleteButton(text: "Cancel", red: false)
                }
            }
            .multilineTextAlignment(.leading)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(13)
            .background(RoundedRectangle(cornerRadius: 13).fill(Color.PrimaryBackground).shadow(color: systemColorScheme == .dark ? Color.clear : Color.gray.opacity(0.25), radius: 6))
            .overlay(RoundedRectangle(cornerRadius: 13).stroke(systemColorScheme == .dark ? Color.gray.opacity(0.1) : Color.clear, lineWidth: 1.3))
            .offset(y: offset)
            .gesture(
                DragGesture()
                    .onChanged { gesture in
                        if gesture.translation.height < 0 {
                            offset = gesture.translation.height / 3
                        } else {
                            offset = gesture.translation.height
                        }
                    }
                    .onEnded { value in
                        if value.translation.height > 20 {
                            dismiss()
                        } else {
                            withAnimation {
                                offset = 0
                            }
                        }
                    }
            )
            .padding(.horizontal, 17)
            .padding(.bottom, bottomEdge == 0 ? 13 : bottomEdge)
        }
        .edgesIgnoringSafeArea(.all)
        .background(BackgroundBlurView())
    }
}

struct DetailedBudgetView: View {
    @Environment(\.presentationMode) var presentationMode: Binding<PresentationMode>
    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController
    let budget: Budget

    @State private var toDelete: Budget?

    @State var newTransaction = false

    @State private var toEdit: Budget?

    var body: some View {
        VStack(spacing: 15) {
            HStack {
                Button {
                    self.presentationMode.wrappedValue.dismiss()
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: "chevron.left")
                            .font(Font.satoshi(.subheadline, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)

                        Text("Back")
                            .font(Font.satoshi(.body, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                    }
                    .padding(.vertical, 6)
                    .padding(.horizontal, 8)
                    .fixedSize(horizontal: false, vertical: /*@START_MENU_TOKEN@*/true/*@END_MENU_TOKEN@*/)
                    .background(Color.SecondaryBackground, in: Capsule())
                }

                Spacer()

                DetailedBudgetViewTopBarButton(imageName: "plus", color: Color("110")) {
                    newTransaction = true
                }

                DetailedBudgetViewTopBarButton(imageName: "pencil", color: Color("6")) {
                    toEdit = budget
                }

                DetailedBudgetViewTopBarButton(imageName: "trash.fill", color: Color.AlertRed) {
                    toDelete = budget
                }
            }
            .padding(.horizontal, 20)

            TimeBudgetView(budget: budget)
        }
        .padding(.vertical, 15)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        .navigationBarBackButtonHidden(true)
        .navigationBarTitle("")
        .navigationBarHidden(true)
        .background(Color.PrimaryBackground)
        .sheet(item: $toEdit, onDismiss: {
            toEdit = nil
        }) { budget in
            BrandNewBudgetView(overallBudgetCreated: false, toEditBudget: budget)
        }
        .fullScreenCover(isPresented: $newTransaction) {
            TransactionView(category: budget.category)
        }
        .fullScreenCover(item: $toDelete, onDismiss: {
            toDelete = nil
        }) { budget in
            DeleteBudgetAlert(toDelete: budget)
        }
    }
}

struct DetailedBucketView: View {
    @Environment(\.presentationMode) var presentationMode: Binding<PresentationMode>
    let bucket: Bucket

    @State private var toEdit: Bucket?
    @State private var toDelete: Bucket?

    var body: some View {
        VStack(spacing: 15) {
            HStack {
                Button {
                    self.presentationMode.wrappedValue.dismiss()
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: "chevron.left")
                            .font(Font.satoshi(.subheadline, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)

                        Text("Back")
                            .font(Font.satoshi(.body, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                    }
                    .padding(.vertical, 6)
                    .padding(.horizontal, 8)
                    .background(Color.SecondaryBackground, in: Capsule())
                }

                Spacer()

                DetailedBudgetViewTopBarButton(imageName: "pencil", color: Color("6")) {
                    toEdit = bucket
                }

                DetailedBudgetViewTopBarButton(imageName: "trash.fill", color: Color.AlertRed) {
                    toDelete = bucket
                }
            }
            .padding(.horizontal, 20)

            BucketTransactionsView(bucket: bucket)
        }
        .padding(.vertical, 15)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        .navigationBarBackButtonHidden(true)
        .navigationBarTitle("")
        .navigationBarHidden(true)
        .background(Color.PrimaryBackground)
        .fullScreenCover(item: $toEdit, onDismiss: {
            toEdit = nil
        }) { bucket in
            BucketEditorSheet(bucket: bucket)
        }
        .fullScreenCover(item: $toDelete, onDismiss: {
            toDelete = nil
        }) { bucket in
            DeleteBucketAlert(bucket: bucket) {
                self.presentationMode.wrappedValue.dismiss()
            }
        }
    }
}

struct BucketTransactionsView: View {
    let bucket: Bucket

    @SectionedFetchRequest<Date?, Transaction> private var transactions: SectionedFetchResults<Date?, Transaction>
    @State private var totalSpent = 0.0

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    private var timeframeSummary: String {
        guard let timeframe = bucket.timeframe else { return "All transactions" }

        switch timeframe {
        case BucketTimeframeOption.thisMonth.rawValue:
            return "This month"
        case BucketTimeframeOption.thisYear.rawValue:
            return "This year"
        case BucketTimeframeOption.customRange.rawValue:
            guard let start = bucket.startDate, let end = bucket.endDate else { return "Custom range" }
            let formatter = DateFormatter()
            formatter.dateFormat = "d MMM"
            return "\(formatter.string(from: start)) - \(formatter.string(from: end))"
        default:
            return "All transactions"
        }
    }

    var body: some View {
        VStack(spacing: 20) {
            VStack(spacing: 10) {
                HStack(spacing: 7.5) {
                    Text(bucket.emoji ?? "🏷️")
                        .font(Font.satoshi(.subheadline))
                    Text(bucket.name ?? "Untitled Goal")
                        .font(Font.satoshi(.title3, weight: .medium))
                        .lineLimit(1)
                }
                .foregroundColor(Color.PrimaryText)

                Text(timeframeSummary)
                    .font(Font.satoshi(.subheadline, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
            }
            .padding(.bottom, 10)

            VStack(spacing: -4) {
                DetailedBudgetDifferenceDollarView(amount: totalSpent, red: false)

                Text("\(transactions.flatMap { $0 }.count) \(transactions.flatMap { $0 }.count == 1 ? "transaction" : "transactions")")
                    .font(Font.satoshi(.subheadline, weight: .medium))
                    .foregroundColor(Color.SubtitleText)
            }
            .padding(.horizontal, 25)

            ScrollView(showsIndicators: false) {
                if transactions.count == 0 {
                    NoResultsView(fullscreen: false)
                } else {
                    ListView(transactions: _transactions)
                }
            }
            .frame(maxHeight: .infinity)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        .onAppear {
            refreshTotal()
        }
    }

    private func refreshTotal() {
        totalSpent = transactions.flatMap { $0 }.reduce(0) { $0 + $1.wrappedAmount }
    }

    init(bucket: Bucket) {
        self.bucket = bucket

        let bucketPredicate = NSPredicate(format: "bucket == %@", bucket)
        let incomePredicate = NSPredicate(format: "income = %d", false)
        let predicate = NSCompoundPredicate(type: .and, subpredicates: [bucketPredicate, incomePredicate])

        _transactions = SectionedFetchRequest<Date?, Transaction>(
            sectionIdentifier: \.day,
            sortDescriptors: [
                SortDescriptor(\.day, order: .reverse),
                SortDescriptor(\.date, order: .reverse)
            ],
            predicate: predicate
        )
    }
}

struct DetailedMainBudgetView: View {
    @Environment(\.presentationMode) var presentationMode: Binding<PresentationMode>
    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController
    let budget: MainBudget

    @State private var toDelete: MainBudget?

    @State private var toEdit: MainBudget?

    var body: some View {
        VStack(spacing: 15) {
            HStack {
                Button {
                    self.presentationMode.wrappedValue.dismiss()
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: "chevron.left")
                            .font(Font.satoshi(.subheadline, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)

                        Text("Back")
                            .font(Font.satoshi(.body, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                    }
                    .padding(.vertical, 6)
                    .padding(.horizontal, 8)
                    .fixedSize(horizontal: false, vertical: /*@START_MENU_TOKEN@*/true/*@END_MENU_TOKEN@*/)
//                    .frame(height: 30, alignment: .center)
                    .background(Color.SecondaryBackground, in: Capsule())
                }

                Spacer()

                DetailedBudgetViewTopBarButton(imageName: "pencil", color: Color("6")) {
                    toEdit = budget
                }

                DetailedBudgetViewTopBarButton(imageName: "trash.fill", color: Color.AlertRed) {
                    toDelete = budget
                }
            }
            .padding(.horizontal, 20)

            TimeMainBudgetView(budget: budget)
        }
        .padding(.vertical, 15)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        .navigationBarBackButtonHidden(true)
        .navigationBarTitle("")
        .navigationBarHidden(true)
        .background(Color.PrimaryBackground)
        .fullScreenCover(item: $toEdit, onDismiss: {
            toEdit = nil
        }) { budget in
            BrandNewBudgetView(overallBudgetCreated: true, toEditMainBudget: budget)
        }
        .fullScreenCover(item: $toDelete, onDismiss: {
            toDelete = nil
        }) { budget in
            DeleteMainBudgetAlert(toDelete: budget)
        }
    }
}

struct TimeBudgetView: View {
    let budget: Budget

    var budgetAmount: Double {
        return budget.amount
    }

    var budgetType: Int {
        return Int(budget.type)
    }

    @State var startDate = Date.now

    var dateString: String {
        let dateFormatter = DateFormatter()

        if budgetType == 1 {
            dateFormatter.dateFormat = "d MMM yyyy"
            return dateFormatter.string(from: startDate)
        } else if budgetType == 2 {
            let endDate = Calendar.current.date(byAdding: .day, value: 6, to: startDate) ?? Date.now
            dateFormatter.dateFormat = "d MMM"
            return dateFormatter.string(from: startDate) + " - " + dateFormatter.string(from: endDate)
        } else if budgetType == 3 {
            var endDate = Calendar.current.date(byAdding: .month, value: 1, to: startDate)!
            endDate = Calendar.current.date(byAdding: .day, value: -1, to: endDate)!
            dateFormatter.dateFormat = "d MMM"
            return dateFormatter.string(from: startDate) + " - " + dateFormatter.string(from: endDate)
        } else if budgetType == 4 {
            var endDate = Calendar.current.date(byAdding: .year, value: 1, to: startDate)!
            endDate = Calendar.current.date(byAdding: .day, value: -1, to: endDate)!
            dateFormatter.dateFormat = "d MMM yy"
            return dateFormatter.string(from: startDate) + " - " + dateFormatter.string(from: endDate)
        } else {
            return ""
        }
    }

    @State var totalSpent = 0.0

    var timeLeft: String {
        let calendar = Calendar.current

        if budgetType == 1 {
            let components = calendar.dateComponents([.hour], from: budget.wrappedDate, to: Date.now)
            return String(localized: "\(24 - (components.hour ?? 0)) hours left")
        } else {
            return String(localized: "\(daysLeftNumber) days left")
        }
    }

    var subtitleText: String {
        if budget.startDate == startDate {
            return timeLeft
        } else {
            return dateString
        }
    }

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    var difference: Double {
        abs(budgetAmount - totalSpent)
    }

    var differenceSubtitle: String {
        if budgetAmount >= totalSpent {
            if startDate == budget.startDate {
                if budgetType == 1 {
                    return String(localized: "left today")
                } else if budgetType == 2 {
                    return String(localized: "left this week")
                } else if budgetType == 3 {
                    return String(localized: "left this month")
                } else if budgetType == 4 {
                    return String(localized: "left this year")
                } else {
                    return ""
                }
            } else {
                if budgetType == 1 {
                    let dateFormatter = DateFormatter()
                    dateFormatter.dateFormat = "d MMM"
                    return String(localized: "left on \(dateFormatter.string(from: startDate))")
                } else if budgetType == 2 {
                    let components = Calendar.current.dateComponents([.day], from: startDate, to: budget.wrappedDate)
                    let weekString = String(localized: "\((components.day ?? 0) / 7) weeks ago")
                    return String(localized: "left \(weekString)")
                } else if budgetType == 3 {
                    let components = Calendar.current.dateComponents([.month], from: startDate, to: budget.wrappedDate)
                    let monthString = String(localized: "\(components.month!) months ago")
                    return String(localized: "left \(monthString)")
                } else if budgetType == 4 {
                    let components = Calendar.current.dateComponents([.year], from: startDate, to: budget.wrappedDate)
                    let yearString = String(localized: "\(components.year!) months ago")
                    return String(localized: "left \(yearString)")
                } else {
                    return ""
                }
            }
        } else {
            if startDate == budget.startDate {
                if budgetType == 1 {
                    return String(localized: "over today")
                } else if budgetType == 2 {
                    return String(localized: "over this week")
                } else if budgetType == 3 {
                    return String(localized: "over this month")
                } else if budgetType == 4 {
                    return String(localized: "over this year")
                } else {
                    return ""
                }
            } else {
                if budgetType == 1 {
                    let dateFormatter = DateFormatter()
                    dateFormatter.dateFormat = "d MMM"
                    return String(localized: "over on \(dateFormatter.string(from: startDate))")
                } else if budgetType == 2 {
                    let components = Calendar.current.dateComponents([.day], from: startDate, to: budget.wrappedDate)
                    let weekString = String(localized: "\((components.day ?? 0) / 7) weeks ago")
                    return String(localized: "over \(weekString)")
                } else if budgetType == 3 {
                    let components = Calendar.current.dateComponents([.month], from: startDate, to: budget.wrappedDate)
                    let monthString = String(localized: "\(components.month!) months ago")
                    return String(localized: "over \(monthString)")
                } else if budgetType == 4 {
                    let components = Calendar.current.dateComponents([.year], from: startDate, to: budget.wrappedDate)
                    let yearString = String(localized: "\(components.year!) months ago")
                    return String(localized: "over \(yearString)")
                } else {
                    return ""
                }
            }
        }
    }

    // for week, month, year only

    var daysLeftNumber: Int {
        let calendar = Calendar.current

        if budgetType == 1 {
            return 0
        } else if budgetType == 2 {
            let components = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
            return 7 - (components.day ?? 0)
        } else if budgetType == 3 {
            let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
            let numberOfDays = components1.day!
            let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
            let numberOfDaysPast = components2.day!

            return Int(numberOfDays - numberOfDaysPast)
        } else if budgetType == 4 {
            let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
            let numberOfDays = components1.day!
            let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
            let numberOfDaysPast = components2.day!
            return Int(numberOfDays - numberOfDaysPast)
        } else {
            return 0
        }
    }

    var leftPerDay: Double {
        if budgetType >= 2 {
            return (budgetAmount - totalSpent) / Double(daysLeftNumber)
        } else {
            return 0
        }
    }

    var showExtraDetails: Bool {
        if budgetType >= 2 {
            return budget.startDate == startDate && totalSpent < budgetAmount && daysLeftNumber != 1
        } else {
            return false
        }
    }

    var isOverBudget: Bool {
        budgetAmount > 0 && totalSpent > budgetAmount
    }

    var overBudgetBoundaryPercent: Double {
        guard totalSpent > 0 else { return 1 }
        return min(max(budgetAmount / totalSpent, 0), 1)
    }

    var body: some View {
        VStack(spacing: 20) {
            // budget name and emoji and time left
            VStack(spacing: 10) {
                HStack(spacing: 7.5) {
                    Text(budget.wrappedEmoji)
                        .font(Font.satoshi(.subheadline))
                    Text(budget.wrappedName)
                        .font(Font.satoshi(.title3, weight: .medium))
                        .lineLimit(1)
                }
                .foregroundColor(Color.PrimaryText)

                Text(subtitleText)
                    .font(Font.satoshi(.subheadline, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
                    .padding(4)
                    .padding(.horizontal, 7)
                    .background(Color.SecondaryBackground, in: Capsule())
            }
            .padding(.bottom, 15)

            // amount left and averages

            if budgetType >= 2 {
                HStack(alignment: .top, spacing: 15) {
                    VStack(alignment: showExtraDetails ? .leading : .center, spacing: -4) {
                        DetailedBudgetDifferenceDollarView(amount: difference, red: totalSpent >= budgetAmount)

                        Text(differenceSubtitle)
                            .font(Font.satoshi(.subheadline, weight: .medium))
                            .foregroundColor(Color.SubtitleText)
                    }
                    .frame(maxWidth: .infinity, alignment: showExtraDetails ? .leading : .center)

                    if showExtraDetails {
                        VStack(alignment: .trailing, spacing: -4) {
                            DetailedBudgetDollarView(amount: leftPerDay)

                            Text("left each day")
                                .font(Font.satoshi(.subheadline, weight: .medium))
//                                .font(Font.satoshi(15, weight: .medium))
                                .foregroundColor(Color.SubtitleText)
                        }
                        .frame(maxWidth: .infinity, alignment: .trailing)
                    }
                }
                .frame(maxWidth: .infinity)
                .padding(.horizontal, 25)
            } else {
                VStack(spacing: -4) {
                    DetailedBudgetDifferenceDollarView(amount: difference, red: totalSpent >= budgetAmount)

                    Text(differenceSubtitle)
                        .font(Font.satoshi(.subheadline, weight: .medium))
//                        .font(Font.satoshi(15, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                }
                .padding(.horizontal, 25)
            }

            // bar graph

            VStack(spacing: 5) {
                GeometryReader { proxy in
                    ZStack(alignment: .leading) {
                        RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                            .fill(Color.SecondaryBackground.opacity(0.5))
                            .frame(width: proxy.size.width)

                        if isOverBudget {
                            if let category = budget.category {
                                OverBudgetHorizontalBar(fillColor: Color.IncomeGreen, boundaryPercent: overBudgetBoundaryPercent, totalWidth: proxy.size.width, height: 28)
                            }
                        } else if totalSpent / budgetAmount < 0.98 {
                            if let category = budget.category {
                                AnimatedHorizontalBarGraphBudget(category: category)
                                    .frame(width: proxy.size.width * (1 - totalSpent / budgetAmount))
                            }
                        }
                    }
                    .frame(maxWidth: .infinity)
                }
                .frame(height: 28)

                HStack {
                    Text("\(currencySymbol)\(totalSpent, specifier: "%.2f")")
                    Spacer()
                    Text("\(currencySymbol)\(budgetAmount, specifier: "%.2f")")
                }
                .frame(maxWidth: .infinity)
                .font(Font.satoshi(.caption))
                .foregroundColor(Color.SubtitleText)
            }
            .padding(.bottom, budgetType >= 2 ? 20 : 0)
            .padding(.horizontal, 25)

            if budgetType == 1 {
                Divider()
                    .overlay(Color.Outline)
                    .padding(.horizontal, 25)
            }

            if let category = budget.category {
                ScrollView(showsIndicators: false) {
                    if budgetType == 1 {
                        FilteredCategoryDayBudgetView(category: category, day: startDate, totalSpent: $totalSpent)
                            .padding(.horizontal, 15)
                    } else {
                        FilteredBudgetView(category: category, startDate: startDate, totalSpent: $totalSpent, type: budgetType - 1)
                            .padding(.horizontal, 15)
                    }
                }
                .frame(maxHeight: .infinity)

                BudgetStepperView(category: category, date: $startDate, startDate: budget.wrappedDate, budgetType: budgetType)
                    .padding(.horizontal, 25)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        .onAppear {
            startDate = budget.wrappedDate
        }
    }
}

struct FilteredCategoryDayBudgetView: View {
    @FetchRequest private var transactions: FetchedResults<Transaction>
    @Binding var totalSpent: Double
    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    var date: Date

    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController
    @AppStorage("showCents", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var showCents: Bool = true
    @AppStorage("swapTimeLabel", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var swapTimeLabel: Bool = false
    @AppStorage("showExpenseOrIncomeSign", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var showExpenseOrIncomeSign: Bool = true

    var body: some View {
        VStack(spacing: 0) {
            if transactions.count == 0 {
                NoResultsView(fullscreen: false)
            } else {
                ForEach(transactions, id: \.id) { transaction in
                    SingleTransactionView(transaction: transaction, showCents: showCents, currencySymbol: currencySymbol, currency: currency, swapTimeLabel: swapTimeLabel, future: false, showExpenseOrIncomeSign: showExpenseOrIncomeSign)
                }
            }
        }
        .onAppear {
            DispatchQueue.main.async {
                var holding = 0.0
                transactions.forEach { transaction in

                    holding += transaction.wrappedAmount
                }

                totalSpent = holding
            }
        }
        .onChange(of: date) { _ in
            DispatchQueue.main.async {
                var holding = 0.0
                transactions.forEach { transaction in

                    holding += transaction.wrappedAmount
                }

                totalSpent = holding
            }
        }
//        .fullScreenCover(item: $toDelete, onDismiss: {
//            toDelete = nil
//        }) { transaction in
//            DeleteTransactionAlert(toDelete: transaction, stopRecurring: false)
//        }
//        .fullScreenCover(item: $toEdit, onDismiss: {
//            toEdit = nil
//        }) { transaction in
//            TransactionView(toEdit: transaction)
//        }
        .frame(maxHeight: .infinity)
    }

    init(category: Category?, day: Date, totalSpent: Binding<Double>) {
        date = day

        let datePredicate = NSPredicate(format: "%K == %@", #keyPath(Transaction.day), day as CVarArg)
        let dateCapPredicate = NSPredicate(format: "%K <= %@", #keyPath(Transaction.date), Date.now as CVarArg)
        let incomePredicate = NSPredicate(format: "income = %d", false)

        if let unwrappedCategory = category {
            let categoryPredicate = NSPredicate(format: "%K == %@", #keyPath(Transaction.category), unwrappedCategory)

            let andPredicate = NSCompoundPredicate(type: .and, subpredicates: [datePredicate, categoryPredicate, incomePredicate, dateCapPredicate])

            _transactions = FetchRequest<Transaction>(sortDescriptors: [
                SortDescriptor(\.date, order: .reverse)
            ], predicate: andPredicate)
        } else {
            let andPredicate = NSCompoundPredicate(type: .and, subpredicates: [datePredicate, incomePredicate, dateCapPredicate])

            _transactions = FetchRequest<Transaction>(sortDescriptors: [
                SortDescriptor(\.date, order: .reverse)
            ], predicate: andPredicate)
        }

        _totalSpent = totalSpent
    }
}

struct FilteredBudgetView: View {
    @SectionedFetchRequest<Date?, Transaction> private var transactions: SectionedFetchResults<Date?, Transaction>

    @Binding var totalSpent: Double
    var date: Date

    var body: some View {
        VStack(spacing: 30) {
            if transactions.count == 0 {
                NoResultsView(fullscreen: false)
            }

            ListView(transactions: _transactions)
        }
        .frame(maxHeight: .infinity)
        .onAppear {
            DispatchQueue.main.async {
                var holding = 0.0
                transactions.forEach { day in

                    day.forEach { transaction in
                        holding += transaction.wrappedAmount
                    }
                }

                totalSpent = holding
            }
        }
        .onChange(of: date) { _ in
            DispatchQueue.main.async {
                var holding = 0.0
                transactions.forEach { day in

                    day.forEach { transaction in
                        holding += transaction.wrappedAmount
                    }
                }
                totalSpent = holding
            }
        }
    }

    init(category: Category? = nil, startDate: Date, totalSpent: Binding<Double>, type: Int) {
        date = startDate

        let startPredicate = NSPredicate(format: "%K >= %@", #keyPath(Transaction.date), startDate as CVarArg)
        let incomePredicate = NSPredicate(format: "income = %d", false)
        let endPredicate: NSPredicate

        var calendar = Calendar(identifier: .gregorian)

        calendar.firstWeekday = UserDefaults(suiteName: "group.com.riskcreatives.duit")!.integer(forKey: "firstWeekday")
        calendar.minimumDaysInFirstWeek = 4

        if type == 1 {
            if calendar.isDate(startDate, equalTo: Date.now, toGranularity: .weekOfYear) {
                endPredicate = NSPredicate(format: "%K < %@", #keyPath(Transaction.date), Date.now as CVarArg)
            } else {
                let next = calendar.date(byAdding: .day, value: 7, to: startDate) ?? Date.now
                endPredicate = NSPredicate(format: "%K < %@", #keyPath(Transaction.date), next as CVarArg)
            }
        } else if type == 2 {
            if calendar.isDate(startDate, equalTo: Date.now, toGranularity: .month) {
                endPredicate = NSPredicate(format: "%K < %@", #keyPath(Transaction.date), Date.now as CVarArg)
            } else {
                let next = calendar.date(byAdding: .month, value: 1, to: startDate) ?? Date.now
                endPredicate = NSPredicate(format: "%K < %@", #keyPath(Transaction.date), next as CVarArg)
            }
        } else {
            if calendar.isDate(startDate, equalTo: Date.now, toGranularity: .year) {
                endPredicate = NSPredicate(format: "%K < %@", #keyPath(Transaction.date), Date.now as CVarArg)
            } else {
                let next = calendar.date(byAdding: .year, value: 1, to: startDate) ?? Date.now
                endPredicate = NSPredicate(format: "%K < %@", #keyPath(Transaction.date), next as CVarArg)
            }
        }

        if let unwrappedCategory = category {
            let categoryPredicate = NSPredicate(format: "%K == %@", #keyPath(Transaction.category), unwrappedCategory)

            let andPredicate = NSCompoundPredicate(type: .and, subpredicates: [startPredicate, endPredicate, categoryPredicate, incomePredicate])

            _transactions = SectionedFetchRequest<Date?, Transaction>(sectionIdentifier: \.day, sortDescriptors: [
                SortDescriptor(\.day, order: .reverse),
                SortDescriptor(\.date, order: .reverse)
            ], predicate: andPredicate)
        } else {
            let andPredicate = NSCompoundPredicate(type: .and, subpredicates: [startPredicate, endPredicate, incomePredicate])

            _transactions = SectionedFetchRequest<Date?, Transaction>(sectionIdentifier: \.day, sortDescriptors: [
                SortDescriptor(\.day, order: .reverse),
                SortDescriptor(\.date, order: .reverse)
            ], predicate: andPredicate)
        }

        _totalSpent = totalSpent
    }
}

struct TimeMainBudgetView: View {
    let budget: MainBudget

    var budgetAmount: Double {
        return budget.amount
    }

    var budgetType: Int {
        return Int(budget.type)
    }

    @State var startDate = Date.now

    var dateString: String {
        let dateFormatter = DateFormatter()

        if budgetType == 1 {
            dateFormatter.dateFormat = "d MMM yyyy"
            return dateFormatter.string(from: startDate)
        } else if budgetType == 2 {
            let endDate = Calendar.current.date(byAdding: .day, value: 6, to: startDate) ?? Date.now
            dateFormatter.dateFormat = "d MMM"
            return dateFormatter.string(from: startDate) + " - " + dateFormatter.string(from: endDate)
        } else if budgetType == 3 {
            var endDate = Calendar.current.date(byAdding: .month, value: 1, to: startDate)!
            endDate = Calendar.current.date(byAdding: .day, value: -1, to: endDate)!
            dateFormatter.dateFormat = "d MMM"
            return dateFormatter.string(from: startDate) + " - " + dateFormatter.string(from: endDate)
        } else if budgetType == 4 {
            var endDate = Calendar.current.date(byAdding: .year, value: 1, to: startDate)!
            endDate = Calendar.current.date(byAdding: .day, value: -1, to: endDate)!
            dateFormatter.dateFormat = "d MMM yy"
            return dateFormatter.string(from: startDate) + " - " + dateFormatter.string(from: endDate)
        } else {
            return ""
        }
    }

    @State var totalSpent = 0.0

    var timeLeft: String {
        let calendar = Calendar.current

        if budgetType == 1 {
            let components = calendar.dateComponents([.hour], from: budget.wrappedDate, to: Date.now)
            return String(localized: "\(24 - (components.hour ?? 0)) hours left")
        } else {
            return String(localized: "\(daysLeftNumber) days left")
        }
    }

    var subtitleText: String {
        if budget.startDate == startDate {
            return timeLeft
        } else {
            return dateString
        }
    }

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    var difference: Double {
        abs(budgetAmount - totalSpent)
    }

    var differenceSubtitle: String {
        if budgetAmount >= totalSpent {
            if startDate == budget.startDate {
                if budgetType == 1 {
                    return String(localized: "left today")
                } else if budgetType == 2 {
                    return String(localized: "left this week")
                } else if budgetType == 3 {
                    return String(localized: "left this month")
                } else if budgetType == 4 {
                    return String(localized: "left this year")
                } else {
                    return ""
                }
            } else {
                if budgetType == 1 {
                    let dateFormatter = DateFormatter()
                    dateFormatter.dateFormat = "d MMM"
                    return String(localized: "left on \(dateFormatter.string(from: startDate))")
                } else if budgetType == 2 {
                    let components = Calendar.current.dateComponents([.day], from: startDate, to: budget.wrappedDate)
                    let weekString = String(localized: "\((components.day ?? 0) / 7) weeks ago")
                    return String(localized: "left \(weekString)")
                } else if budgetType == 3 {
                    let components = Calendar.current.dateComponents([.month], from: startDate, to: budget.wrappedDate)
                    let monthString = String(localized: "\(components.month!) months ago")
                    return String(localized: "left \(monthString)")
                } else if budgetType == 4 {
                    let components = Calendar.current.dateComponents([.year], from: startDate, to: budget.wrappedDate)
                    let yearString = String(localized: "\(components.year!) months ago")
                    return String(localized: "left \(yearString)")
                } else {
                    return ""
                }
            }
        } else {
            if startDate == budget.startDate {
                if budgetType == 1 {
                    return String(localized: "over today")
                } else if budgetType == 2 {
                    return String(localized: "over this week")
                } else if budgetType == 3 {
                    return String(localized: "over this month")
                } else if budgetType == 4 {
                    return String(localized: "over this year")
                } else {
                    return ""
                }
            } else {
                if budgetType == 1 {
                    let dateFormatter = DateFormatter()
                    dateFormatter.dateFormat = "d MMM"
                    return String(localized: "over on \(dateFormatter.string(from: startDate))")
                } else if budgetType == 2 {
                    let components = Calendar.current.dateComponents([.day], from: startDate, to: budget.wrappedDate)
                    let weekString = String(localized: "\((components.day ?? 0) / 7) weeks ago")
                    return String(localized: "over \(weekString)")
                } else if budgetType == 3 {
                    let components = Calendar.current.dateComponents([.month], from: startDate, to: budget.wrappedDate)
                    let monthString = String(localized: "\(components.month!) months ago")
                    return String(localized: "over \(monthString)")
                } else if budgetType == 4 {
                    let components = Calendar.current.dateComponents([.year], from: startDate, to: budget.wrappedDate)
                    let yearString = String(localized: "\(components.year!) months ago")
                    return String(localized: "over \(yearString)")
                } else {
                    return ""
                }
            }
        }
    }

    // for week, month, year only

    var daysLeftNumber: Int {
        let calendar = Calendar.current

        if budgetType == 1 {
            return 0
        } else if budgetType == 2 {
            let components = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
            return 7 - (components.day ?? 0)
        } else if budgetType == 3 {
            let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
            let numberOfDays = components1.day!
            let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
            let numberOfDaysPast = components2.day!

            return Int(numberOfDays - numberOfDaysPast)
        } else if budgetType == 4 {
            let components1 = calendar.dateComponents([.day], from: budget.wrappedDate, to: budget.endDate)
            let numberOfDays = components1.day!
            let components2 = calendar.dateComponents([.day], from: budget.wrappedDate, to: Date.now)
            let numberOfDaysPast = components2.day!
            return Int(numberOfDays - numberOfDaysPast)
        } else {
            return 0
        }
    }

    var leftPerDay: Double {
        if budgetType >= 2 {
            return (budgetAmount - totalSpent) / Double(daysLeftNumber)
        } else {
            return 0
        }
    }

    var showExtraDetails: Bool {
        if budgetType >= 2 {
            return budget.startDate == startDate && totalSpent < budgetAmount && daysLeftNumber != 1
        } else {
            return false
        }
    }

    var isOverBudget: Bool {
        budgetAmount > 0 && totalSpent > budgetAmount
    }

    var overBudgetBoundaryPercent: Double {
        guard totalSpent > 0 else { return 1 }
        return min(max(budgetAmount / totalSpent, 0), 1)
    }

    var body: some View {
        VStack(spacing: 20) {
            // budget name and emoji and time left
            VStack(spacing: 10) {
                Text("Overall Budget")
                    .font(Font.satoshi(.title3, weight: .medium))
                    .lineLimit(1)
                    .foregroundColor(Color.PrimaryText)

                Text(subtitleText)
                    .font(Font.satoshi(.subheadline, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
                    .padding(4)
                    .padding(.horizontal, 7)
                    .background(Color.SecondaryBackground, in: Capsule())
            }
            .padding(.bottom, 15)

            // amount left and averages

            if budgetType >= 2 {
                HStack(alignment: .top, spacing: 15) {
                    VStack(alignment: showExtraDetails ? .leading : .center, spacing: -4) {
                        DetailedBudgetDifferenceDollarView(amount: difference, red: totalSpent >= budgetAmount)

                        Text(differenceSubtitle)
                            .font(Font.satoshi(.subheadline, weight: .medium))
                            .foregroundColor(Color.SubtitleText)
                    }
                    .frame(maxWidth: .infinity, alignment: showExtraDetails ? .leading : .center)

                    if showExtraDetails {
                        VStack(alignment: .trailing, spacing: -4) {
                            DetailedBudgetDollarView(amount: leftPerDay)

                            Text("left each day")
                                .font(Font.satoshi(.subheadline, weight: .medium))
                                .foregroundColor(Color.SubtitleText)
                        }
                        .frame(maxWidth: .infinity, alignment: .trailing)
                    }
                }
                .frame(maxWidth: .infinity)
                .padding(.horizontal, 25)
            } else {
                VStack(spacing: -4) {
                    DetailedBudgetDifferenceDollarView(amount: difference, red: totalSpent >= budgetAmount)

                    Text(differenceSubtitle)
                        .font(Font.satoshi(.subheadline, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                }
                .padding(.horizontal, 25)
            }

            // bar graph

            VStack(spacing: 5) {
                GeometryReader { proxy in
                    ZStack(alignment: .leading) {
                        RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                            .fill(Color.SecondaryBackground)
                            .frame(width: proxy.size.width)

                        if isOverBudget {
                            OverBudgetHorizontalBar(fillColor: Color.IncomeGreen, boundaryPercent: overBudgetBoundaryPercent, totalWidth: proxy.size.width, height: 28)
                        } else if totalSpent / budgetAmount < 0.98 {
                            AnimatedHorizontalBarGraphMainBudget()
                                .frame(width: proxy.size.width * (1 - totalSpent / budgetAmount))
                        }
                    }
                    .frame(maxWidth: .infinity)
                }
                .frame(height: 28)

                HStack {
                    Text("\(currencySymbol)\(totalSpent, specifier: "%.2f")")
                    Spacer()
                    Text("\(currencySymbol)\(budgetAmount, specifier: "%.2f")")
                }
                .frame(maxWidth: .infinity)
                .font(Font.satoshi(.caption))
                .foregroundColor(Color.SubtitleText)
            }
            .padding(.bottom, budgetType >= 2 ? 20 : 0)
            .padding(.horizontal, 25)

            if budgetType == 1 {
                Divider()
                    .overlay(Color.Outline)
                    .padding(.horizontal, 25)
            }

            ScrollView(showsIndicators: false) {
                if budgetType == 1 {
                    FilteredCategoryDayBudgetView(category: nil, day: startDate, totalSpent: $totalSpent)
                        .padding(.horizontal, 15)
                } else {
                    FilteredBudgetView(category: nil, startDate: startDate, totalSpent: $totalSpent, type: budgetType - 1)
                        .padding(.horizontal, 15)
                }
            }
            .frame(maxHeight: .infinity)

            BudgetStepperView(category: nil, date: $startDate, startDate: budget.wrappedDate, budgetType: budgetType)
                .padding(.horizontal, 25)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        .onAppear {
            startDate = budget.wrappedDate
        }
    }
}

struct AnimatedHorizontalBarGraphBudget: View {
    let category: Category

    @State var showBar: Bool = false
    @AppStorage("animated", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var animated: Bool = true

    var body: some View {
        HStack(spacing: 0) {
            RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                .foregroundColor(Color.IncomeGreen)
                .frame(width: showBar ? nil : 0, alignment: .leading)

            Spacer(minLength: 0)
        }
        .onAppear {
            DispatchQueue.main.asyncAfter(deadline: .now()) {
                if !animated {
                    showBar = true
                } else {
                    withAnimation(.easeInOut(duration: 0.7)) {
                        showBar = true
                    }
                }
            }
        }
    }
}

struct AnimatedHorizontalBarGraphMainBudget: View {
    @State var showBar: Bool = false
    @AppStorage("animated", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var animated: Bool = true

    var body: some View {
        HStack(spacing: 0) {
            RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                .fill(Color.IncomeGreen)
                .frame(width: showBar ? nil : 0, alignment: .leading)

            Spacer(minLength: 0)
        }
        .onAppear {
            DispatchQueue.main.asyncAfter(deadline: .now()) {
                if !animated {
                    showBar = true
                } else {
                    withAnimation(.easeInOut(duration: 0.7)) {
                        showBar = true
                    }
                }
            }
        }
    }
}

struct AnimatedCurvedBarGraphBudget: View {
    var transactions: FetchedResults<Transaction>
    var budgetTotal: Double
    let cornerRadius: Double
    let width: Double
    let color: String
    @State var percent: Double = 0

    @AppStorage("animated", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var animated: Bool = true

    var body: some View {
        DonutSemicircle(percent: percent, cornerRadius: cornerRadius, width: width)
            .fill(Color(color))
            .onAppear {
                DispatchQueue.main.asyncAfter(deadline: .now()) {
                    var holdingTotal = 0.0
                    transactions.forEach { transaction in
                        holdingTotal += transaction.wrappedAmount
                    }

                    if !animated {
                        percent = 1 - (holdingTotal / budgetTotal)
                    } else {
                        withAnimation(.easeInOut(duration: 0.7)) {
                            percent = 1 - (holdingTotal / budgetTotal)
                        }
                    }
                }
            }
    }
}

struct AnimatedCurvedBarGraphMainBudget: View {
    var transactions: FetchedResults<Transaction>
    var budgetTotal: Double
    let cornerRadius: Double
    let width: Double

    @State var percent: Double = 0

    @AppStorage("animated", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var animated: Bool = true

    var body: some View {
        DonutSemicircle(percent: percent, cornerRadius: cornerRadius, width: width)
            .fill(Color.DarkBackground)
            .onAppear {
                DispatchQueue.main.asyncAfter(deadline: .now()) {
                    var holdingTotal = 0.0
                    transactions.forEach { transaction in
                        holdingTotal += transaction.wrappedAmount
                    }

                    if !animated {
                        percent = 1 - (holdingTotal / budgetTotal)
                    } else {
                        withAnimation(.easeInOut(duration: 0.7)) {
                            percent = 1 - (holdingTotal / budgetTotal)
                        }
                    }
                }
            }
    }
}

struct OverBudgetCurvedBarGraphMainBudget: View {
    let boundaryPercent: Double
    let cornerRadius: Double
    let width: Double

    @State private var revealPercent: Double = 0

    @AppStorage("animated", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var animated: Bool = true

    var body: some View {
        ZStack {
            DonutSemicircle(percent: revealPercent, cornerRadius: cornerRadius, width: width)
                .fill(Color.IncomeGreen)

            DonutSemicircle(percent: 1, cornerRadius: cornerRadius, width: width)
                .fill(Color.BudgetRed)
                .overlay {
                    DonutSemicircle(percent: revealPercent, cornerRadius: cornerRadius, width: width)
                        .fill(Color.black)
                        .blendMode(.destinationOut)
                }
                .compositingGroup()
        }
        .onAppear {
            let target = min(max(boundaryPercent, 0), 1)
            if !animated {
                revealPercent = target
            } else {
                withAnimation(.easeInOut(duration: 0.7)) {
                    revealPercent = target
                }
            }
        }
    }
}

struct BudgetStepperView: View {
    @FetchRequest private var transactions: FetchedResults<Transaction>

    @Binding var date: Date
    var firstDate: Date {
        if transactions.isEmpty {
            return Date.now
        } else {
            return transactions[0].day ?? Date.now
        }
    }

    var endDate: Date {
        if type == 1 {
            return Calendar.current.date(byAdding: .day, value: 1, to: date)!
        } else if type == 2 {
            return Calendar.current.date(byAdding: .day, value: 6, to: date)!
        } else if type == 3 {
            let holdingDate = Calendar.current.date(byAdding: .month, value: 1, to: date)!
            return Calendar.current.date(byAdding: .day, value: -1, to: holdingDate)!
        } else if type == 4 {
            let holdingDate = Calendar.current.date(byAdding: .year, value: 1, to: date)!
            return Calendar.current.date(byAdding: .day, value: -1, to: holdingDate)!
        }

        return startDate
    }

    let startDate: Date
    let type: Int

    var dateString: String {
        let dateFormatter = DateFormatter()

        if type == 1 {
            dateFormatter.dateFormat = "d MMM yyyy"
            return dateFormatter.string(from: date)
        } else if type == 4 {
            dateFormatter.dateFormat = "d MMM yy"
            return dateFormatter.string(from: date) + " - " + dateFormatter.string(from: endDate)
        } else {
            dateFormatter.dateFormat = "d MMM"
            return dateFormatter.string(from: date) + " - " + dateFormatter.string(from: endDate)
        }
    }

    var body: some View {
        HStack {
            StepperButtonView(left: true, disabled: date <= firstDate) {
                if date > firstDate {
                    if type == 1 {
                        date = Calendar.current.date(byAdding: .day, value: -1, to: date)!
                    } else if type == 2 {
                        date = Calendar.current.date(byAdding: .day, value: -7, to: date)!
                    } else if type == 3 {
                        date = Calendar.current.date(byAdding: .month, value: -1, to: date)!
                    } else if type == 4 {
                        date = Calendar.current.date(byAdding: .year, value: -1, to: date)!
                    }
                }
            }

            Spacer()

            Text(dateString)
                .font(Font.satoshi(.title3, weight: .bold))

            Spacer()

            StepperButtonView(left: false, disabled: date == startDate) {
                if date < startDate {
                    if type == 1 {
                        date = Calendar.current.date(byAdding: .day, value: 1, to: date)!
                    } else if type == 2 {
                        date = Calendar.current.date(byAdding: .day, value: 7, to: date)!
                    } else if type == 3 {
                        date = Calendar.current.date(byAdding: .month, value: 1, to: date)!
                    } else if type == 4 {
                        date = Calendar.current.date(byAdding: .year, value: -1, to: date)!
                    }
                }
            }
        }
        .frame(maxWidth: .infinity)
    }

    init(category: Category?, date: Binding<Date>, startDate: Date, budgetType: Int) {
        if let unwrappedCategory = category {
            _transactions = FetchRequest<Transaction>(sortDescriptors: [
                SortDescriptor(\.day)
            ], predicate: NSPredicate(format: "%K == %@", #keyPath(Transaction.category), unwrappedCategory))
        } else {
            _transactions = FetchRequest<Transaction>(sortDescriptors: [
                SortDescriptor(\.day)
            ])
        }

        _date = date

        self.startDate = startDate
        type = budgetType
    }
}
