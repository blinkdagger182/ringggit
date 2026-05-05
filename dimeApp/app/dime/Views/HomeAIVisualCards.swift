//
//  HomeAIVisualCards.swift
//  dime
//

import SwiftUI

// MARK: - Dispatcher

struct AIVisualCardView: View {
    let visual: AIVisualCard

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var currency: String = Locale.current.currencyCode ?? "MYR"

    var currencySymbol: String {
        let f = NumberFormatter()
        f.numberStyle = .currency
        f.currencyCode = currency
        return f.currencySymbol
    }

    var body: some View {
        switch visual {
        case .summary(let data):
            AISummaryCardView(data: data, currencySymbol: currencySymbol)
        case .transactionsLogged(let actions):
            AITransactionsLoggedCardView(actions: actions, currencySymbol: currencySymbol)
        case .recurring(let items):
            AIRecurringCardView(items: items, currencySymbol: currencySymbol)
        }
    }
}

// MARK: - Summary Card

struct AISummaryCardView: View {
    let data: AIVisualSummary
    let currencySymbol: String

    @AppStorage("showCents", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var showCents: Bool = true

    private var total: Double { data.totalExpense + data.totalIncome }

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            // Header
            Text("Summary")
                .font(.system(.subheadline, design: .rounded).weight(.semibold))
                .foregroundColor(Color(.secondaryLabel))

            HStack(alignment: .top) {
                // Left: amounts
                VStack(alignment: .leading, spacing: 6) {
                    // Net / primary amount
                    let displayAmount = data.totalExpense > 0 ? data.totalExpense : data.totalIncome
                    Text("\(currencySymbol)\(formatAmt(displayAmount))")
                        .font(.system(size: 26, weight: .bold, design: .rounded))
                        .foregroundColor(Color(.label))
                        .minimumScaleFactor(0.6)
                        .lineLimit(1)

                    // Comparison badge
                    if let pct = data.comparisonPct {
                        HStack(spacing: 4) {
                            Image(systemName: pct <= 0 ? "arrow.down.circle.fill" : "arrow.up.circle.fill")
                                .font(.system(size: 12, weight: .semibold))
                                .foregroundColor(pct <= 0 ? Color("IncomeGreen") : Color("AlertRed"))
                            Text("\(Int(abs(pct)))% \(pct <= 0 ? "lower" : "higher") than last \(periodUnit(data.period))")
                                .font(.system(.caption, design: .rounded).weight(.medium))
                                .foregroundColor(pct <= 0 ? Color("IncomeGreen") : Color("AlertRed"))
                        }
                    }

                    // Income / expense breakdown (when both > 0)
                    if data.totalExpense > 0 && data.totalIncome > 0 {
                        HStack(spacing: 10) {
                            Label("\(currencySymbol)\(formatAmt(data.totalIncome))", systemImage: "arrow.down.circle.fill")
                                .font(.system(.caption, design: .rounded).weight(.medium))
                                .foregroundColor(Color("IncomeGreen"))
                            Label("\(currencySymbol)\(formatAmt(data.totalExpense))", systemImage: "arrow.up.circle.fill")
                                .font(.system(.caption, design: .rounded).weight(.medium))
                                .foregroundColor(Color("AlertRed"))
                        }
                    }
                }

                Spacer(minLength: 12)

                // Right: donut chart
                DonutChart(
                    expense: data.totalExpense,
                    income: data.totalIncome,
                    count: data.count
                )
                .frame(width: 88, height: 88)
            }

            // Period pill
            Text(data.period.capitalized)
                .font(.system(.caption2, design: .rounded).weight(.semibold))
                .foregroundColor(Color(.secondaryLabel))
                .padding(.horizontal, 8)
                .padding(.vertical, 3)
                .background(Color(.tertiarySystemBackground), in: Capsule())
        }
        .padding(16)
        .background(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(Color(.secondarySystemBackground))
                .shadow(color: Color.black.opacity(0.06), radius: 8, y: 2)
        )
    }

    private func formatAmt(_ v: Double) -> String {
        showCents ? String(format: "%.2f", v) : String(format: "%.0f", v)
    }

    private func periodUnit(_ period: String) -> String {
        if period.contains("month") { return "month" }
        if period.contains("week") { return "week" }
        if period.contains("year") { return "year" }
        return "period"
    }
}

// MARK: - Donut Chart

private struct DonutChart: View {
    let expense: Double
    let income: Double
    let count: Int

    private var total: Double { max(expense + income, 0.0001) }
    private var expenseFraction: CGFloat { CGFloat(expense / total) }

    var body: some View {
        ZStack {
            // Track
            Circle()
                .stroke(Color(.systemGray5), lineWidth: 11)

            // Expense arc — gradient teal→lavender
            Circle()
                .trim(from: 0, to: expenseFraction)
                .stroke(
                    AngularGradient(
                        colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8"), Color(hex: "4ECDC4")],
                        center: .center
                    ),
                    style: StrokeStyle(lineWidth: 11, lineCap: .round)
                )
                .rotationEffect(.degrees(-90))

            // Income arc
            if income > 0 {
                Circle()
                    .trim(from: expenseFraction, to: 1)
                    .stroke(Color("IncomeGreen").opacity(0.75),
                            style: StrokeStyle(lineWidth: 11, lineCap: .round))
                    .rotationEffect(.degrees(-90))
            }

            // Center text
            VStack(spacing: 1) {
                Text("\(count)")
                    .font(.system(.body, design: .rounded).weight(.bold))
                    .foregroundColor(Color(.label))
                Text("Transactions")
                    .font(.system(size: 7.5, design: .rounded).weight(.medium))
                    .foregroundColor(Color(.secondaryLabel))
                    .multilineTextAlignment(.center)
            }
        }
    }
}

// MARK: - Transactions Logged Card

struct AITransactionsLoggedCardView: View {
    let actions: [AITransactionAction]
    let currencySymbol: String

    @AppStorage("showCents", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var showCents: Bool = true

    private var expenses: [AITransactionAction] { actions.filter { !$0.income } }
    private var income: [AITransactionAction] { actions.filter { $0.income } }
    private var totalExpense: Double { expenses.reduce(0) { $0 + $1.amount } }
    private var totalIncome: Double { income.reduce(0) { $0 + $1.amount } }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Header
            HStack {
                ZStack {
                    Circle()
                        .fill(
                            LinearGradient(
                                colors: [Color(hex: "4ECDC4").opacity(0.18), Color(hex: "9BAAF8").opacity(0.22)],
                                startPoint: .topLeading, endPoint: .bottomTrailing
                            )
                        )
                        .frame(width: 28, height: 28)
                    Image(systemName: "checkmark")
                        .font(.system(size: 12, weight: .bold))
                        .foregroundStyle(
                            LinearGradient(
                                colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                startPoint: .leading, endPoint: .trailing
                            )
                        )
                }

                Text("\(actions.count) transaction\(actions.count == 1 ? "" : "s") logged")
                    .font(.system(.subheadline, design: .rounded).weight(.semibold))
                    .foregroundColor(Color(.label))

                Spacer()

                // Totals pill
                if totalExpense > 0 || totalIncome > 0 {
                    VStack(alignment: .trailing, spacing: 1) {
                        if totalExpense > 0 {
                            Text("-\(currencySymbol)\(formatAmt(totalExpense))")
                                .font(.system(.caption, design: .rounded).weight(.semibold))
                                .foregroundColor(Color("AlertRed"))
                        }
                        if totalIncome > 0 {
                            Text("+\(currencySymbol)\(formatAmt(totalIncome))")
                                .font(.system(.caption, design: .rounded).weight(.semibold))
                                .foregroundColor(Color("IncomeGreen"))
                        }
                    }
                }
            }
            .padding(14)

            // Divider
            Rectangle()
                .fill(Color(.separator).opacity(0.45))
                .frame(height: 1)
                .padding(.horizontal, 14)

            // Transaction rows
            VStack(spacing: 0) {
                ForEach(Array(actions.enumerated()), id: \.offset) { idx, action in
                    TransactionLogRow(action: action, currencySymbol: currencySymbol, showCents: showCents)

                    if idx < actions.count - 1 {
                        Rectangle()
                            .fill(Color(.separator).opacity(0.3))
                            .frame(height: 1)
                            .padding(.leading, 50)
                    }
                }
            }
            .padding(.bottom, 4)
        }
        .background(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(Color(.secondarySystemBackground))
                .shadow(color: Color.black.opacity(0.06), radius: 8, y: 2)
        )
    }

    private func formatAmt(_ v: Double) -> String {
        showCents ? String(format: "%.2f", v) : String(format: "%.0f", v)
    }
}

private struct TransactionLogRow: View {
    let action: AITransactionAction
    let currencySymbol: String
    let showCents: Bool

    private var timeStr: String {
        let f = DateFormatter()
        f.dateFormat = "h:mm a"
        return f.string(from: action.date)
    }

    private var dateStr: String {
        let cal = Calendar.current
        if cal.isDateInToday(action.date) { return "Today" }
        if cal.isDateInYesterday(action.date) { return "Yesterday" }
        let f = DateFormatter()
        f.dateFormat = "d MMM"
        return f.string(from: action.date)
    }

    private var amtStr: String {
        let v = action.amount
        let formatted = showCents ? String(format: "%.2f", v) : String(format: "%.0f", v)
        return (action.income ? "+" : "-") + currencySymbol + formatted
    }

    var body: some View {
        HStack(spacing: 10) {
            // Category initial circle
            ZStack {
                Circle()
                    .fill(
                        action.income
                            ? Color("IncomeGreen").opacity(0.15)
                            : Color(hex: "4ECDC4").opacity(0.12)
                    )
                    .frame(width: 34, height: 34)
                Text(String(action.note.prefix(1)).uppercased())
                    .font(.system(.subheadline, design: .rounded).weight(.bold))
                    .foregroundColor(action.income ? Color("IncomeGreen") : Color(hex: "4ECDC4"))
            }

            VStack(alignment: .leading, spacing: 2) {
                Text(action.note)
                    .font(.system(.subheadline, design: .rounded).weight(.medium))
                    .foregroundColor(Color(.label))
                    .lineLimit(1)
                HStack(spacing: 4) {
                    Text(action.categoryName)
                        .font(.system(.caption2, design: .rounded).weight(.medium))
                        .foregroundColor(Color(.secondaryLabel))
                    Text("·")
                        .foregroundColor(Color(.tertiaryLabel))
                    Text(dateStr)
                        .font(.system(.caption2, design: .rounded))
                        .foregroundColor(Color(.secondaryLabel))
                    Text(timeStr)
                        .font(.system(.caption2, design: .rounded))
                        .foregroundColor(Color(.tertiaryLabel))
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            Text(amtStr)
                .font(.system(.subheadline, design: .rounded).weight(.semibold))
                .foregroundColor(action.income ? Color("IncomeGreen") : Color(.label))
                .lineLimit(1)
                .layoutPriority(1)
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 10)
    }
}

// MARK: - Recurring Card

struct AIRecurringCardView: View {
    let items: [AIRecurringItem]
    let currencySymbol: String

    @AppStorage("showCents", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var showCents: Bool = true

    private var expenses: [AIRecurringItem] { items.filter { !$0.income } }
    private var incomeItems: [AIRecurringItem] { items.filter { $0.income } }
    private var monthlyExpense: Double { expenses.reduce(0) { $0 + monthlyAmount($1) } }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Header
            HStack {
                ZStack {
                    Circle()
                        .fill(Color(hex: "9BAAF8").opacity(0.15))
                        .frame(width: 28, height: 28)
                    Image(systemName: "arrow.clockwise")
                        .font(.system(size: 12, weight: .bold))
                        .foregroundStyle(
                            LinearGradient(
                                colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                startPoint: .leading, endPoint: .trailing
                            )
                        )
                }

                VStack(alignment: .leading, spacing: 1) {
                    Text("Recurring detected")
                        .font(.system(.subheadline, design: .rounded).weight(.semibold))
                        .foregroundColor(Color(.label))
                    if monthlyExpense > 0 {
                        Text("\(currencySymbol)\(formatAmt(monthlyExpense))/mo in subscriptions")
                            .font(.system(.caption, design: .rounded))
                            .foregroundColor(Color(.secondaryLabel))
                    }
                }

                Spacer()
            }
            .padding(14)

            Rectangle()
                .fill(Color(.separator).opacity(0.45))
                .frame(height: 1)
                .padding(.horizontal, 14)

            VStack(spacing: 0) {
                ForEach(Array(items.enumerated()), id: \.offset) { idx, item in
                    RecurringRow(item: item, currencySymbol: currencySymbol, showCents: showCents)
                    if idx < items.count - 1 {
                        Rectangle()
                            .fill(Color(.separator).opacity(0.3))
                            .frame(height: 1)
                            .padding(.leading, 50)
                    }
                }
            }
            .padding(.bottom, 4)
        }
        .background(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(Color(.secondarySystemBackground))
                .shadow(color: Color.black.opacity(0.06), radius: 8, y: 2)
        )
    }

    private func monthlyAmount(_ item: AIRecurringItem) -> Double {
        switch item.frequency {
        case "daily":   return item.amount * 30
        case "weekly":  return item.amount * 4.33
        case "yearly":  return item.amount / 12
        default:        return item.amount
        }
    }

    private func formatAmt(_ v: Double) -> String {
        showCents ? String(format: "%.2f", v) : String(format: "%.0f", v)
    }
}

private struct RecurringRow: View {
    let item: AIRecurringItem
    let currencySymbol: String
    let showCents: Bool

    private var frequencyLabel: String {
        switch item.frequency {
        case "daily":   return "Daily"
        case "weekly":  return "Weekly"
        case "yearly":  return "Yearly"
        default:        return "Monthly"
        }
    }

    private var amtStr: String {
        let formatted = showCents ? String(format: "%.2f", item.amount) : String(format: "%.0f", item.amount)
        return (item.income ? "+" : "-") + currencySymbol + formatted
    }

    var body: some View {
        HStack(spacing: 10) {
            ZStack {
                Circle()
                    .fill(
                        item.income
                            ? Color("IncomeGreen").opacity(0.12)
                            : Color(hex: "9BAAF8").opacity(0.14)
                    )
                    .frame(width: 34, height: 34)
                Image(systemName: item.income ? "arrow.down" : "arrow.clockwise")
                    .font(.system(size: 13, weight: .semibold))
                    .foregroundColor(item.income ? Color("IncomeGreen") : Color(hex: "9BAAF8"))
            }

            VStack(alignment: .leading, spacing: 2) {
                Text(item.name)
                    .font(.system(.subheadline, design: .rounded).weight(.medium))
                    .foregroundColor(Color(.label))
                    .lineLimit(1)
                Text(frequencyLabel)
                    .font(.system(.caption2, design: .rounded).weight(.medium))
                    .foregroundColor(Color(.secondaryLabel))
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            Text(amtStr)
                .font(.system(.subheadline, design: .rounded).weight(.semibold))
                .foregroundColor(item.income ? Color("IncomeGreen") : Color(.label))
                .lineLimit(1)
                .layoutPriority(1)
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 10)
    }
}
