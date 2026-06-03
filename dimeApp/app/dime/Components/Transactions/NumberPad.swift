//
//  NumberPad.swift
//  dime
//
//  Created by Yumi on 2023-10-29.
//

import SwiftUI
import CoreHaptics

struct NumericTextTransition: ViewModifier {
    let value: String
    func body(content: Content) -> some View {
        if #available(iOS 16.0, *) {
            content
                .contentTransition(.numericText())
                .animation(.spring(response: 0.25, dampingFraction: 0.82), value: value)
        } else {
            content
        }
    }
}

enum AssignedDecimal {
    case none, first, second
}

private enum CalculatorOperator: String {
    case add = "+"
    case subtract = "-"
    case multiply = "×"
    case divide = "÷"

    func apply(_ lhs: Decimal, _ rhs: Decimal) -> Decimal? {
        switch self {
        case .add:
            return lhs + rhs
        case .subtract:
            return lhs - rhs
        case .multiply:
            return lhs * rhs
        case .divide:
            guard rhs != 0 else { return nil }
            return lhs / rhs
        }
    }
}

private enum CalculatorKey: Hashable {
    case digit(Int)
    case decimal
    case delete
    case empty
    case operation(CalculatorOperator)

    var isOperation: Bool {
        if case .operation = self { return true }
        return false
    }

    var isEmpty: Bool {
        if case .empty = self { return true }
        return false
    }
}

struct NumberPad: View {
    @Binding var price: Double
    @Binding var category: Category?
    @Binding var isEditingDecimal: Bool
    @Binding var decimalValuesAssigned: AssignedDecimal
    var showingNotePicker: Bool = false
    var submitLabel: String?
    var showsSubmitButton: Bool = true
    var showsOperators: Bool = false
    var compactOperators: Bool = false
    @Binding var expressionPreview: String
    var resetID: Int = 0
    var commitID: Int = 0
    var submit: () -> Void

    @AppStorage("numberEntryType", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var numberEntryType: Int = 1
    @AppStorage("haptics", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
        var hapticType: Int = 1

    var numPadNumbers = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    @State private var engine: CHHapticEngine?
    @State private var currentInput = "0"
    @State private var storedValue: Decimal?
    @State private var pendingOperator: CalculatorOperator?

    init(
        price: Binding<Double>,
        category: Binding<Category?>,
        isEditingDecimal: Binding<Bool>,
        decimalValuesAssigned: Binding<AssignedDecimal>,
        showingNotePicker: Bool = false,
        submitLabel: String? = nil,
        showsSubmitButton: Bool = true,
        showsOperators: Bool = false,
        compactOperators: Bool = false,
        expressionPreview: Binding<String> = .constant(""),
        resetID: Int = 0,
        commitID: Int = 0,
        submit: @escaping () -> Void
    ) {
        _price = price
        _category = category
        _isEditingDecimal = isEditingDecimal
        _decimalValuesAssigned = decimalValuesAssigned
        self.showingNotePicker = showingNotePicker
        self.submitLabel = submitLabel
        self.showsSubmitButton = showsSubmitButton
        self.showsOperators = showsOperators
        self.compactOperators = compactOperators
        _expressionPreview = expressionPreview
        self.resetID = resetID
        self.commitID = commitID
        self.submit = submit
    }

    var body: some View {
        if compactOperators {
            keypadContent
        } else {
            keypadContent
                .keyboardAwareHeight(showToolbar: showingNotePicker)
        }
    }

    private var keypadContent: some View {
        GeometryReader { proxy in
            if showsOperators {
                VStack(spacing: proxy.size.height * (compactOperators ? 0.022 : 0.028)) {
                    ForEach(calculatorRows, id: \.self) { row in
                        HStack(spacing: proxy.size.width * (compactOperators ? 0.026 : 0.03)) {
                            ForEach(row, id: \.self) { key in
                                CalculatorKeyButton(key: key, size: proxy.size)
                            }
                        }
                    }
                }
                .frame(width: proxy.size.width, height: proxy.size.height)
            } else {
                VStack(spacing: proxy.size.height * 0.04) {
                    ForEach(numPadNumbers, id: \.self) { array in
                        HStack(spacing: proxy.size.width * 0.05) {
                            ForEach(array, id: \.self) { singleNumber in
                                NumberButton(number: singleNumber, size: proxy.size)
                            }
                        }
                    }
                    HStack(spacing: proxy.size.width * 0.05) {
                        if numberEntryType == 1 {
                            Button {
                                deleteLastDigit()
                            } label: {
                                Image("tag-cross")
                                    .resizable()
                                    .frame(width: 32, height: 32)
                                    .frame(width: proxy.size.width * 0.3, height: proxy.size.height * 0.22)
                                    .background(Color.DarkBackground)
                                    .foregroundColor(Color.LightIcon)
                                    .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
                            }
                            .buttonStyle(NumPadButton())
                        } else {
                            Button {
                                isEditingDecimal = true
                            } label: {
                                Text(".")
                                    .font(Font.satoshi(34, weight: .regular))
                                    .frame(width: proxy.size.width * 0.3, height: proxy.size.height * 0.22)
                                    .background(Color.SecondaryBackground)
                                    .foregroundColor(Color.PrimaryText)
                                    .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
                            }
                            .buttonStyle(NumPadButton())
                        }

                        NumberButton(number: 0, size: proxy.size)

                        if showsSubmitButton {
                            Button {
                                submit()
                            } label: {
                                Group {
                                    if let submitLabel {
                                        Text(submitLabel)
                                            .font(Font.satoshi(.body, weight: .semibold))
                                            .lineLimit(1)
                                            .minimumScaleFactor(0.75)
                                    } else if #available(iOS 17.0, *) {
                                        Image(systemName: "checkmark.square.fill")
                                            .font(Font.satoshi(30, weight: .medium))
                                            .symbolEffect(.bounce.up.byLayer, value: price != 0 && category != nil)
                                    } else {
                                        Image(systemName: "checkmark.square.fill")
                                            .font(Font.satoshi(30, weight: .medium))
                                    }
                                }
                                .frame(width: proxy.size.width * 0.3, height: proxy.size.height * 0.22)
                                .foregroundColor(Color.LightIcon)
                                .background(Color.DarkBackground, in: RoundedRectangle(cornerRadius: 10, style: .continuous))
                            }
                            .buttonStyle(NumPadButton())
                        }
                    }
                }
                .frame(width: proxy.size.width, height: proxy.size.height)
            }
        }
        .padding(.bottom, compactOperators ? 4 : 15)
        .onAppear {
            prepareHaptics()
            syncCurrentInput()
        }
        .onChange(of: resetID) { _ in
            resetCalculatorState()
        }
        .onChange(of: commitID) { _ in
            commitCalculatorResult()
        }
    }

    private var calculatorRows: [[CalculatorKey]] {
        [
            [.digit(7), .digit(8), .digit(9), .operation(.divide)],
            [.digit(4), .digit(5), .digit(6), .operation(.multiply)],
            [.digit(1), .digit(2), .digit(3), .operation(.subtract)],
            [.decimal, .digit(0), .delete, .operation(.add)]
        ]
    }

    @ViewBuilder
    private func CalculatorKeyButton(key: CalculatorKey, size: CGSize) -> some View {
        Button {
            handleCalculatorKey(key)
        } label: {
            Group {
                switch key {
                case let .digit(number):
                    Text("\(number)")
                case .decimal:
                    Text(".")
                case .delete:
                    Image(systemName: "delete.left")
                case .empty:
                    Color.clear
                case let .operation(op):
                    Text(op.rawValue)
                }
            }
            .font(Font.satoshi(key.isOperation ? (compactOperators ? 23 : 26) : (compactOperators ? 28 : 31), weight: .regular))
            .frame(width: key.isOperation ? size.width * 0.17 : size.width * 0.25, height: size.height * (compactOperators ? 0.205 : 0.22))
            .foregroundColor(key.isOperation ? Color.LightIcon : Color.PrimaryText)
            .background(
                key.isEmpty ? Color.clear : (key.isOperation ? Color.DarkBackground.opacity(0.96) : Color.LightIcon.opacity(0.92)),
                in: RoundedRectangle(cornerRadius: 14, style: .continuous)
            )
            .overlay(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .stroke(key.isOperation || key.isEmpty ? Color.clear : Color.Outline.opacity(0.5), lineWidth: 1)
            )
            .shadow(color: key.isOperation || key.isEmpty ? Color.clear : Color.black.opacity(0.045), radius: 7, x: 0, y: 4)
        }
        .buttonStyle(NumPadButton())
        .disabled(key.isEmpty)
    }

    private func handleCalculatorKey(_ key: CalculatorKey) {
        switch key {
        case let .digit(number):
            appendDigit(number)
        case .decimal:
            appendDecimal()
        case .delete:
            deleteCalculatorInput()
        case .empty:
            break
        case let .operation(op):
            applyOperator(op)
        }
    }

    private func appendDigit(_ number: Int) {
        if hapticType == 2 {
            hapticTap()
        }

        if currentInput == "0" {
            currentInput = "\(number)"
        } else {
            currentInput += "\(number)"
        }

        updatePriceFromCurrentInput()
        syncDecimalDisplayState()
        updateExpressionPreview()
    }

    private func appendDecimal() {
        if hapticType == 2 {
            hapticTap()
        }
        guard !currentInput.contains(".") else { return }
        currentInput += "."
        isEditingDecimal = true
        decimalValuesAssigned = .none
        updateExpressionPreview()
    }

    private func deleteCalculatorInput() {
        if hapticType == 2 {
            hapticTap()
        }
        if currentInput.count <= 1 {
            currentInput = "0"
        } else {
            currentInput.removeLast()
            if currentInput == "-" || currentInput.isEmpty {
                currentInput = "0"
            }
        }

        updatePriceFromCurrentInput()
        syncDecimalDisplayState()
        updateExpressionPreview()
    }

    private func applyOperator(_ op: CalculatorOperator) {
        if hapticType == 2 {
            hapticTap()
        }
        let rhs = Decimal(string: sanitizedCurrentInput) ?? 0

        if let lhs = storedValue, let pendingOperator {
            guard let result = pendingOperator.apply(lhs, rhs) else {
                expressionPreview = "Cannot divide by zero"
                return
            }
            storedValue = result
            currentInput = formatDecimal(result)
            price = max(0, NSDecimalNumber(decimal: result).doubleValue)
            syncDecimalDisplayState()
        } else {
            storedValue = rhs
        }

        pendingOperator = op
        currentInput = "0"
        updateExpressionPreview()
    }

    private var sanitizedCurrentInput: String {
        currentInput.hasSuffix(".") ? String(currentInput.dropLast()) : currentInput
    }

    private func updatePriceFromCurrentInput() {
        let value = Decimal(string: sanitizedCurrentInput) ?? 0
        if let storedValue, let pendingOperator, let result = pendingOperator.apply(storedValue, value) {
            price = max(0, NSDecimalNumber(decimal: result).doubleValue)
        } else {
            price = max(0, NSDecimalNumber(decimal: value).doubleValue)
        }
    }

    private func updateExpressionPreview() {
        guard let storedValue, let pendingOperator else {
            expressionPreview = ""
            return
        }

        let lhs = formatDecimal(storedValue)
        let rhs = currentInput == "0" ? "" : " \(currentInput)"
        expressionPreview = "\(lhs) \(pendingOperator.rawValue)\(rhs)"
    }

    private func syncCurrentInput() {
        guard price > 0 else { return }
        currentInput = formatDecimal(Decimal(price))
    }

    private func resetCalculatorState() {
        storedValue = nil
        pendingOperator = nil
        currentInput = "0"
        expressionPreview = ""
    }

    private func commitCalculatorResult() {
        guard let storedValue, let pendingOperator else {
            expressionPreview = ""
            currentInput = price > 0 ? formatDecimal(Decimal(price)) : "0"
            return
        }

        let result: Decimal
        if currentInput == "0" {
            result = storedValue
        } else {
            let rhs = Decimal(string: sanitizedCurrentInput) ?? 0
            guard let calculatedResult = pendingOperator.apply(storedValue, rhs) else {
                expressionPreview = "Cannot divide by zero"
                return
            }
            result = calculatedResult
        }

        let committed = max(0, NSDecimalNumber(decimal: result).doubleValue)
        price = committed
        self.storedValue = nil
        self.pendingOperator = nil
        currentInput = committed > 0 ? formatDecimal(Decimal(committed)) : "0"
        syncDecimalDisplayState()
        expressionPreview = ""
    }

    private func syncDecimalDisplayState() {
        guard currentInput.contains(".") else {
            isEditingDecimal = false
            decimalValuesAssigned = .none
            return
        }

        isEditingDecimal = true
        let decimalCount = currentInput
            .split(separator: ".", omittingEmptySubsequences: false)
            .dropFirst()
            .first?
            .count ?? 0

        switch decimalCount {
        case 0:
            decimalValuesAssigned = .none
        case 1:
            decimalValuesAssigned = .first
        default:
            decimalValuesAssigned = .second
        }
    }

    private func formatDecimal(_ value: Decimal) -> String {
        let number = NSDecimalNumber(decimal: value)
        let formatter = NumberFormatter()
        formatter.minimumFractionDigits = 0
        formatter.maximumFractionDigits = 2
        formatter.numberStyle = .decimal
        return formatter.string(from: number) ?? number.stringValue
    }

    func prepareHaptics() {
        guard CHHapticEngine.capabilitiesForHardware().supportsHaptics else { return }

        do {
            engine = try CHHapticEngine()
            try engine?.start()
        } catch {
            print("There was an error creating the engine: \(error.localizedDescription)")
        }
    }

    func hapticTap() {
        // make sure that the device supports haptics
        guard CHHapticEngine.capabilitiesForHardware().supportsHaptics else { return }

        let hapticDict = [
            CHHapticPattern.Key.pattern: [
                [CHHapticPattern.Key.event: [
                    CHHapticPattern.Key.eventType: CHHapticEvent.EventType.hapticTransient,
                    CHHapticPattern.Key.time: CHHapticTimeImmediate,
                    CHHapticPattern.Key.eventDuration: 1.0]
                ]
            ]
        ]

        do {
            let pattern = try CHHapticPattern(dictionary: hapticDict)

            let player = try engine?.makePlayer(with: pattern)

            engine?.notifyWhenPlayersFinished { _ in
                return .stopEngine
            }

            try engine?.start()
            try player?.start(atTime: 0)
        } catch {
            print("Failed to play pattern: \(error.localizedDescription).")
        }

//        
//        var events = [CHHapticEvent]()
//
//        // create one intense, sharp tap
//        let intensity = CHHapticEventParameter(parameterID: .hapticIntensity, value: 1)
//        let sharpness = CHHapticEventParameter(parameterID: .hapticSharpness, value: 1)
//        let event = CHHapticEvent(eventType: .hapticTransient, parameters: [intensity, sharpness], relativeTime: 0)
//        events.append(event)
//
//        // convert those events into a pattern and play it immediately
//        do {
//            let pattern = try CHHapticPattern(events: events, parameters: [])
//            let player = try engine?.makePlayer(with: pattern)
//            try player?.start(atTime: 0)
//        } catch {
//            print("Failed to play pattern: \(error.localizedDescription).")
//        }
    }

    public func deleteLastDigit() {
        if numberEntryType == 1 {
            price = Double(Int(price * 10)) / 100
        } else if !isEditingDecimal {
            price = Double(Int(price / 10))
        } else {
            switch decimalValuesAssigned {
                case .none:
                    isEditingDecimal = false
                    return
                case .first:
                    price = Double(Int(price))
                    decimalValuesAssigned = .none
                case .second:
                    price = Double(Int(price * 10)) / 10
                    decimalValuesAssigned = .first
            }
        }
    }

    @ViewBuilder
    private func NumberButton(number: Int, size: CGSize) -> some View {
        var disabled: Bool {
            price >= 100000000
        }

        Button {
            if disabled {
                return
            }

            if hapticType == 2 {
                hapticTap()
            }

            if numberEntryType == 1 {
                price *= 10
                price += Double(number) / 100
            } else {
                if isEditingDecimal {
                    switch decimalValuesAssigned {
                        case .none:
                            price += Double(number) / 10
                            decimalValuesAssigned = .first
                        case .first:
                            price += Double(number) / 100
                            decimalValuesAssigned = .second
                        case .second:
                            return
                    }
                } else {
                    price *= 10
                    price += Double(number)
                }
            }
        } label: {
            Text("\(number)")
                .font(Font.satoshi(34, weight: .regular))
                .frame(width: size.width * 0.3, height: size.height * 0.22)
                .background(Color.SecondaryBackground)
                .foregroundColor(Color.PrimaryText)
                .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
                .opacity(disabled ? 0.6 : 1)
        }
        .disabled(disabled)
        .buttonStyle(NumPadButton())
    }
}

func splitDouble(_ num: Double) -> [String] {
    let formatter = NumberFormatter()
    formatter.minimumFractionDigits = 0
    formatter.maximumFractionDigits = 2
    var str = formatter.string(from: NSNumber(value: num))!

    if floor(num) == num {
        str = String(format: "%.0f", num)
    }

    return str.map { String($0) }.filter { $0 != "." }
}

struct NumberPadTextView: View {
    @Binding var price: Double
    @Binding var isEditingDecimal: Bool
    @Binding var decimalValuesAssigned: AssignedDecimal
    var onCurrencyTap: (() -> Void)? = nil
    var currencyOverride: String? = nil
    var income: Bool = false
    var showSign: Bool = false
    var showCurrency: Bool = true

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var globalCurrency: String = Locale.current.currencyCode!
    var effectiveCurrency: String { currencyOverride ?? globalCurrency }
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: effectiveCurrency)!
    }
//
//    var displayNumbers: [String] {
//        return splitDouble(price)
//    }

    @AppStorage("numberEntryType", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var numberEntryType: Int = 1

    public var amount: String {
        let hasFractionalCents = abs(price.truncatingRemainder(dividingBy: 1)) > 0.0001

        if numberEntryType == 1 {
            return String(format: "%.2f", price)
        }
        if isEditingDecimal {
            switch decimalValuesAssigned {
                case .none:
                    return String(format: "%.0f", price) + "."
                case .first:
                    return String(format: "%.1f", price)
                case .second:
                    return String(format: "%.2f", price)
            }
        }
        if hasFractionalCents {
            return String(format: "%.2f", price)
        }
        return String(format: "%.0f", price)
    }

    @Environment(\.dynamicTypeSize) var dynamicTypeSize

    var largerFontSize: CGFloat {
        switch dynamicTypeSize {
        case .xSmall:
            return 46
        case .small:
            return 47
        case .medium:
            return 48
        case .large:
            return 50
        case .xLarge:
            return 56
        case .xxLarge:
            return 58
        case .xxxLarge:
            return 62
        default:
            return 50
        }
    }

    var body: some View {
        HStack(alignment: .lastTextBaseline, spacing: 8) {
            if !showCurrency {
                HStack(alignment: .lastTextBaseline, spacing: 0) {
                    Text(currencySymbol)
                        .font(Font.satoshi(largerFontSize, weight: .regular))
                        .foregroundColor(income ? Color.IncomeGreen.opacity(0.78) : Color.SubtitleText)

                    Text(amount)
                        .font(Font.satoshi(largerFontSize, weight: .regular))
                        .foregroundColor(income ? Color.IncomeGreen : Color.PrimaryText)
                        .modifier(NumericTextTransition(value: amount))
                }
                .frame(maxWidth: .infinity, alignment: .center)
            } else if let onCurrencyTap {
                Button(action: onCurrencyTap) {
                    HStack(alignment: .lastTextBaseline, spacing: 5) {
                        Text(effectiveCurrency)
                            .font(Font.satoshi(.title2, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                        Image(systemName: "chevron.down")
                            .font(.system(size: 10, weight: .bold))
                            .foregroundColor(Color.SubtitleText.opacity(0.65))
                            .offset(y: -4)
                    }
                }
                .buttonStyle(.plain)
                Text("\(showSign ? (income ? "+" : "-") : "")\(amount)")
                    .font(Font.satoshi(largerFontSize, weight: .regular))
                    .foregroundColor(Color.PrimaryText)
                    .modifier(NumericTextTransition(value: amount))
            } else {
                HStack(alignment: .lastTextBaseline, spacing: 0) {
                    Text(effectiveCurrency + " ")
                        .font(Font.satoshi(.title2, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                    Text("\(showSign ? (income ? "+" : "-") : "")\(amount)")
                        .font(Font.satoshi(largerFontSize, weight: .regular))
                        .foregroundColor(Color.PrimaryText)
                        .modifier(NumericTextTransition(value: amount))
                }
            }
        }
        .minimumScaleFactor(0.5)
        .lineLimit(1)
        .padding(.trailing, showCurrency && numberEntryType == 2 ? 40 : 0)
        .frame(maxWidth: .infinity, alignment: showCurrency ? .leading : .center)
        .overlay(alignment: .trailing) {
            if numberEntryType == 2 {
                DeleteButton()
            }
        }
//        .animation(.snappy.delay(0.1), value: displayNumbers)
    }

    @ViewBuilder
   func DeleteButton() -> some View {
       Button {
         deleteLastDigit()
       } label: {
         Image(systemName: "delete.left.fill")
           .font(Font.satoshi(16, weight: .semibold))
           .foregroundColor(Color.SubtitleText)
           .padding(7)
           .background(Color.SecondaryBackground, in: Circle())
           .contentShape(Circle())
       }
       .disabled(price == 0 && !isEditingDecimal)
   }

    public func deleteLastDigit() {
        if numberEntryType == 1 {
            price = Double(Int(price * 10)) / 100
        } else if !isEditingDecimal {
            price = Double(Int(price / 10))
        } else {
            switch decimalValuesAssigned {
                case .none:
                    isEditingDecimal = false
                    return
                case .first:
                    price = Double(Int(price))
                    decimalValuesAssigned = .none
                case .second:
                    price = Double(Int(price * 10)) / 10
                    decimalValuesAssigned = .first
            }
        }
    }
}
