//
//  TransactionView.swift
//  xpenz
//
//  Created by Rafael Soh on 14/5/22.
//

import Combine
import CoreData
import Foundation
import PDFKit
import Popovers
import SwiftUI
import UniformTypeIdentifiers

private struct TransactionLineDraft: Identifiable {
    let id = UUID()
    var amount: Double
    var category: Category?
    var note: String
    var date: Date
    var income: Bool
}

struct TransactionView: View {
    @FetchRequest(sortDescriptors: [], predicate: NSPredicate(format: "income = %d", false)) private
    var expenseCategories: FetchedResults<Category>
    @FetchRequest(sortDescriptors: [], predicate: NSPredicate(format: "income = %d", true)) private
    var incomeCategories: FetchedResults<Category>
    @FetchRequest(sortDescriptors: [NSSortDescriptor(key: "dateCreated", ascending: true)]) private
    var buckets: FetchedResults<Bucket>

    @Environment(\.managedObjectContext) var moc
    @EnvironmentObject var dataController: DataController
    @Environment(\.dismiss) var dismiss

    @Environment(\.colorScheme) var colorScheme
    var boldText: Bool {
        UIAccessibility.isBoldTextEnabled
    }

    @AppStorage("topEdge", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var topEdge:
    Double = 20

    @State private var note = ""
    @State var category: Category?
    @State var bucket: Bucket?
    @State private var date = Date.now
    @State private var repeatType = 0
    @State private var repeatCoefficient = 1
    @State private var showRecurring = false
    @State var income = false
    @State private var recurringDetection: (type: Int, coefficient: Int)?
    @State private var showCurrencyPicker = false
    @State private var showBucketMenu = false
    @State private var showMoreDetails = false
    @State private var showNoteSheet = false
    @State private var calculatorExpression = ""
    @State private var calculatorResetID = 0
    @State private var calculatorCommitID = 0
    @State private var transactionCurrency: String = Locale.current.currencyCode ?? "MYR"
    @State private var isBatchMode = false
    @State private var lineDrafts: [TransactionLineDraft] = []
    @State private var showExpandedBatchLines = false
    @State private var attemptedBatchAddWithoutCategory = false
    @Namespace private var batchBasketAnimation

    var transactionTypeString: String {
        if income {
            return "Income"
        } else {
            return "Expense"
        }
    }

    private var canSaveTransaction: Bool {
        price > 0 && category != nil
    }

    private var batchTotal: Double {
        lineDrafts.reduce(0) { $0 + $1.amount }
    }

    private var canAddBatchLine: Bool {
        price > 0 && category != nil
    }

    private var canSaveBatch: Bool {
        !lineDrafts.isEmpty && lineDrafts.allSatisfy { $0.category != nil }
    }

    private var shouldShowBatchCategoryError: Bool {
        isBatchMode && attemptedBatchAddWithoutCategory && price > 0 && category == nil
    }

    private var hasAttachmentDetails: Bool {
        attachmentReference != nil || legacyReferenceText != nil
    }

    private var batchAmountFontSize: CGFloat {
        UIScreen.main.bounds.height < 820 ? 30 : 36
    }

    @State var showCategoryPicker = false
    @State var showCategorySheet = false

    @AppStorage("currency", store: UserDefaults(suiteName: "group.com.riskcreatives.duit")) var currency: String = Locale.current.currencyCode!
    var currencySymbol: String {
        return Locale.current.localizedCurrencySymbol(forCurrencyCode: currency)!
    }

    @State var showingDatePicker = false
    @State var showingCategoryView = false

    // toasts
    @State var showToast = false
    @State var toastTitle = ""
    @State var toastImage = ""

    // shaking category error
    @State var categoryButtonTextColor = Color.SubtitleText
    @State var categoryButtonBackgroundColor = Color.clear
    @State var categoryButtonOutlineColor = Color.Outline
    @State var shake: Bool = false

    @ObservedObject var keyboardHeightHelper = KeyboardHeightHelper()

    @AppStorage(
        "firstTransactionViewLaunch", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var firstLaunch: Bool = true

    // edit mode
    let toEdit: Transaction?

    // receipt prefill — applied in onAppear to avoid @State init fragility
    private let receiptPrefill: ScannedReceiptPrefill?
    private let receiptPrefillFingerprint: String?
    private let prefillAmount: Double?
    @State private var appliedReceiptPrefillFingerprint: String?

    // delete mode

    @State var toDelete: Transaction?
    @State var deleteMode = false

    @AppStorage("bottomEdge", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var bottomEdge: Double = 15

    @State private var offset: CGFloat = 0

    let repeatOverlays = ["D", "W", "M"]
    let numberArray = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    var repeatButtonAccessibility: String {
        if repeatType == 1 {
            return "transaction recurs daily, button to edit recurring duration"
        } else if repeatType == 2 {
            return "transaction recurs weekly, button to edit recurring duration"
        } else if repeatType == 3 {
            return "transaction recurs monthly, button to edit recurring duration"
        } else {
            return "button to make transaction recurring"
        }
    }

    var dateString: String {
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "d MMM yyyy"
        return dateFormatter.string(from: date)
    }

    var showTime: Bool {
        let roundedFont = UIFont.rounded(ofSize: fontSize, weight: .semibold)

        let attributes = [NSAttributedString.Key.font: roundedFont]

        let dateSize: CGSize

        if isDateToday(date: date) {
            dateSize = (("Today, " + getDateString(date: date)) as NSString).size(
                withAttributes: attributes)
        } else {
            dateSize = (getDateString(date: date) as NSString).size(withAttributes: attributes)
        }

        let categorySize = (category?.fullName ?? "X Category").size(withAttributes: attributes)
        let timeSize = (getTimeString(date: date) as NSString).size(withAttributes: attributes)

        let screenWidth: CGFloat

        if boldText {
            screenWidth = (UIScreen.main.bounds.width - 200)
        } else {
            screenWidth = (UIScreen.main.bounds.width - 150)
        }

        if (dateSize.width + categorySize.width + timeSize.width) > screenWidth {
            return false
        } else {
            return true
        }
    }

    @AppStorage("colourScheme", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var colourScheme: Int = 0

    @Environment(\.colorScheme) var systemColorScheme

    var darkMode: Bool {
        (colourScheme == 0 && systemColorScheme == .dark) || colourScheme == 2
    }

    var backgroundColor: Color {
        if darkMode {
            return Color("AlwaysDarkBackground")
        } else {
            return Color("AlwaysLightBackground")
        }
    }

    @State var showPicker = false
    @State var animateIcon = false

    @Namespace var animation

    @State var swipingOffset: CGFloat = 0
    @GestureState var isDragging = false

    // show recommendations

    @State var textFieldFocused: Bool = false

    @AppStorage(
        "showTransactionRecommendations", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var showRecommendations: Bool = false

    var suggestedTransactions: [Transaction] {
        return dataController.getSuggestedNotes(searchQuery: note, category: category, income: income)
    }

    var showingNotePicker: Bool {
        return note != "" && toEdit == nil && !suggestedTransactions.isEmpty && textFieldFocused
        && showRecommendations
    }

    @Environment(\.dynamicTypeSize) var dynamicTypeSize

    var fontSize: CGFloat {
        switch dynamicTypeSize {
        case .xSmall:
            return 14
        case .small:
            return 15
        case .medium:
            return 16
        case .large:
            return 17
        case .xLarge:
            return 19
        case .xxLarge:
            return 21
        case .xxxLarge:
            return 23
        default:
            return 23
        }
    }

    var widthOfCategoryButton: CGFloat {
        let fontSize = UIFont.getBodyFontSize(dynamicTypeSize: dynamicTypeSize)

        return "Category".widthOfRoundedString(size: fontSize, weight: .semibold) + 50
    }

    var widthOfBucketButton: CGFloat {
        let fontSize = UIFont.getBodyFontSize(dynamicTypeSize: dynamicTypeSize)
        return "Goal".widthOfRoundedString(size: fontSize, weight: .semibold) + 58
    }

    var capsuleWidth: CGFloat {
        if dynamicTypeSize > .xLarge {
            return 120
        } else {
            return 100
        }
    }

    @State private var price: Double = 0
    @AppStorage("numberEntryType", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var numberEntryType: Int = 1
    @State var isEditingDecimal = false
    @State var decimalValuesAssigned: AssignedDecimal = .none
    @State private var priceString: String = "0"

    @State private var showingAttachmentImporter = false
    @State private var attachmentReference: TransactionAttachmentReference?
    @State private var legacyReferenceText: String?
    @State private var previewAttachmentReference: TransactionAttachmentReference?
    #if DEBUG
    private var debugPrefillInfo: String?
    #endif

    var body: some View {
        GeometryReader { proxy in
            VStack(spacing: isBatchMode ? 10 : 8) {
                transactionHeaderBar
                .padding(.top, isBatchMode ? 2 : 8)

                HStack(alignment: .center) {
                    if isBatchMode { batchAccountChip } else { accountChip }
                    Spacer()
                    if isBatchMode { batchCurrencyChip } else { currencyChip }
                }

                ZStack {
                    // swipe to change between income and expense
                    Color.PrimaryBackground
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .simultaneousGesture(
                            DragGesture()
                                .updating(
                                    $isDragging,
                                    body: { _, state, _ in
                                        state = true
                                    }
                                )
                                .onChanged { gesture in
                                    let swipe = gesture.translation.width

                                    if income {
                                        if swipe < 0 {
                                            swipingOffset = max(-capsuleWidth, -pow(abs(swipe), 0.8)) + capsuleWidth
                                        }

                                    } else {
                                        if swipe > 0 {
                                            swipingOffset = min(capsuleWidth, pow(swipe, 0.8))
                                        }
                                    }
                                }
                                .onEnded { _ in
                                    if income {
                                        if swipingOffset < (capsuleWidth / 2) {
                                            withAnimation {
                                                swipingOffset = 0
                                                income = false
                                            }
                                        } else {
                                            withAnimation {
                                                swipingOffset = capsuleWidth
                                            }
                                        }
                                    } else {
                                        if swipingOffset > (capsuleWidth / 2) {
                                            withAnimation {
                                                swipingOffset = capsuleWidth
                                                income = true
                                            }
                                        } else {
                                            withAnimation {
                                                swipingOffset = 0
                                            }
                                        }
                                    }
                                }
                        )

                    if isBatchMode {
                        batchModeMainContent
                            .frame(maxWidth: .infinity, alignment: .top)
                    } else {
                        VStack(alignment: .leading, spacing: 14) {
                            VStack(alignment: .leading, spacing: 6) {
                                Text("Amount")
                                    .font(Font.satoshi(.headline, weight: .medium))
                                    .foregroundColor(Color.SubtitleText)

                                calculationBubble

                                NumberPadTextView(price: $price, isEditingDecimal: $isEditingDecimal, decimalValuesAssigned: $decimalValuesAssigned, currencyOverride: transactionCurrency, income: income, showSign: false, showCurrency: false)
                                    .scaleEffect(price > 0 ? 1 : 0.98)
                                    .animation(.easeOut(duration: 0.18), value: price)
                            }

                            #if DEBUG
                            if let dbg = debugPrefillInfo {
                                Text(dbg)
                                    .font(.system(.caption2, design: .monospaced))
                                    .foregroundColor(.orange)
                                    .padding(6)
                                    .background(Color.orange.opacity(0.12), in: RoundedRectangle(cornerRadius: 8))
                            }
                            #endif

                            if let detection = recurringDetection, repeatType == 0 {
                                recurringDetectionBanner(detection)
                            }
                        }
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                    }
                }
                .frame(maxWidth: .infinity)
                .frame(maxHeight: isBatchMode ? nil : .infinity)

                if showingNotePicker {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(suggestedTransactions, id: \.self) { transaction in
                                Button {
                                    note = transaction.wrappedNote
                                    withAnimation {
                                        if price == 0 {
                                            price = transaction.wrappedAmount
                                        }
                                        if category == nil {
                                            category = transaction.category
                                        }
                                    }
                                    self.hideKeyboard()
                                } label: {
                                    HStack(spacing: 3) {
                                        Text(transaction.wrappedNote)
                                            .foregroundStyle(Color.PrimaryText)
                                            .lineLimit(1)
                                            .padding(.vertical, 3.5)
                                            .padding(.horizontal, 7)

                                        Text("\(currencySymbol)\(Int(round(transaction.wrappedAmount)))")
                                            .lineLimit(1)
                                            .foregroundStyle(Color(hex: transaction.wrappedColour))
                                            .padding(.vertical, 3.5)
                                            .padding(.horizontal, 5)
                                            .background(
                                                Color(hex: transaction.wrappedColour).opacity(0.23),
                                                in: RoundedRectangle(cornerRadius: 6.5, style: .continuous))
                                    }
                                    .font(Font.satoshi(.body, weight: .semibold))
                                    .padding(5)
                                    .background(
                                        Color.SecondaryBackground,
                                        in: RoundedRectangle(cornerRadius: 11.5, style: .continuous))
                                }
                            }
                        }
                    }
                    .padding(.bottom, 5)
                } else {
                    if !isBatchMode, !showCategoryPicker, hasAttachmentDetails {
                        moreDetailsSection
                            .padding(.bottom, 5)
                    }

                    if !isBatchMode, !showCategoryPicker {
                        preKeypadControlRow
                            .padding(.bottom, 5)
                    }
                }

                // date and category picker

                VStack(spacing: 0) {
                    if showCategoryPicker {
                        NewCategoryPickerView(
                            category: $category, showPicker: $showCategoryPicker,
                            showSheet: $showCategorySheet, income: income
                        )
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .transition(AnyTransition.move(edge: .trailing).combined(with: .opacity))
                    } else {
                        NumberPad(
                            price: $price,
                            category: $category,
                            isEditingDecimal: $isEditingDecimal,
                            decimalValuesAssigned: $decimalValuesAssigned,
                            showingNotePicker: showingNotePicker,
                            submitLabel: nil,
                            showsSubmitButton: false,
                            showsOperators: true,
                            compactOperators: isBatchMode,
                            expressionPreview: $calculatorExpression,
                            resetID: calculatorResetID,
                            commitID: calculatorCommitID
                        ) {
                            attemptSubmit()
                        }
                        .frame(height: isBatchMode ? min(220, max(204, proxy.size.height * 0.25)) : nil)
                        .transition(AnyTransition.move(edge: .leading).combined(with: .opacity))

                        bottomActionBar

                    }
                }

            }
            .padding(.horizontal, 16)
            .padding(.top, isBatchMode ? 8 : 12)
            .padding(.bottom, isBatchMode ? 8 : 12)
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
            .background(Color.PrimaryBackground.ignoresSafeArea())
            .onTapGesture {
                self.hideKeyboard()
            }
            .fullScreenCover(isPresented: $deleteMode) {
                ZStack(alignment: .bottom) {
                    Color.clear
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .contentShape(Rectangle())
                        .onTapGesture {
                            deleteMode = false
                            toDelete = nil
                        }

                    VStack(alignment: .leading, spacing: 1.5) {
                        Text("Delete Expense?")
                            .font(Font.satoshi(20, weight: .medium))
                            .foregroundColor(.PrimaryText)

                        Text("This action cannot be undone.")
                            .font(Font.satoshi(16, weight: .medium))
                            .foregroundColor(.SubtitleText)
                            .padding(.bottom, 15)

                        Button {
                            deleteMode = false

                            withAnimation {
                                if let itemToDelete = toDelete {
                                    moc.delete(itemToDelete)
                                }
                                dataController.save()
                            }

                            dismiss()

                        } label: {
                            Text("Delete")
                                .font(Font.satoshi(20, weight: .semibold))
                                .foregroundColor(.white)
                                .frame(height: 45)
                                .frame(maxWidth: .infinity)
                                .background(
                                    Color.AlertRed, in: RoundedRectangle(cornerRadius: 9, style: .continuous))
                        }
                        .padding(.bottom, 8)

                        Button {
                            withAnimation(.easeOut(duration: 0.7)) {
                                deleteMode = false
                                toDelete = nil
                            }

                        } label: {
                            Text("Cancel")
                                .font(Font.satoshi(20, weight: .semibold))
                                .foregroundColor(Color.PrimaryText.opacity(0.9))
                                .frame(height: 45)
                                .frame(maxWidth: .infinity)
                                .background(
                                    Color.SecondaryBackground,
                                    in: RoundedRectangle(cornerRadius: 9, style: .continuous))
                        }
                    }
                    .padding(13)
                    .background(
                        RoundedRectangle(cornerRadius: 13).fill(Color.PrimaryBackground).shadow(
                            color: systemColorScheme == .dark ? Color.clear : Color.gray.opacity(0.25), radius: 6)
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: 13).stroke(
                            systemColorScheme == .dark ? Color.gray.opacity(0.1) : Color.clear, lineWidth: 1.3)
                    )
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
                                    deleteMode = false
                                    toDelete = nil
                                } else {
                                    withAnimation {
                                        offset = 0
                                    }
                                }
                            }
                    )
                    .padding(.horizontal, 17)
                    .padding(.bottom, bottomEdge == 0 ? 12 : bottomEdge - 3)
                }
                .edgesIgnoringSafeArea(.all)
                .background(BackgroundBlurView())
            }
            .overlay {
                ZStack(alignment: .bottom) {
                    GeometryReader { _ in
                        EmptyView()
                    }
                    .background(Color.black)
                    .opacity(showingDatePicker ? 0.3 : 0)
                    .onTapGesture {
                        showingDatePicker = false
                    }

                    DatePicker("Date", selection: $date)
                        .datePickerStyle(.graphical)
                        .padding(.horizontal, 15)
                        .padding(.vertical, 8)
                        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 13))
                        .padding(17)
                        .onChange(of: dateString) { _ in

                            DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                                showingDatePicker = false
                            }

                            if date > Date.now {
                                animateIcon = true
                            } else {
                                animateIcon = false
                            }
                        }
                        .padding(.bottom, 10)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)
                .opacity(showingDatePicker ? 1 : 0)
                .allowsHitTesting(showingDatePicker)
                .animation(.easeOut(duration: 0.25), value: showingDatePicker)
            }
        }
        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
        .animation(.easeOut(duration: 0.2), value: showToast)
        .ignoresSafeArea(.keyboard, edges: .all)
        .frame(maxHeight: .infinity)
        .background(Color.PrimaryBackground.ignoresSafeArea())
        .onChange(of: showToast) { newValue in
            if newValue {
                DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                    showToast = false
                }
            }
        }
        .onChange(of: note) { newNote in
            detectRecurring(for: newNote)
        }
        .onChange(of: income) { _ in
            UIImpactFeedbackGenerator(style: .light).impactOccurred()
            category = nil
            attemptedBatchAddWithoutCategory = false
            detectRecurring(for: note)
        }
        .onChange(of: isDragging) { _ in
            if !isDragging {
                if income {
                    withAnimation(.easeInOut(duration: 0.3)) {
                        swipingOffset = capsuleWidth
                    }
                } else {
                    withAnimation(.easeInOut(duration: 0.3)) {
                        swipingOffset = 0
                    }
                }
            }
        }
        .onAppear {
            DispatchQueue.main.async {
                if let transaction = toEdit {
                    repeatType = Int(transaction.recurringType)
                    repeatCoefficient = Int(transaction.recurringCoefficient)
                    price = transaction.wrappedAmount

                    if transaction.wrappedAmount.truncatingRemainder(dividingBy: 1) > 0 && numberEntryType == 2 {
                        isEditingDecimal = true
                        decimalValuesAssigned = .second
                    }
                    attachmentReference = TransactionAttachmentReference.decode(from: transaction.reference)
                    legacyReferenceText = TransactionAttachmentReference.legacyText(from: transaction.reference)

                    if transaction.wrappedDate > Date.now {
                        animateIcon = true
                    }
                } else if receiptPrefill != nil {
                    applyReceiptPrefillIfNeeded()
                    if isBatchMode {
                        assignSuggestedCategoriesToBatchLines()
                        if lineDrafts.contains(where: { $0.category == nil }) {
                            collapseBatchToSingleTransaction()
                        }
                    }
                } else if isBatchMode {
                    assignSuggestedCategoriesToBatchLines()
                    if lineDrafts.contains(where: { $0.category == nil }) {
                        collapseBatchToSingleTransaction()
                    }
                }
            }
        }
        .onChange(of: receiptPrefillFingerprint) { _ in
            applyReceiptPrefillIfNeeded()
        }
        .sheet(isPresented: $showCategorySheet) {
            CategoryView(mode: .transaction, income: income)
        }
        .sheet(isPresented: $showPicker) {
            if #available(iOS 16.0, *) {
                CustomRecurringView(
                    repeatType: $repeatType, repeatCoefficient: $repeatCoefficient, showPicker: $showPicker
                )
                .presentationDetents([.height(230)])
            } else {
                CustomRecurringView(
                    repeatType: $repeatType, repeatCoefficient: $repeatCoefficient, showPicker: $showPicker)
            }
        }
        .fileImporter(isPresented: $showingAttachmentImporter, allowedContentTypes: [.pdf, .image]) { result in
            switch result {
            case let .success(file):
                let accessedSecurityScope = file.startAccessingSecurityScopedResource()
                defer {
                    if accessedSecurityScope {
                        file.stopAccessingSecurityScopedResource()
                    }
                }

                if let savedReference = TransactionAttachmentStore.saveImportedFile(from: file) {
                    attachmentReference = savedReference
                    legacyReferenceText = nil
                } else {
                    toastImage = "xmark"
                    toastTitle = "Couldn't attach file"
                    showToast = true
                }
            case let .failure(error):
                toastImage = "xmark"
                toastTitle = error.localizedDescription
                showToast = true
            }
        }
        .onChange(of: dynamicTypeSize) { _ in
            if income {
                swipingOffset = capsuleWidth
            }
        }
        .sheet(item: $previewAttachmentReference) { reference in
            TransactionAttachmentPreviewSheet(reference: reference)
        }
        .sheet(isPresented: $showCurrencyPicker) {
            CurrencyPickerSheet(currency: $transactionCurrency)
        }
        .sheet(isPresented: $showNoteSheet) {
            TransactionNoteSheet(note: $note)
                .compactNoteSheet()
        }
        .overlay {
            if showExpandedBatchLines {
                BatchTransactionsExpandedView(
                    lineDrafts: $lineDrafts,
                    transactionCurrency: transactionCurrency,
                    batchTotal: batchTotal,
                    namespace: batchBasketAnimation
                ) {
                    withAnimation(.spring(response: 0.42, dampingFraction: 0.88)) {
                        showExpandedBatchLines = false
                    }
                }
                .transition(.identity)
                .zIndex(20)
            }
        }
    }

    func isDateToday(date: Date) -> Bool {
        let calendar = Calendar.current

        return calendar.isDateInToday(date)
    }

    func getDateString(date: Date) -> String {
        let formatter = DateFormatter()

        if isDateToday(date: date) {
            formatter.dateFormat = "d MMM"

            return formatter.string(from: date)
        } else {
            formatter.dateFormat = "E, d MMM"

            return formatter.string(from: date)
        }
    }

    func getTimeString(date: Date) -> String {
        let formatter = DateFormatter()

        formatter.dateFormat = "HH:mm"

        return formatter.string(from: date)
    }

    func toggleFieldColors() {
        if categoryButtonTextColor == Color.AlertRed
            || categoryButtonBackgroundColor == Color.AlertRed.opacity(0.23) {
            withAnimation(.linear) {
                categoryButtonTextColor = Color.SubtitleText
                categoryButtonBackgroundColor = Color.clear
                categoryButtonOutlineColor = Color.Outline
            }
        } else {
            withAnimation(.easeOut(duration: 1.0)) {
                categoryButtonTextColor = Color.AlertRed
                categoryButtonBackgroundColor = Color.AlertRed.opacity(0.23)
                categoryButtonOutlineColor = Color.AlertRed
            }
            withAnimation(.easeInOut(duration: 0.1).repeatCount(5)) {
                shake = true
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                withAnimation(.easeInOut(duration: 0.1)) {
                    shake = false
                }
                withAnimation(.easeOut(duration: 0.6)) {
                    categoryButtonTextColor = Color.SubtitleText
                    categoryButtonBackgroundColor = Color.clear
                    categoryButtonOutlineColor = Color.Outline
                }
            }
        }

    }

    private func attemptSubmit() {
        calculatorCommitID += 1

        DispatchQueue.main.async {
            submit()
        }
    }

    private func attemptAddBatchLine() {
        calculatorCommitID += 1

        DispatchQueue.main.async {
            addBatchLine()
        }
    }

    private func applyReceiptPrefillIfNeeded() {
        guard toEdit == nil,
              let prefill = receiptPrefill,
              let fingerprint = receiptPrefillFingerprint,
              appliedReceiptPrefillFingerprint != fingerprint else {
            return
        }

        appliedReceiptPrefillFingerprint = fingerprint
        income = false
        swipingOffset = 0

        if let amount = prefill.amount, amount > 0 {
            price = amount
            calculatorExpression = ""
            if amount.truncatingRemainder(dividingBy: 1) > 0 {
                isEditingDecimal = true
                decimalValuesAssigned = .second
            } else {
                isEditingDecimal = false
                decimalValuesAssigned = .none
            }
        }

        if prefill.lineItems.count >= 2, lineDrafts.isEmpty {
            let prefillDate = prefill.date ?? Date.now
            isBatchMode = true
            lineDrafts = prefill.lineItems.map {
                TransactionLineDraft(
                    amount: $0.amount,
                    category: nil,
                    note: $0.note,
                    date: prefillDate,
                    income: false
                )
            }
            assignSuggestedCategoriesToBatchLines()
        }

        let trimmedNote = prefill.note.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmedNote.isEmpty {
            note = prefill.note
            detectRecurring(for: prefill.note)
        }

        if let prefillDate = prefill.date {
            date = prefillDate
        }

        if let reference = prefill.attachmentReference {
            attachmentReference = reference
            legacyReferenceText = nil
        }
    }

    private func addBatchLine() {
        let generator = UINotificationFeedbackGenerator()

        guard price > 0 else {
            toastImage = "centsign.circle"
            toastTitle = "Missing Amount"
            showToast = true
            generator.notificationOccurred(.error)
            return
        }

        guard let category else {
            toastImage = "tray"
            toastTitle = "Missing Category"
            showToast = true
            attemptedBatchAddWithoutCategory = true
            toggleFieldColors()
            generator.notificationOccurred(.error)
            return
        }

        withAnimation(.spring(response: 0.32, dampingFraction: 0.86)) {
            lineDrafts.append(
                TransactionLineDraft(
                    amount: price,
                    category: category,
                    note: note.trimmingCharacters(in: .whitespacesAndNewlines),
                    date: date,
                    income: income
                )
            )
        }

        attemptedBatchAddWithoutCategory = false
        generator.notificationOccurred(.success)
        withAnimation(.easeOut(duration: 0.2)) {
            resetCurrentLine(keepCategory: false)
        }
    }

    private func saveBatchTransactions() {
        assignSuggestedCategoriesToBatchLines()

        guard canSaveBatch else {
            toastImage = "tray"
            toastTitle = "Missing Category"
            showToast = true
            toggleFieldColors()
            UINotificationFeedbackGenerator().notificationOccurred(.error)
            return
        }

        let generator = UINotificationFeedbackGenerator()
        let calendar = Calendar(identifier: .gregorian)

        for draft in lineDrafts {
            let transaction = Transaction(context: moc)
            transaction.id = UUID()
            transaction.amount = draft.amount
            transaction.category = draft.category
            transaction.note = draft.note.isEmpty ? draft.category?.wrappedName ?? "" : draft.note
            transaction.date = draft.date
            transaction.income = draft.income
            transaction.bucket = bucket
            transaction.currency = transactionCurrency
            transaction.reference = attachmentReference?.serializedReference ?? legacyReferenceText
            transaction.day = calendar.date(bySettingHour: 0, minute: 0, second: 0, of: draft.date) ?? Date.now

            let dateComponents = calendar.dateComponents([.month, .year], from: draft.date)
            transaction.month = calendar.date(from: dateComponents) ?? Date.now
        }

        try? moc.save()
        generator.notificationOccurred(.success)
        dismiss()
    }

    private func resetCurrentLine(keepCategory: Bool) {
        price = 0
        note = ""
        if !keepCategory {
            category = nil
        }
        isEditingDecimal = false
        decimalValuesAssigned = .none
        calculatorExpression = ""
        calculatorResetID += 1
    }

    private func editBatchLine(_ draft: TransactionLineDraft) {
        withAnimation(.easeInOut(duration: 0.18)) {
            price = draft.amount
            category = draft.category
            note = draft.note
            date = draft.date
            income = draft.income
            lineDrafts.removeAll { $0.id == draft.id }
            calculatorExpression = ""
            calculatorResetID += 1

            if draft.amount.truncatingRemainder(dividingBy: 1) > 0 {
                isEditingDecimal = true
                decimalValuesAssigned = .second
            } else {
                isEditingDecimal = false
                decimalValuesAssigned = .none
            }
        }
    }

    private func assignSuggestedCategoriesToBatchLines() {
        let availableCategories = Array(expenseCategories)
        guard !availableCategories.isEmpty else { return }

        lineDrafts = lineDrafts.map { draft in
            var updatedDraft = draft
            if updatedDraft.category == nil {
                updatedDraft.category = suggestedCategory(for: updatedDraft.note, categories: availableCategories)
            }
            return updatedDraft
        }
    }

    private func suggestedCategory(for note: String, categories: [Category]) -> Category? {
        let normalizedNote = note.lowercased()

        if let exactMatch = categories.first(where: { normalizedNote.contains($0.wrappedName.lowercased()) }) {
            return exactMatch
        }

        let keywordGroups: [(keywords: [String], categoryHints: [String])] = [
            (["coffee", "latte", "tea", "drink", "kopi"], ["coffee", "drink", "food"]),
            (["food", "meal", "rice", "noodle", "restaurant", "burger", "pizza"], ["food", "restaurant", "dining"]),
            (["grab", "taxi", "bus", "train", "fuel", "parking"], ["transport", "taxi", "fuel"]),
            (["market", "grocery", "grocer", "milk", "bread"], ["grocery", "groceries", "food"]),
            (["pharmacy", "clinic", "medicine"], ["health", "medical"]),
            (["shirt", "shoe", "clothes"], ["shopping", "clothing"])
        ]

        for group in keywordGroups where group.keywords.contains(where: { normalizedNote.contains($0) }) {
            if let match = categories.first(where: { category in
                let name = category.wrappedName.lowercased()
                return group.categoryHints.contains { name.contains($0) }
            }) {
                return match
            }
        }

        return categories.first
    }

    private func collapseBatchToSingleTransaction() {
        guard !lineDrafts.isEmpty else { return }

        price = batchTotal
        note = lineDrafts.map(\.note).filter { !$0.isEmpty }.joined(separator: ", ")
        category = Array(expenseCategories).first
        income = false
        isBatchMode = false
        lineDrafts = []
        calculatorExpression = ""
        calculatorResetID += 1

        if price.truncatingRemainder(dividingBy: 1) > 0 && numberEntryType == 2 {
            isEditingDecimal = true
            decimalValuesAssigned = .second
        }
    }

    func submit() {
        let generator = UINotificationFeedbackGenerator()

        if price == 0 && category == nil {
            toastImage = "questionmark.app"
            toastTitle = "Incomplete Entry"
            showToast = true
            toggleFieldColors()

            generator.notificationOccurred(.error)

            return
        } else if price == 0 {
            toastImage = "centsign.circle"
            toastTitle = "Missing Amount"
            showToast = true
            generator.notificationOccurred(.error)
            return
        } else if category == nil {
            toastImage = "tray"
            toastTitle = "Missing Category"
            showToast = true

            toggleFieldColors()

            generator.notificationOccurred(.error)
            return
        }

        generator.notificationOccurred(.success)

        if let editedTransaction = toEdit {
            if note.trimmingCharacters(in: .whitespacesAndNewlines) == "" {
                editedTransaction.note = category!.wrappedName
            } else {
                editedTransaction.note = note.trimmingCharacters(in: .whitespaces)
            }

            if let unwrappedCategory = category {
                editedTransaction.category = unwrappedCategory
            }

            editedTransaction.bucket = bucket
            editedTransaction.currency = transactionCurrency
            editedTransaction.reference = attachmentReference?.serializedReference ?? legacyReferenceText

            DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                withAnimation(.easeInOut(duration: 0.5)) {
                    editedTransaction.amount = price
                    editedTransaction.date = date
                    editedTransaction.income = income

                    let calendar = Calendar(identifier: .gregorian)

                    editedTransaction.day =
                    calendar.date(bySettingHour: 0, minute: 0, second: 0, of: date) ?? Date.now

                    let dateComponents = calendar.dateComponents([.month, .year], from: date)

                    editedTransaction.month = calendar.date(from: dateComponents) ?? Date.now

                    if repeatType > 0 {
                        editedTransaction.onceRecurring = true
                        editedTransaction.recurringType = Int16(repeatType)
                        editedTransaction.recurringCoefficient = Int16(repeatCoefficient)

                        dataController.updateRecurringTransaction(transaction: editedTransaction)
                    } else {
                        editedTransaction.onceRecurring = false
                        editedTransaction.recurringType = Int16(repeatType)
                        editedTransaction.recurringCoefficient = Int16(repeatCoefficient)
                    }

                    dataController.save()
                }
            }

            dismiss()

            return
        }

        let transaction = Transaction(context: moc)

        if note.trimmingCharacters(in: .whitespacesAndNewlines) == "" {
            transaction.note = category?.wrappedName ?? ""
        } else {
            transaction.note = note.trimmingCharacters(in: .whitespaces)
        }

        transaction.income = income

        if let unwrappedCategory = category {
            transaction.category = unwrappedCategory
        }

        transaction.bucket = bucket
        transaction.currency = transactionCurrency
        transaction.reference = attachmentReference?.serializedReference ?? legacyReferenceText

        transaction.amount = price
        transaction.date = date
        transaction.id = UUID()

        let calendar = Calendar(identifier: .gregorian)

        transaction.day = calendar.date(bySettingHour: 0, minute: 0, second: 0, of: date) ?? Date.now

        let dateComponents = calendar.dateComponents([.month, .year], from: date)

        transaction.month = calendar.date(from: dateComponents) ?? Date.now

        if repeatType > 0 {
            transaction.onceRecurring = true
            transaction.recurringType = Int16(repeatType)
            transaction.recurringCoefficient = Int16(repeatCoefficient)
            dataController.updateRecurringTransaction(transaction: transaction)
        }

        try? moc.save()

        dismiss()
    }

    @ViewBuilder
    private var calculationBubble: some View {
        HStack(spacing: 8) {
            Text(calculatorExpression.isEmpty ? " " : calculatorExpression)
                .font(Font.satoshi(.callout, weight: .semibold))
                .foregroundColor(Color.SubtitleText)
                .lineLimit(1)

            if !calculatorExpression.isEmpty {
                Button {
                    withAnimation(.easeOut(duration: 0.16)) {
                        calculatorExpression = ""
                        calculatorResetID += 1
                    }
                } label: {
                    Image(systemName: "xmark")
                        .font(Font.satoshi(9, weight: .bold))
                        .foregroundColor(Color.SubtitleText)
                        .frame(width: 18, height: 18)
                        .background(Color.PrimaryBackground.opacity(0.8), in: Circle())
                }
                .buttonStyle(.plain)
            }
        }
        .padding(.leading, 12)
        .padding(.trailing, calculatorExpression.isEmpty ? 12 : 7)
        .frame(height: 32)
        .background(Color.SecondaryBackground.opacity(calculatorExpression.isEmpty ? 0 : 0.8), in: Capsule())
        .frame(maxWidth: .infinity, alignment: .center)
        .opacity(calculatorExpression.isEmpty ? 0 : 1)
        .animation(.easeOut(duration: 0.18), value: calculatorExpression.isEmpty)
    }

    @ViewBuilder
    private var preKeypadControlRow: some View {
        HStack(spacing: 8) {
            HStack(spacing: 7) {
                Group {
                    if date < Date.now {
                        Image(systemName: "calendar")
                    } else if #available(iOS 17.0, *) {
                        Image(systemName: "rays")
                            .symbolEffect(
                                .variableColor.iterative.dimInactiveLayers.nonReversing,
                                options: .repeating,
                                value: animateIcon
                            )
                    } else {
                        Image(systemName: "slowmo")
                    }
                }
                .foregroundColor(Color.SubtitleText)
                .font(Font.satoshi(.subheadline, weight: .semibold))

                Text(isDateToday(date: date) ? "Today, \(getDateString(date: date))" : getDateString(date: date))
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                    .lineLimit(1)

                Spacer(minLength: 0)
            }
            .padding(.vertical, 8.5)
            .padding(.horizontal, 10)
            .frame(maxWidth: .infinity, alignment: .leading)
            .overlay(
                RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                    .strokeBorder(Color.Outline, lineWidth: 1.5)
            )
            .contentShape(Rectangle())
            .onTapGesture {
                UIApplication.shared.endEditing()
                showingDatePicker = true
            }

            noteIconButton
        }
    }

    @ViewBuilder
    private func transactionTypeToggleButton(
        title: String,
        systemImage: String,
        selected: Bool,
        action: @escaping () -> Void
    ) -> some View {
        Button(action: action) {
            HStack(spacing: selected ? 7 : 0) {
                Image(systemName: systemImage)
                    .font(Font.satoshi(.footnote, weight: .bold))
                    .frame(width: 22, height: 22)
                    .background(selected ? Color.LightIcon : Color.SubtitleText.opacity(0.22), in: Circle())
                    .foregroundColor(selected ? Color.DarkBackground : Color.SubtitleText)

                if selected {
                    Text(title)
                        .font(Font.satoshi(.callout, weight: .semibold))
                        .lineLimit(1)
                        .minimumScaleFactor(0.82)
                        .transition(.opacity.combined(with: .move(edge: .trailing)))
                }
            }
            .foregroundColor(selected ? Color.PrimaryText : Color.SubtitleText)
            .padding(.horizontal, selected ? 10 : 8)
            .frame(width: selected ? 126 : 50, height: 40)
            .background(selected ? Color.LightIcon.opacity(0.88) : Color.clear, in: Capsule())
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private var accountChip: some View {
        HStack(spacing: 9) {
            Text("R")
                .font(Font.satoshi(.callout, weight: .bold))
                .foregroundColor(.white)
                .frame(width: 34, height: 34)
                .background(Color(hex: "4A5240"), in: Circle())

            VStack(alignment: .leading, spacing: 1) {
                Text("Personal")
                    .font(Font.satoshi(.callout, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                    .lineLimit(1)
                Text("\(currencySymbol)0")
                    .font(Font.satoshi(.callout, weight: .medium))
                    .foregroundColor(Color.SubtitleText)
                    .lineLimit(1)
            }
        }
        .padding(.vertical, 7)
        .padding(.leading, 8)
        .padding(.trailing, 14)
        .background(Color.SecondaryBackground, in: Capsule())
        .overlay(Capsule().stroke(Color.Outline.opacity(0.75), lineWidth: 1.2))
    }

    @ViewBuilder
    private var currencyChip: some View {
        Button {
            showCurrencyPicker = true
        } label: {
            HStack(spacing: 7) {
                Text(flagEmoji(for: transactionCurrency))
                    .font(Font.satoshi(.body, weight: .semibold))
                Text(transactionCurrency)
                    .font(Font.satoshi(.body, weight: .bold))
                    .foregroundColor(Color.PrimaryText)
            }
            .padding(.vertical, 10)
            .padding(.horizontal, 14)
            .background(Color.SecondaryBackground, in: Capsule())
            .overlay(Capsule().stroke(Color.Outline.opacity(0.75), lineWidth: 1.2))
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private var batchAccountChip: some View {
        HStack(spacing: 8) {
            Text("R")
                .font(Font.satoshi(.callout, weight: .bold))
                .foregroundColor(.white)
                .frame(width: 30, height: 30)
                .background(Color(hex: "4A5240"), in: Circle())

            VStack(alignment: .leading, spacing: 0) {
                Text("Personal")
                    .font(Font.satoshi(.callout, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                    .lineLimit(1)
                Text("\(currencySymbol)0")
                    .font(Font.satoshi(.footnote, weight: .medium))
                    .foregroundColor(Color.SubtitleText)
                    .lineLimit(1)
            }

            Image(systemName: "chevron.down")
                .font(Font.satoshi(10, weight: .bold))
                .foregroundColor(Color.SubtitleText)
                .padding(.leading, 4)
        }
        .padding(.vertical, 6)
        .padding(.leading, 7)
        .padding(.trailing, 12)
        .background(Color.SecondaryBackground, in: Capsule())
        .overlay(Capsule().stroke(Color.Outline.opacity(0.68), lineWidth: 1.1))
    }

    @ViewBuilder
    private var batchCurrencyChip: some View {
        Button {
            showCurrencyPicker = true
        } label: {
            HStack(spacing: 7) {
                Text(flagEmoji(for: transactionCurrency))
                    .font(Font.satoshi(.callout, weight: .semibold))
                Text(transactionCurrency)
                    .font(Font.satoshi(.callout, weight: .bold))
                    .foregroundColor(Color.PrimaryText)
                Image(systemName: "chevron.down")
                    .font(Font.satoshi(10, weight: .bold))
                    .foregroundColor(Color.SubtitleText)
            }
            .padding(.vertical, 9)
            .padding(.horizontal, 13)
            .background(Color.SecondaryBackground, in: Capsule())
            .overlay(Capsule().stroke(Color.Outline.opacity(0.68), lineWidth: 1.1))
        }
        .buttonStyle(.plain)
    }

    private func flagEmoji(for currencyCode: String) -> String {
        switch currencyCode.uppercased() {
        case "MYR": return "🇲🇾"
        case "SGD": return "🇸🇬"
        case "USD": return "🇺🇸"
        case "EUR": return "🇪🇺"
        case "GBP": return "🇬🇧"
        case "JPY": return "🇯🇵"
        case "IDR": return "🇮🇩"
        case "THB": return "🇹🇭"
        default: return "💳"
        }
    }

    @ViewBuilder
    private var noteIconButton: some View {
        Button {
            showNoteSheet = true
        } label: {
            Image(systemName: note.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "note.text" : "note.text.badge.plus")
                .font(Font.satoshi(.body, weight: .semibold))
                .foregroundColor(note.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? Color.SubtitleText : Color.PrimaryText)
                .frame(width: 42, height: 42)
                .overlay(
                    RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                        .strokeBorder(Color.Outline, lineWidth: 1.5)
                )
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private var transactionHeaderBar: some View {
        ZStack {
            HStack(spacing: 10) {
                headerCloseButton
                    .frame(width: 56, alignment: .leading)

                Spacer(minLength: 4)

                headerTrailingActions
                    .frame(width: 56, alignment: .trailing)
            }
            .frame(maxWidth: .infinity)

            if showToast && !isBatchMode {
                headerToast
            } else {
                transactionModeSegment
            }
        }
        .frame(maxWidth: .infinity)
        .frame(height: 46)
    }

    @ViewBuilder
    private var headerCloseButton: some View {
        Button {
            dismiss()
        } label: {
            Image(systemName: "xmark")
                .font(Font.satoshi(.subheadline, weight: .semibold))
                .dynamicTypeSize(...DynamicTypeSize.xxLarge)
                .foregroundColor(Color.SubtitleText)
                .frame(width: 40, height: 40)
                .background(Color.SecondaryBackground, in: Circle())
                .contentShape(Circle())
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private var headerTrailingActions: some View {
        if isBatchMode {
            Button {
                saveBatchTransactions()
            } label: {
                Text("Save")
                    .font(Font.satoshi(.callout, weight: .bold))
                    .foregroundColor(canSaveBatch ? Color(hex: "4A5240") : Color.SubtitleText.opacity(0.58))
                    .frame(height: 40, alignment: .trailing)
            }
            .buttonStyle(.plain)
            .disabled(!canSaveBatch)
        } else {
            HStack(spacing: 8) {
                headerRecurringButton

                if toEdit != nil {
                    headerDeleteButton
                }
            }
        }
    }

    @ViewBuilder
    private var headerRecurringButton: some View {
        Button {
            showRecurring = true
        } label: {
            if repeatType > 0 {
                Image(systemName: "repeat")
                    .font(Font.satoshi(.subheadline, weight: .semibold))
                    .dynamicTypeSize(...DynamicTypeSize.xxLarge)
                    .overlay(alignment: .topTrailing) {
                        Text(repeatOverlays[repeatType - 1])
                            .font(Font.satoshi(6, weight: .black))
                            .foregroundColor(Color.IncomeGreen)
                            .frame(width: 10, alignment: .leading)
                            .offset(x: 5.7, y: 1.5)
                    }
                    .foregroundColor(Color.IncomeGreen)
                    .frame(width: 34, height: 34)
                    .background(Color.IncomeGreen.opacity(0.23), in: Circle())
                    .contentShape(Circle())
            } else {
                Image(systemName: "repeat")
                    .font(Font.satoshi(16, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
                    .frame(width: 34, height: 34)
                    .background(Color.SecondaryBackground, in: Circle())
                    .contentShape(Circle())
            }
        }
        .accessibilityRemoveTraits(.isButton)
        .accessibilityLabel(repeatButtonAccessibility)
        .popover(
            present: $showRecurring,
            attributes: {
                $0.position = .absolute(
                    originAnchor: .bottom,
                    popoverAnchor: .top
                )
                $0.rubberBandingMode = .none
                $0.sourceFrameInset = UIEdgeInsets(top: 0, left: 0, bottom: -10, right: 0)
                $0.presentation.animation = .easeInOut(duration: 0.2)
                $0.dismissal.animation = .easeInOut(duration: 0.3)
            }
        ) {
            RecurringPickerView(
                repeatType: $repeatType, repeatCoefficient: $repeatCoefficient,
                showMenu: $showRecurring, showPicker: $showPicker)
        } background: {
            backgroundColor.opacity(0.6)
        }
    }

    @ViewBuilder
    private var headerDeleteButton: some View {
        Button {
            toDelete = toEdit
            deleteMode = true
        } label: {
            Image(systemName: "trash.fill")
                .font(Font.satoshi(.subheadline, weight: .semibold))
                .dynamicTypeSize(...DynamicTypeSize.xxLarge)
                .foregroundColor(Color.AlertRed)
                .frame(width: 34, height: 34)
                .background(Color.AlertRed.opacity(0.23), in: Circle())
                .contentShape(Circle())
        }
        .accessibilityLabel("delete transaction")
    }

    @ViewBuilder
    private var headerToast: some View {
        HStack(spacing: 6.5) {
            Image(systemName: toastImage)
                .font(Font.satoshi(.subheadline, weight: .semibold))
                .foregroundColor(Color.AlertRed)

            Text(toastTitle)
                .font(Font.satoshi(.body, weight: .semibold))
                .lineLimit(1)
                .foregroundColor(Color.AlertRed)
        }
        .padding(8)
        .background(
            Color.AlertRed.opacity(0.23),
            in: RoundedRectangle(cornerRadius: 9, style: .continuous)
        )
        .transition(AnyTransition.opacity.combined(with: .move(edge: .top)))
        .frame(maxWidth: dynamicTypeSize > .xLarge ? 250 : 200)
    }

    @ViewBuilder
    private var transactionModeSegment: some View {
        HStack(spacing: 3) {
            transactionTypeToggleButton(
                title: "Expense",
                systemImage: "arrow.up.right",
                selected: !income && !isBatchMode
            ) {
                withAnimation(.easeInOut(duration: 0.18)) {
                    isBatchMode = false
                    income = false
                    swipingOffset = 0
                }
            }

            transactionTypeToggleButton(
                title: "Income",
                systemImage: "arrow.down.left",
                selected: income && !isBatchMode
            ) {
                withAnimation(.easeInOut(duration: 0.18)) {
                    isBatchMode = false
                    income = true
                    swipingOffset = capsuleWidth
                }
            }

            if toEdit == nil {
                batchModeToggleButton
            }
        }
        .padding(3)
        .fixedSize(horizontal: true, vertical: true)
        .overlay(Capsule().stroke(Color.Outline.opacity(0.4), lineWidth: 1.3))
    }

    @ViewBuilder
    private var batchModeMainContent: some View {
        VStack(spacing: 7) {
            batchPinnedTopControls
                .layoutPriority(2)

            currentLineComposerCard

            batchLinesCard
        }
    }

    @ViewBuilder
    private var batchPinnedTopControls: some View {
        VStack(spacing: 4) {
            HStack(spacing: 8) {
                Image(systemName: "square.stack.3d.up")
                    .font(Font.satoshi(.footnote, weight: .semibold))
                    .foregroundColor(Color(hex: "4A5240"))
                    .frame(width: 22, height: 22)
                    .background(Color.SecondaryBackground, in: Circle())

                Text("\(lineDrafts.count) \(lineDrafts.count == 1 ? "transaction" : "transactions") · \(transactionCurrency) \(formatMoneyFixed(batchTotal))")
                    .font(Font.satoshi(.footnote, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
            }
        }
        .frame(maxWidth: .infinity, alignment: .top)
    }

    @ViewBuilder
    private var currentLineComposerCard: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Current line")
                .font(Font.satoshi(.footnote, weight: .semibold))
                .foregroundColor(Color(hex: "4A5240"))

            VStack(spacing: 2) {
                if !calculatorExpression.isEmpty {
                    Text(calculatorExpression)
                        .font(Font.satoshi(11, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                        .lineLimit(1)
                }

                HStack(spacing: 10) {
                    Spacer()

                    HStack(alignment: .firstTextBaseline, spacing: 7) {
                        Text(transactionCurrency)
                            .font(Font.satoshi(max(17, batchAmountFontSize - 18), weight: .bold))
                            .foregroundColor(Color(hex: "4A5240"))

                        Text(formatMoneyFixed(price))
                            .font(Font.satoshi(batchAmountFontSize, weight: .medium))
                            .foregroundColor(Color.PrimaryText)
                            .lineLimit(1)
                            .minimumScaleFactor(0.72)
                            .modifier(NumericTextTransition(value: formatMoneyFixed(price)))
                    }
                    .frame(maxWidth: .infinity, alignment: .center)

                    Button {
                        price = 0
                        isEditingDecimal = false
                        decimalValuesAssigned = .none
                        calculatorExpression = ""
                        calculatorResetID += 1
                    } label: {
                        Image(systemName: "delete.left")
                            .font(Font.satoshi(.footnote, weight: .semibold))
                            .foregroundColor(Color.PrimaryText)
                            .frame(width: 36, height: 36)
                            .background(Color.SecondaryBackground, in: Circle())
                            .overlay(Circle().stroke(Color.Outline.opacity(0.45), lineWidth: 1))
                    }
                    .buttonStyle(.plain)
                }
            }

            Divider().overlay(Color.Outline.opacity(0.65))

            HStack(spacing: 8) {
                Button {
                    showNoteSheet = true
                } label: {
                    lineDetailChip(
                        icon: "text.bubble",
                        text: note.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "Note" : note
                    )
                }
                .buttonStyle(.plain)

                Button {
                    showingDatePicker = true
                } label: {
                    lineDetailChip(icon: "calendar", text: getDateString(date: date))
                }
                .buttonStyle(.plain)

                if hasAttachmentDetails {
                    Button {
                        if let attachmentReference {
                            previewAttachmentReference = attachmentReference
                        }
                    } label: {
                        lineDetailChip(icon: "paperclip", text: "Attachment")
                    }
                    .buttonStyle(.plain)
                }
            }
        }
        .padding(9)
        .background(Color.LightIcon.opacity(0.78), in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(Color.Outline.opacity(0.62), lineWidth: 1)
        )
        .shadow(color: Color.black.opacity(0.05), radius: 10, x: 0, y: 6)
    }

    private func lineDetailChip(icon: String, text: String) -> some View {
        HStack(spacing: 6) {
            Image(systemName: icon)
                .font(Font.satoshi(.footnote, weight: .semibold))
                .foregroundColor(Color.SubtitleText)

            Text(text)
                .font(Font.satoshi(.footnote, weight: .semibold))
                .foregroundColor(Color.SubtitleText)
                .lineLimit(1)
        }
        .frame(minHeight: 22)
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    @ViewBuilder
    private var noteChipButton: some View {
        Button {
            showNoteSheet = true
        } label: {
            HStack(spacing: 6) {
                Image(systemName: note.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "note.text" : "note.text.badge.plus")
                    .font(Font.satoshi(.subheadline, weight: .semibold))
                Text(note.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "Note" : "Note added")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .lineLimit(1)
            }
            .foregroundColor(note.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? Color.SubtitleText : Color.PrimaryText)
            .padding(.vertical, 8.5)
            .padding(.horizontal, 10)
            .frame(maxWidth: .infinity, alignment: .leading)
            .overlay(
                RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                    .strokeBorder(Color.Outline, lineWidth: 1.5)
            )
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private var bottomActionBar: some View {
        HStack(spacing: 10) {
            Button {
                if (expenseCategories.count == 0 && !income) || (incomeCategories.count == 0 && income) {
                    showCategorySheet = true
                } else {
                    withAnimation(.easeInOut) {
                        showCategoryPicker = true
                    }
                }
            } label: {
                HStack(spacing: 8) {
                    Image(systemName: "square.grid.2x2")
                        .font(Font.satoshi(.callout, weight: .semibold))
                    Text(category?.wrappedName ?? "Select category")
                        .font(Font.satoshi(.callout, weight: .semibold))
                        .lineLimit(1)
                }
                .foregroundColor(category.map { Color(hex: $0.wrappedColour) } ?? Color.PrimaryText)
                .padding(.horizontal, isBatchMode ? 14 : 16)
                .frame(height: isBatchMode ? 48 : 52)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(Color.SecondaryBackground, in: Capsule())
                .overlay(Capsule().stroke(Color.Outline.opacity(0.85), lineWidth: 1.2))
            }
            .buttonStyle(.plain)
            .frame(width: isBatchMode ? 190 : nil)

            Button {
                if isBatchMode {
                    attemptAddBatchLine()
                } else {
                    attemptSubmit()
                }
            } label: {
                Text(isBatchMode ? batchAddLineLabel : (toEdit == nil ? "Add" : "Save"))
                    .font(Font.satoshi(.body, weight: .bold))
                    .foregroundColor((isBatchMode ? canAddBatchLine : canSaveTransaction) ? Color.LightIcon : Color.SubtitleText.opacity(0.86))
                    .lineLimit(1)
                    .minimumScaleFactor(0.78)
                    .frame(maxWidth: isBatchMode ? .infinity : nil)
                    .frame(width: isBatchMode ? nil : 96, height: isBatchMode ? 48 : 52)
                    .background((isBatchMode ? canAddBatchLine : canSaveTransaction) ? Color.DarkBackground : Color.SecondaryBackground, in: Capsule())
                    .overlay(Capsule().stroke(Color.Outline.opacity((isBatchMode ? canAddBatchLine : canSaveTransaction) ? 0 : 0.85), lineWidth: 1.2))
                    .animation(.easeInOut(duration: 0.18), value: isBatchMode ? canAddBatchLine : canSaveTransaction)
            }
            .buttonStyle(.plain)
            .disabled(isBatchMode ? !canAddBatchLine : !canSaveTransaction)
        }
        .padding(.top, isBatchMode ? 8 : 0)
        .padding(.bottom, isBatchMode ? 10 : 2)
    }

    private var batchAddLineLabel: String {
        if price <= 0 {
            return "Enter amount"
        }

        if category == nil {
            return "Choose category first"
        }

        return "Add \(transactionCurrency) \(formatMoneyFixed(price))"
    }

    @ViewBuilder
    private var batchModeToggleButton: some View {
        Button {
            guard !isBatchMode else { return }
            withAnimation(.easeInOut(duration: 0.18)) {
                isBatchMode = true
                income = false
                swipingOffset = 0
            }
        } label: {
            HStack(spacing: isBatchMode ? 7 : 0) {
                Image(systemName: "square.stack.3d.up")
                    .font(Font.satoshi(.footnote, weight: .bold))
                    .frame(width: 22, height: 22)
                    .background(isBatchMode ? Color.LightIcon : Color.SubtitleText.opacity(0.22), in: Circle())
                    .foregroundColor(isBatchMode ? Color.DarkBackground : Color.SubtitleText)

                if isBatchMode {
                    Text("Batch")
                        .font(Font.satoshi(.callout, weight: .semibold))
                        .lineLimit(1)
                        .minimumScaleFactor(0.82)
                        .transition(.opacity.combined(with: .move(edge: .trailing)))
                }
            }
            .foregroundColor(isBatchMode ? Color.PrimaryText : Color.SubtitleText)
            .padding(.horizontal, isBatchMode ? 10 : 8)
            .frame(width: isBatchMode ? 126 : 50, height: 40)
            .background(isBatchMode ? Color.LightIcon.opacity(0.88) : Color.clear, in: Capsule())
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private var batchLinesCard: some View {
        VStack(alignment: .leading, spacing: lineDrafts.isEmpty ? 6 : 7) {
            HStack {
                Text("Added transactions")
                    .font(Font.satoshi(.callout, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)

                Spacer()

                HStack(spacing: 6) {
                    if lineDrafts.count > 3 {
                        Text("View all (\(lineDrafts.count))")
                            .font(Font.satoshi(.footnote, weight: .bold))
                            .foregroundColor(Color(hex: "4A5240"))
                            .padding(.horizontal, 10)
                            .frame(height: 28)
                            .background(Color(hex: "4A5240").opacity(0.08), in: Capsule())
                    } else {
                        Text("\(lineDrafts.count) \(lineDrafts.count == 1 ? "item" : "items")")
                            .font(Font.satoshi(.footnote, weight: .bold))
                            .foregroundColor(Color.SubtitleText)
                            .padding(.horizontal, 10)
                            .frame(height: 28)
                            .background(Color(hex: "4A5240").opacity(0.10), in: Capsule())
                    }

                    Button {
                        guard !lineDrafts.isEmpty else { return }
                        withAnimation(.spring(response: 0.42, dampingFraction: 0.88)) {
                            showExpandedBatchLines = true
                        }
                    } label: {
                        Image(systemName: "arrow.up.left.and.arrow.down.right")
                            .font(Font.satoshi(11, weight: .bold))
                            .foregroundColor(lineDrafts.isEmpty ? Color.SubtitleText.opacity(0.45) : Color(hex: "4A5240"))
                            .frame(width: 28, height: 28)
                            .background(Color(hex: "4A5240").opacity(lineDrafts.isEmpty ? 0.05 : 0.10), in: Circle())
                    }
                    .buttonStyle(.plain)
                    .disabled(lineDrafts.isEmpty)
                }
            }

            if lineDrafts.isEmpty {
                VStack(alignment: .leading, spacing: 6) {
                    Text("Choose a category, then add this amount.")
                        .font(Font.satoshi(.callout, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                        .lineLimit(1)
                        .minimumScaleFactor(0.86)

                    Text("Batch total · \(transactionCurrency) 0.00")
                        .font(Font.satoshi(.footnote, weight: .semibold))
                        .foregroundColor(Color.SubtitleText.opacity(0.82))
                        .lineLimit(1)
                }
            } else {
                if lineDrafts.count <= 3 {
                    batchLineRows(lineDrafts)
                } else {
                    ScrollView(showsIndicators: true) {
                        batchLineRows(lineDrafts)
                    }
                    .frame(maxHeight: 126)
                }

                Divider().overlay(Color.Outline.opacity(0.65))

                HStack {
                    Text("Batch total")
                        .font(Font.satoshi(.callout, weight: .semibold))
                        .foregroundColor(Color.PrimaryText)

                    Spacer()

                    Text("\(transactionCurrency) \(formatMoney(batchTotal))")
                        .font(Font.satoshi(.callout, weight: .semibold))
                        .foregroundColor(Color(hex: "4A5240"))
                        .animation(.easeOut(duration: 0.18), value: batchTotal)
                }
                .padding(.top, 2)
            }
        }
        .padding(lineDrafts.isEmpty ? 10 : 11)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .fill(Color.LightIcon.opacity(0.72))
                .matchedGeometryEffect(id: "batchBasketCard", in: batchBasketAnimation)
        )
        .overlay(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(Color.Outline.opacity(0.65), lineWidth: 1)
        )
        .shadow(color: Color.black.opacity(0.06), radius: 10, x: 0, y: 6)
    }

    @ViewBuilder
    private func batchLineRows(_ drafts: [TransactionLineDraft]) -> some View {
        VStack(spacing: 0) {
            ForEach(drafts) { draft in
                batchLineRow(draft)

                if draft.id != drafts.last?.id {
                    Divider()
                        .overlay(Color.Outline.opacity(0.65))
                        .padding(.leading, 46)
                }
            }
        }
    }

    @ViewBuilder
    private func batchLineRow(_ draft: TransactionLineDraft) -> some View {
        HStack(spacing: 10) {
            Text(draft.category?.wrappedEmoji ?? "•")
                .font(Font.satoshi(.callout, weight: .semibold))
                .frame(width: 32, height: 32)
                .background((draft.category.map { Color(hex: $0.wrappedColour).opacity(0.16) } ?? Color.SecondaryBackground), in: Circle())

            VStack(alignment: .leading, spacing: 2) {
                Text(draft.category?.wrappedName ?? "Uncategorized")
                    .font(Font.satoshi(.callout, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                    .lineLimit(1)

                Text(draft.note.isEmpty ? "No notes" : draft.note)
                    .font(Font.satoshi(11, weight: .medium))
                    .foregroundColor(Color.SubtitleText)
                    .lineLimit(1)
            }

            Spacer()

            Text("\(transactionCurrency) \(formatMoney(draft.amount))")
                .font(Font.satoshi(.callout, weight: .semibold))
                .foregroundColor(Color.PrimaryText)
                .lineLimit(1)

            Button {
                withAnimation(.easeInOut(duration: 0.18)) {
                    lineDrafts.removeAll { $0.id == draft.id }
                }
            } label: {
                Image(systemName: "xmark")
                    .font(Font.satoshi(10, weight: .bold))
                    .foregroundColor(Color.SubtitleText.opacity(0.8))
                    .frame(width: 28, height: 28)
                    .background(Color.SecondaryBackground.opacity(0.72), in: Circle())
            }
            .buttonStyle(.plain)
        }
        .padding(.vertical, 5)
        .contentShape(Rectangle())
        .onTapGesture {
            editBatchLine(draft)
        }
    }
    @ViewBuilder
    private var batchSaveButton: some View {
        Button {
            saveBatchTransactions()
        } label: {
            HStack(spacing: 8) {
                Image(systemName: "checkmark")
                    .font(Font.satoshi(.body, weight: .bold))

                Text("Save \(lineDrafts.count) \(lineDrafts.count == 1 ? "transaction" : "transactions") · \(transactionCurrency) \(formatMoney(batchTotal))")
                    .font(Font.satoshi(.body, weight: .bold))
                    .lineLimit(1)
                    .minimumScaleFactor(0.8)
            }
            .foregroundColor(canSaveBatch ? Color.LightIcon : Color.SubtitleText)
            .frame(maxWidth: .infinity)
            .frame(height: 56)
            .background(canSaveBatch ? Color.DarkBackground : Color.SecondaryBackground, in: Capsule())
            .overlay(Capsule().stroke(Color.Outline.opacity(canSaveBatch ? 0 : 0.85), lineWidth: 1.2))
        }
        .buttonStyle(.plain)
        .disabled(!canSaveBatch)
    }

    private func formatMoney(_ value: Double) -> String {
        value.truncatingRemainder(dividingBy: 1) == 0
            ? String(format: "%.0f", value)
            : String(format: "%.2f", value)
    }

    private func formatMoneyFixed(_ value: Double) -> String {
        value == 0 ? "0" : String(format: "%.2f", value)
    }

    @ViewBuilder
    private var primaryCategoryButton: some View {
        Button {
            if (expenseCategories.count == 0 && !income) || (incomeCategories.count == 0 && income) {
                showCategorySheet = true
            } else {
                withAnimation(.easeInOut) {
                    showCategoryPicker.toggle()
                }
            }
        } label: {
            HStack(spacing: 10) {
                Group {
                    if showCategoryPicker {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundColor(Color.AlertRed)
                    } else if let category {
                        Text(category.wrappedEmoji)
                    } else {
                        Image(systemName: "circle.grid.2x2")
                            .foregroundColor(categoryButtonTextColor)
                    }
                }
                .font(Font.satoshi(.subheadline, weight: .semibold))

                VStack(alignment: .leading, spacing: 2) {
                    Text("Category")
                        .font(Font.satoshi(.footnote, weight: .medium))
                        .foregroundColor(Color.SubtitleText)

                    Text(showCategoryPicker ? "Close picker" : category?.wrappedName ?? "Choose a category")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(category.map { Color(hex: $0.wrappedColour) } ?? categoryButtonTextColor)
                        .lineLimit(1)
                }

                Spacer()

                Image(systemName: "chevron.right")
                    .font(Font.satoshi(11, weight: .bold))
                    .foregroundColor(Color.SubtitleText)
                    .rotationEffect(.degrees(showCategoryPicker ? 90 : 0))
            }
            .padding(.horizontal, 12)
            .frame(height: 54)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(
                category.map { Color(hex: $0.wrappedColour).opacity(0.13) } ?? categoryButtonBackgroundColor,
                in: RoundedRectangle(cornerRadius: 14, style: .continuous)
            )
            .overlay(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .strokeBorder(category.map { Color(hex: $0.wrappedColour).opacity(0.28) } ?? categoryButtonOutlineColor, lineWidth: 1.5)
            )
            .offset(x: shake ? -5 : 0)
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private var moreDetailsSection: some View {
        VStack(spacing: 8) {
            Button {
                withAnimation(.easeInOut(duration: 0.2)) {
                    showMoreDetails.toggle()
                }
            } label: {
                HStack(spacing: 8) {
                    Text("More details")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(Color.PrimaryText)

                    Spacer()

                    Text(moreDetailsSummary)
                        .font(Font.satoshi(.footnote, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                        .lineLimit(1)

                    Image(systemName: "chevron.down")
                        .font(Font.satoshi(10, weight: .bold))
                        .foregroundColor(Color.SubtitleText)
                        .rotationEffect(.degrees(showMoreDetails ? 180 : 0))
                }
                .padding(.vertical, 9)
                .padding(.horizontal, 12)
                .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 13, style: .continuous))
                .overlay(
                    RoundedRectangle(cornerRadius: 13, style: .continuous)
                        .strokeBorder(Color.Outline.opacity(0.75), lineWidth: 1.2)
                )
            }
            .buttonStyle(.plain)

            if showMoreDetails {
                VStack(spacing: 8) {
                    transactionAttachmentBox
                }
                .transition(.opacity.combined(with: .move(edge: .top)))
            }
        }
    }

    private var moreDetailsSummary: String {
        "Attachment"
    }

    @ViewBuilder
    private var dateDetailButton: some View {
        HStack(spacing: 7) {
            Group {
                if date < Date.now {
                    Image(systemName: "calendar")
                } else if #available(iOS 17.0, *) {
                    Image(systemName: "rays")
                        .symbolEffect(.variableColor.iterative.dimInactiveLayers.nonReversing, options: .repeating, value: animateIcon)
                } else {
                    Image(systemName: "slowmo")
                        .foregroundColor(Color.SubtitleText)
                }
            }
            .foregroundColor(Color.SubtitleText)
            .font(Font.satoshi(.subheadline, weight: .semibold))

            Text(isDateToday(date: date) ? "Today, \(getDateString(date: date))" : getDateString(date: date))
                .font(Font.satoshi(.body, weight: .semibold))
                .lineLimit(1)

            Spacer()

            Text(getTimeString(date: date))
                .font(Font.satoshi(.body, weight: .semibold))
                .foregroundColor(Color.SubtitleText)
        }
        .foregroundColor(Color.PrimaryText)
        .padding(.vertical, 8.5)
        .padding(.horizontal, 10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .overlay(
            RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                .strokeBorder(Color.Outline, lineWidth: 1.5)
        )
        .contentShape(Rectangle())
        .onTapGesture {
            UIApplication.shared.endEditing()
            showingDatePicker = true
        }
    }

    @ViewBuilder
    private var bucketPickerButton: some View {
        Menu {
            Button {
                bucket = nil
            } label: {
                Label("No Goal", systemImage: bucket == nil ? "checkmark" : "flag.slash")
            }

            ForEach(buckets, id: \.self) { item in
                Button {
                    bucket = item
                } label: {
                    Label {
                        Text(item.name ?? "Untitled Goal")
                    } icon: {
                        Text(item.emoji ?? "🏷️")
                    }
                }
            }
        } label: {
            HStack(spacing: 7) {
                Image(systemName: bucket == nil ? "flag" : "flag.fill")
                    .font(Font.satoshi(.subheadline, weight: .semibold))
                    .foregroundColor(bucket == nil ? Color.SubtitleText : Color(hex: bucket?.colour ?? "4A5240"))

                if let bucket {
                    Text("\(bucket.emoji ?? "🏷️") \(bucket.name ?? "Goal")")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .lineLimit(1)
                        .foregroundColor(Color.PrimaryText)
                } else {
                    Text("Goal")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .lineLimit(1)
                        .foregroundColor(Color.SubtitleText)
                }

                Spacer()

                Image(systemName: "chevron.down")
                    .font(Font.satoshi(10, weight: .bold))
                    .foregroundColor(Color.SubtitleText)
            }
            .padding(.vertical, 8.5)
            .padding(.horizontal, 10)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(
                RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                    .fill(bucket == nil ? Color.clear : Color(hex: bucket?.colour ?? "4A5240").opacity(0.14))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                    .strokeBorder(bucket == nil ? Color.Outline : Color(hex: bucket?.colour ?? "4A5240").opacity(0.28), lineWidth: 1.5)
            )
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private var bucketPillButton: some View {
        Button { showBucketMenu = true } label: {
            HStack(spacing: 7) {
                Image(systemName: bucket == nil ? "flag" : "flag.fill")
                    .font(Font.satoshi(.subheadline, weight: .semibold))
                    .foregroundColor(bucket == nil ? Color.SubtitleText : Color(hex: bucket?.colour ?? "4A5240"))
                if let bucket {
                    Text("\(bucket.emoji ?? "🏷️") \(bucket.name ?? "Goal")")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .lineLimit(1)
                        .foregroundColor(Color.PrimaryText)
                } else {
                    Text("Goal")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                }
                Spacer()
                Image(systemName: "chevron.down")
                    .font(Font.satoshi(9, weight: .bold))
                    .foregroundColor(Color.SubtitleText)
            }
            .padding(.vertical, 7)
            .padding(.horizontal, 10)
            .frame(maxWidth: .infinity, alignment: .leading)
            .overlay(
                RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                    .stroke(Color.Outline, lineWidth: 1.5)
            )
        }
        .buttonStyle(.plain)
        .popover(
            present: $showBucketMenu,
            attributes: {
                $0.position = .absolute(originAnchor: .bottomLeft, popoverAnchor: .topLeft)
                $0.rubberBandingMode = .none
                $0.sourceFrameInset = UIEdgeInsets(top: 0, left: 0, bottom: -10, right: 0)
                $0.presentation.animation = .easeInOut(duration: 0.2)
                $0.dismissal.animation = .easeInOut(duration: 0.3)
            }
        ) {
            BucketPickerPopover(bucket: $bucket, buckets: Array(buckets), showMenu: $showBucketMenu, darkMode: darkMode)
        } background: {
            backgroundColor.opacity(0.6)
        }
    }

    @ViewBuilder
    private func recurringDetectionBanner(_ detection: (type: Int, coefficient: Int)) -> some View {
        let label: String = {
            switch detection.type {
            case 1: return detection.coefficient == 1 ? "daily" : "every \(detection.coefficient) days"
            case 2: return detection.coefficient == 1 ? "weekly" : "every \(detection.coefficient) weeks"
            case 3: return detection.coefficient == 1 ? "monthly" : "every \(detection.coefficient) months"
            default: return "recurring"
            }
        }()
        Button {
            repeatType = detection.type
            repeatCoefficient = detection.coefficient
            recurringDetection = nil
        } label: {
            HStack(spacing: 6) {
                Image(systemName: "arrow.clockwise")
                    .font(Font.satoshi(12, weight: .semibold))
                Text("Looks \(label) — tap to set recurring")
                    .font(Font.satoshi(.subheadline, weight: .medium))
                    .lineLimit(1)
                Spacer()
                Image(systemName: "chevron.right")
                    .font(Font.satoshi(11, weight: .semibold))
            }
            .foregroundColor(Color.IncomeGreen)
            .padding(.horizontal, 12)
            .padding(.vertical, 9)
            .background(Color.IncomeGreen.opacity(0.12), in: RoundedRectangle(cornerRadius: 11.5, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                    .strokeBorder(Color.IncomeGreen.opacity(0.25), lineWidth: 1.5)
            )
        }
        .buttonStyle(.plain)
        .transition(.opacity.combined(with: .move(edge: .top)))
    }

    private func detectRecurring(for noteText: String) {
        let trimmed = noteText.trimmingCharacters(in: .whitespaces)
        guard trimmed.count >= 2 else {
            withAnimation { recurringDetection = nil }
            return
        }
        let ctx = dataController.container.viewContext
        let req = NSFetchRequest<Transaction>(entityName: "Transaction")
        req.predicate = NSCompoundPredicate(andPredicateWithSubpredicates: [
            NSPredicate(format: "note ==[cd] %@", trimmed),
            NSPredicate(format: "income == %d", income)
        ])
        req.sortDescriptors = [NSSortDescriptor(key: "date", ascending: false)]
        req.fetchLimit = 6
        guard let results = try? ctx.fetch(req), results.count >= 2 else {
            withAnimation { recurringDetection = nil }
            return
        }
        let dates = results.compactMap { $0.date }.sorted(by: >)
        guard dates.count >= 2 else {
            withAnimation { recurringDetection = nil }
            return
        }
        var intervals: [Int] = []
        for i in 0 ..< dates.count - 1 {
            let diff = Calendar.current.dateComponents([.day], from: dates[i + 1], to: dates[i]).day ?? 0
            intervals.append(diff)
        }
        func allClose(_ values: [Int], to target: Int, tolerance: Int) -> Bool {
            values.allSatisfy { abs($0 - target) <= tolerance }
        }
        let avg = intervals.reduce(0, +) / intervals.count
        let result: (type: Int, coefficient: Int)?
        if allClose(intervals, to: avg, tolerance: 2) {
            if allClose(intervals, to: 1, tolerance: 1) {
                result = (1, 1)
            } else if avg % 7 == 0 || allClose(intervals, to: avg, tolerance: 2) && avg >= 5 && avg <= 9 {
                result = (2, max(1, avg / 7))
            } else if allClose(intervals, to: avg, tolerance: 5) && avg >= 25 && avg <= 35 {
                result = (3, 1)
            } else if allClose(intervals, to: avg, tolerance: 5) && avg >= 55 && avg <= 65 {
                result = (3, 2)
            } else {
                result = nil
            }
        } else {
            result = nil
        }
        withAnimation { recurringDetection = result }
    }

    @ViewBuilder
    private var transactionAttachmentBox: some View {
        VStack(alignment: .leading, spacing: 8) {
            if let attachmentReference {
                Button {
                    previewAttachmentReference = attachmentReference
                } label: {
                    TransactionAttachmentCard(
                        reference: attachmentReference,
                        legacyReferenceText: legacyReferenceText,
                        onRemove: {
                            self.attachmentReference = nil
                        }
                    )
                }
                .buttonStyle(.plain)
            } else if let legacyReferenceText {
                TransactionAttachmentCard(
                    reference: nil,
                    legacyReferenceText: legacyReferenceText,
                    onRemove: {
                        self.legacyReferenceText = nil
                    }
                )
            } else {
                Button {
                    showingAttachmentImporter = true
                } label: {
                    HStack(spacing: 10) {
                        Image(systemName: "paperclip")
                            .font(Font.satoshi(.subheadline, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)

                        VStack(alignment: .leading, spacing: 2) {
                            Text("Add Attachment")
                                .font(Font.satoshi(.body, weight: .semibold))
                                .foregroundColor(Color.PrimaryText)

                            Text("Receipt, reference image, or PDF")
                                .font(Font.satoshi(.footnote, weight: .medium))
                                .foregroundColor(Color.SubtitleText)
                        }

                        Spacer()

                        Image(systemName: "plus")
                            .font(Font.satoshi(.caption, weight: .bold))
                            .foregroundColor(Color.SubtitleText)
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 11)
                    .background(Color.SecondaryBackground.opacity(0.55), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                    .overlay(
                        RoundedRectangle(cornerRadius: 14, style: .continuous)
                            .stroke(Color.Outline, lineWidth: 1.5)
                    )
                }
                .buttonStyle(.plain)
            }
        }
    }

    @ViewBuilder
    private var attachmentSquareButton: some View {
        if let ref = attachmentReference {
            Button { previewAttachmentReference = ref } label: {
                HStack(spacing: 8) {
                    ZStack {
                        RoundedRectangle(cornerRadius: 8, style: .continuous)
                            .fill(Color.SecondaryBackground)
                        if let thumb = TransactionAttachmentStore.thumbnail(for: ref) {
                            Image(uiImage: thumb)
                                .resizable()
                                .scaledToFill()
                                .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                        } else {
                            Image(systemName: ref.kind == .pdf ? "doc.richtext.fill" : "photo.fill")
                                .font(Font.satoshi(14, weight: .semibold))
                                .foregroundColor(ref.kind == .pdf ? Color(hex: "F7C65B") : Color(hex: "5B8C5A"))
                        }
                    }
                    .frame(width: 30, height: 30)

                    Text(ref.displayName)
                        .font(Font.satoshi(.footnote, weight: .medium))
                        .foregroundColor(Color.PrimaryText)
                        .lineLimit(1)

                    Spacer()

                    Button {
                        attachmentReference = nil
                    } label: {
                        Image(systemName: "xmark")
                            .font(.system(size: 9, weight: .bold))
                            .foregroundColor(Color.SubtitleText)
                            .frame(width: 18, height: 18)
                            .background(Color.SecondaryBackground, in: Circle())
                    }
                    .buttonStyle(.plain)
                }
                .padding(.vertical, 7)
                .padding(.horizontal, 10)
                .overlay(RoundedRectangle(cornerRadius: 11.5, style: .continuous).stroke(Color.Outline, lineWidth: 1.5))
            }
            .buttonStyle(.plain)
        } else if let legacyText = legacyReferenceText {
            HStack(spacing: 8) {
                Image(systemName: "text.quote")
                    .font(Font.satoshi(14, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
                    .frame(width: 30, height: 30)

                Text(legacyText)
                    .font(Font.satoshi(.footnote, weight: .medium))
                    .foregroundColor(Color.PrimaryText)
                    .lineLimit(1)

                Spacer()

                Button {
                    legacyReferenceText = nil
                } label: {
                    Image(systemName: "xmark")
                        .font(.system(size: 9, weight: .bold))
                        .foregroundColor(Color.SubtitleText)
                        .frame(width: 18, height: 18)
                        .background(Color.SecondaryBackground, in: Circle())
                }
                .buttonStyle(.plain)
            }
            .padding(.vertical, 7)
            .padding(.horizontal, 10)
            .overlay(RoundedRectangle(cornerRadius: 11.5, style: .continuous).stroke(Color.Outline, lineWidth: 1.5))
        } else {
            Button { showingAttachmentImporter = true } label: {
                HStack(spacing: 7) {
                    Image(systemName: "paperclip")
                        .font(Font.satoshi(.subheadline, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                    Text("Add Attachment")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                        .lineLimit(1)
                }
                .padding(.vertical, 7)
                .padding(.horizontal, 10)
                .frame(maxWidth: .infinity, alignment: .leading)
                .overlay(RoundedRectangle(cornerRadius: 11.5, style: .continuous).stroke(Color.Outline, lineWidth: 1.5))
            }
            .buttonStyle(.plain)
        }
    }

    init(prefill: ScannedReceiptPrefill?) {
        let globalCurrency = UserDefaults(suiteName: "group.com.riskcreatives.duit")?.string(forKey: "currency") ?? Locale.current.currencyCode ?? "MYR"
        _transactionCurrency = State(initialValue: globalCurrency)
        _income = State(initialValue: false)

        receiptPrefill = prefill
        receiptPrefillFingerprint = prefill.map(Self.fingerprint(for:))
        prefillAmount = prefill?.amount

        if let prefill {
            if let amount = prefill.amount {
                _price = State(initialValue: amount)
                if amount.truncatingRemainder(dividingBy: 1) > 0 {
                    _isEditingDecimal = State(initialValue: true)
                    _decimalValuesAssigned = State(initialValue: .second)
                }
            }

            if prefill.lineItems.count >= 2 {
                let prefillDate = prefill.date ?? Date.now
                _isBatchMode = State(initialValue: true)
                _lineDrafts = State(initialValue: prefill.lineItems.map {
                    TransactionLineDraft(
                        amount: $0.amount,
                        category: nil,
                        note: $0.note,
                        date: prefillDate,
                        income: false
                    )
                })
            }

            _note = State(initialValue: prefill.note)

            if let date = prefill.date {
                _date = State(initialValue: date)
            }

            _attachmentReference = State(initialValue: prefill.attachmentReference)
        }

        #if DEBUG
        if let prefill {
            debugPrefillInfo = "prefill: amount=\(prefill.amount.map { String($0) } ?? "nil") note=\(prefill.note) sourceText=\(prefill.sourceText.map { "\($0.prefix(60))…" } ?? "nil")"
        } else {
            debugPrefillInfo = "prefill: nil"
        }
        #endif

        toEdit = nil
    }

    init(toEdit: Transaction? = nil) {
        let globalCurrency = UserDefaults(suiteName: "group.com.riskcreatives.duit")?.string(forKey: "currency") ?? Locale.current.currencyCode ?? "MYR"
        if let transaction = toEdit {
            _note = State(initialValue: transaction.wrappedNote)

            if let unwrappedCategory = transaction.category {
                _category = State(initialValue: unwrappedCategory)
            }

            if let unwrappedBucket = transaction.bucket {
                _bucket = State(initialValue: unwrappedBucket)
            }

            _income = State(initialValue: transaction.income)

            if transaction.income {
                _swipingOffset = State(initialValue: 100)
            }

            _date = State(initialValue: transaction.date ?? Date.now)
            _transactionCurrency = State(initialValue: transaction.currency ?? globalCurrency)
        } else {
            _transactionCurrency = State(initialValue: globalCurrency)
        }
        self.toEdit = toEdit
        receiptPrefill = nil
        receiptPrefillFingerprint = nil
        prefillAmount = nil
    }

    init(category: Category? = nil) {
        let globalCurrency = UserDefaults(suiteName: "group.com.riskcreatives.duit")?.string(forKey: "currency") ?? Locale.current.currencyCode ?? "MYR"
        _transactionCurrency = State(initialValue: globalCurrency)
        if let unwrappedCategory = category {
            _income = State(initialValue: false)
            _category = State(initialValue: unwrappedCategory)
        }

        toEdit = nil
        receiptPrefill = nil
        receiptPrefillFingerprint = nil
        prefillAmount = nil
    }

    private static func fingerprint(for prefill: ScannedReceiptPrefill) -> String {
        let amount = prefill.amount.map { String(format: "%.2f", $0) } ?? "nil"
        let date = prefill.date.map { String($0.timeIntervalSince1970) } ?? "nil"
        let lines = prefill.lineItems
            .map { "\($0.note):\(String(format: "%.2f", $0.amount))" }
            .joined(separator: "|")
        let sourceHash = prefill.sourceText.map { String($0.hashValue) } ?? "nil"
        return [amount, prefill.note, date, lines, sourceHash].joined(separator: "§")
    }
}

struct FilteredSearchNewTransactionView: View {
    @FetchRequest<Transaction> private var transactions: FetchedResults<Transaction>

    var searchQuery: String
    var category: Category?

    var body: some View {
        ScrollView(.horizontal) {
            if transactions.count > 0 && category != nil {
                HStack {
                    ForEach(filterOutDupes(day: transactions)) { transaction in
                        Text(transaction.wrappedNote)
                    }
                }
            }
        }
    }

    init(searchQuery: String, category: Category?) {
        let beginPredicate = NSPredicate(
            format: "%K BEGINSWITH[cd] %@", #keyPath(Transaction.note), searchQuery)
        let containPredicate = NSPredicate(
            format: "%K CONTAINS[cd] %@", #keyPath(Transaction.note), searchQuery)
        let compound = NSCompoundPredicate(orPredicateWithSubpredicates: [
            beginPredicate, containPredicate
        ])

        if let unwrappedCategory = category {
            let categoryPredicate = NSPredicate(
                format: "%K == %@", #keyPath(Transaction.category), unwrappedCategory)

            let andPredicate = NSCompoundPredicate(
                type: .and, subpredicates: [compound, categoryPredicate])

            _transactions = FetchRequest<Transaction>(
                sortDescriptors: [
                    SortDescriptor(\.date, order: .reverse)
                ], predicate: andPredicate)
        } else {
            _transactions = FetchRequest<Transaction>(
                sortDescriptors: [
                    SortDescriptor(\.date, order: .reverse)
                ], predicate: compound)
        }

        self.searchQuery = searchQuery
        self.category = category
    }

    func filterOutDupes(day: FetchedResults<Transaction>) -> [Transaction] {
        var seen = [Transaction]()
        let filtered = day.filter { entity -> Bool in
            if seen.contains(where: { $0.wrappedNote == entity.wrappedNote }) {
                return false
            } else {
                seen.append(entity)
                return true
            }
        }

        return filtered
    }
}

struct NumPadButton: ButtonStyle {
    public func makeBody(configuration: Self.Configuration) -> some View {
        return configuration.label
            .scaleEffect(configuration.isPressed ? 0.8 : 1)
            .animation(.easeOut(duration: 0.3), value: configuration.isPressed)
            .opacity(configuration.isPressed ? 0.5 : 1)
    }
}

func getDollarOffset(big: CGFloat, small: CGFloat) -> CGFloat {
    let bigFont = UIFont.rounded(ofSize: big, weight: .regular)
    let smallFont = UIFont.rounded(ofSize: small, weight: .light)

    return bigFont.capHeight - smallFont.capHeight - 1
}

struct CategoryPickerView: View {
    @Binding var category: Category?
    @Binding var showPicker: Bool
    @Binding var showingCategoryView: Bool
    @FetchRequest private var categories: FetchedResults<Category>

    let initialCategory: Category?

    var darkMode: Bool

    var backgroundColor: Color {
        if darkMode {
            return Color("AlwaysDarkBackground")
        } else {
            return Color("AlwaysLightBackground")
        }
    }

    var secondaryBackgroundColor: Color {
        if darkMode {
            return Color("AlwaysDarkSecondaryBackground")
        } else {
            return Color("AlwaysLightSecondaryBackground")
        }
    }

    @Environment(\.dynamicTypeSize) var dynamicTypeSize

    var heightOfScrollView: Double {
        let fontSize = UIFont.getBodyFontSize(dynamicTypeSize: dynamicTypeSize)

        let font = UIFont.rounded(ofSize: fontSize, weight: .semibold)

        if categories.count == 1 && initialCategory != nil {
            return font.lineHeight + 14.1
        }

        if initialCategory != nil {
            let height = Double(min(6, categories.count)) * (font.lineHeight + 14.1)
            let gap = Double(min(5, categories.count - 1)) * 8.0
            return height + gap
        } else {
            let height = Double(min(6, categories.count + 1)) * (font.lineHeight + 14.1)
            let gap = Double(min(5, categories.count)) * 8.0
            return height + gap
        }
    }

    var body: some View {
        //        VStack(alignment: .trailing, spacing: 8) {

        //            if categories.count > 1 || category == nil {
        HStack {
            Color.clear
                .frame(maxWidth: .infinity)
                .contentShape(Rectangle())
                .onTapGesture {
                    showPicker = false
                }

            VStack(
                alignment: .trailing, spacing: (categories.count == 1 && initialCategory != nil) ? 0 : 8
            ) {
                HStack(spacing: 4) {
                    Image(systemName: "pencil")
                        .font(Font.satoshi(15, weight: .bold))
                    Text("Edit")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                }
                .padding(.horizontal, 11)
                .padding(.vertical, 7)
                .foregroundColor(darkMode ? Color("AlwaysLightBackground") : Color("AlwaysDarkBackground"))
                .background(
                    RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                        .fill(
                            darkMode
                            ? Color("AlwaysDarkSecondaryBackground") : Color("AlwaysLightSecondaryBackground"))
                )
                .contentShape(RoundedRectangle(cornerRadius: 13, style: .continuous))
                .onTapGesture {
                    let impactMed = UIImpactFeedbackGenerator(style: .light)
                    impactMed.impactOccurred()
                    showPicker = false
                    showingCategoryView = true
                }

                ScrollView(showsIndicators: false) {
                    ScrollViewReader { value in
                        VStack(alignment: .trailing, spacing: 8) {
                            ForEach(categories) { item in
                                if item != initialCategory {
                                    HStack(spacing: 7) {
                                        Text(item.wrappedEmoji)
                                            .font(Font.satoshi(.footnote))
                                            .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                                        //                                                    .font(Font.satoshi(14))
                                        Text(item.wrappedName)
                                            .font(Font.satoshi(.body, weight: .semibold))
                                            .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                                        //                                                    .font(Font.satoshi(17.5, weight: .semibold))
                                            .lineLimit(1)
                                    }
                                    .id(item.id)
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 7)
                                    .foregroundColor(
                                        darkMode ? Color("AlwaysLightBackground") : Color("AlwaysDarkBackground")
                                    )
                                    .background(
                                        item == category ? secondaryBackgroundColor : backgroundColor,
                                        in: RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                                    )
                                    .contentShape(Rectangle())
                                    .overlay {
                                        RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                                            .strokeBorder(
                                                darkMode ? Color("AlwaysDarkOutline") : Color("AlwaysLightOutline"),
                                                style: StrokeStyle(lineWidth: 1.5))
                                    }
                                    .onTapGesture {
                                        if category == item {
                                            category = nil
                                        } else {
                                            category = item
                                        }

                                        showPicker = false
                                        //
                                    }
                                }
                            }
                        }
                        .onAppear {
                            if let last = categories.last {
                                if category == last && categories.count > 2 {
                                    value.scrollTo(categories[categories.count - 2].id)
                                } else {
                                    value.scrollTo(last.id)
                                }
                            }
                        }
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .trailing)
        }
        .frame(height: heightOfScrollView)
        //            }
    }

    init(
        category: Binding<Category?>?, showPicker: Binding<Bool>, showSheet: Binding<Bool>,
        income: Bool, darkMode: Bool
    ) {
        _categories = FetchRequest<Category>(
            sortDescriptors: [
                SortDescriptor(\.order, order: .reverse)
            ], predicate: NSPredicate(format: "income = %d", income))
        self.darkMode = darkMode
        initialCategory = category?.wrappedValue
        _category = category ?? Binding.constant(nil)
        _showPicker = showPicker
        _showingCategoryView = showSheet
    }
}

struct NoteView: View {
    @Binding var note: String
    @Binding var focused: Bool

    @FocusState private var textFocused: Bool
    let characterLimit = 50

    @Environment(\.dynamicTypeSize) var dynamicTypeSize

    var noteWidth: CGFloat {
        let fontSize: CGFloat = UIFont.getBodyFontSize(dynamicTypeSize: dynamicTypeSize)
        //        let fontSize: CGFloat = fontSize
        let systemFont = UIFont.systemFont(ofSize: fontSize, weight: .semibold)
        let roundedFont: UIFont
        if let descriptor = systemFont.fontDescriptor.withDesign(.rounded) {
            roundedFont = UIFont(descriptor: descriptor, size: fontSize)
        } else {
            roundedFont = systemFont
        }

        let attributes = [NSAttributedString.Key.font: roundedFont]

        let size = (note as NSString).size(withAttributes: attributes)

        let placeholder = String(localized: "Add Note")
        let placeholderSize = (placeholder as NSString).size(withAttributes: attributes)

        if !note.isEmpty {
            return size.width + 2
        } else {
            return placeholderSize.width
        }

        //        return max(size.width + 2, (placeholderSize.width + 1))
    }

    var body: some View {
        HStack(spacing: 7) {
            Image(systemName: "text.alignleft")
                .font(Font.satoshi(.subheadline, weight: .semibold))
                .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
            //                .font(Font.satoshi(14, weight: .semibold))
                .foregroundColor(Color.SubtitleText)

            ZStack(alignment: .leading) {
                TextField("", text: $note)
                    .onReceive(Just(note)) { _ in limitText(characterLimit) }
                    .focused($textFocused)
                    .foregroundColor(Color.PrimaryText)

                if note.isEmpty {
                    Text("Add Note")
                        .foregroundColor(Color.SubtitleText)
                }
            }
            .font(Font.satoshi(.body, weight: .semibold))
            .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .onTapGesture {
            textFocused = true
        }
        .padding(.vertical, 14)
        .padding(.horizontal, 10)
        .frame(maxWidth: .infinity)
        .overlay(
            RoundedRectangle(cornerRadius: 11.5, style: .continuous)
                .stroke(Color.Outline, lineWidth: 1.5)
        )
        .onChange(of: textFocused) { newValue in
            focused = newValue
        }
    }

    func limitText(_ upper: Int) {
        if note.count > upper {
            note = String(note.prefix(upper))
        }
    }
}

private struct TransactionAttachmentCard: View {
    let reference: TransactionAttachmentReference?
    let legacyReferenceText: String?
    let onRemove: () -> Void

    @State private var thumbnail: UIImage?

    var body: some View {
        HStack(spacing: 12) {
            ZStack {
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .fill(Color.SecondaryBackground)
                    .frame(width: 54, height: 54)

                if let thumbnail {
                    Image(uiImage: thumbnail)
                        .resizable()
                        .scaledToFill()
                        .frame(width: 54, height: 54)
                        .clipped()
                        .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                } else {
                    Image(systemName: reference?.kind == .pdf ? "doc.richtext.fill" : "photo")
                        .font(Font.satoshi(.title3, weight: .semibold))
                        .foregroundColor(reference?.kind == .pdf ? Color(hex: "F7C65B") : Color(hex: "5B8C5A"))
                }
            }

            VStack(alignment: .leading, spacing: 3) {
                Text(reference?.displayName ?? "Reference")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                    .lineLimit(1)

                if let sourceLabel = reference?.sourceLabel {
                    Text(sourceLabel)
                        .font(Font.satoshi(.footnote, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                        .lineLimit(1)
                } else if let legacyReferenceText {
                    Text(legacyReferenceText)
                        .font(Font.satoshi(.footnote, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                        .lineLimit(2)
                }

                if let reference, reference.pageCount > 1 {
                    Text("\(reference.pageCount) pages")
                        .font(Font.satoshi(.caption, weight: .medium))
                        .foregroundColor(Color.SubtitleText)
                }
            }

            Spacer()

            Button {
                onRemove()
            } label: {
                Image(systemName: "xmark")
                    .font(Font.satoshi(11, weight: .bold))
                    .foregroundColor(Color.SubtitleText)
                    .frame(width: 24, height: 24)
                    .background(Color.PrimaryBackground.opacity(0.82), in: Circle())
            }
            .buttonStyle(.plain)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(Color.SecondaryBackground.opacity(0.55), in: RoundedRectangle(cornerRadius: 14, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .stroke(Color.Outline, lineWidth: 1.5)
        )
        .onAppear {
            guard let reference else { return }
            thumbnail = TransactionAttachmentStore.thumbnail(for: reference)
        }
    }
}

private struct BatchTransactionsExpandedView: View {
    @Binding var lineDrafts: [TransactionLineDraft]
    let transactionCurrency: String
    let batchTotal: Double
    let namespace: Namespace.ID
    let onClose: () -> Void
    private let basketCornerRadius: CGFloat = 18

    var body: some View {
        ZStack {
            Color.PrimaryBackground
                .ignoresSafeArea()

            VStack(spacing: 0) {
                VStack(spacing: 12) {
                    HStack {
                        VStack(alignment: .leading, spacing: 3) {
                            Text("Added transactions")
                                .font(Font.satoshi(.body, weight: .semibold))
                                .foregroundColor(Color.PrimaryText)

                            Text("\(lineDrafts.count) \(lineDrafts.count == 1 ? "item" : "items") · \(transactionCurrency) \(formatMoney(batchTotal))")
                                .font(Font.satoshi(.footnote, weight: .semibold))
                                .foregroundColor(Color.SubtitleText)
                        }

                        Spacer()

                        Button {
                            onClose()
                        } label: {
                            Image(systemName: "xmark")
                                .font(Font.satoshi(11, weight: .bold))
                                .foregroundColor(Color.SubtitleText)
                                .frame(width: 32, height: 32)
                                .background(Color.SecondaryBackground, in: Circle())
                        }
                        .buttonStyle(.plain)
                    }
                    .padding(.horizontal, 14)
                    .padding(.top, 13)

                    ScrollView(showsIndicators: true) {
                        LazyVStack(spacing: 0) {
                            ForEach(lineDrafts) { draft in
                                expandedBatchLineRow(draft)

                                if draft.id != lineDrafts.last?.id {
                                    Divider()
                                        .overlay(Color.Outline.opacity(0.65))
                                        .padding(.leading, 48)
                                }
                            }
                        }
                        .padding(.horizontal, 14)
                        .padding(.bottom, 4)
                    }

                    Divider().overlay(Color.Outline.opacity(0.65))

                    HStack {
                        Text("Batch total")
                            .font(Font.satoshi(.callout, weight: .semibold))
                            .foregroundColor(Color.PrimaryText)

                        Spacer()

                        Text("\(transactionCurrency) \(formatMoney(batchTotal))")
                            .font(Font.satoshi(.callout, weight: .semibold))
                            .foregroundColor(Color(hex: "4A5240"))
                    }
                    .padding(.horizontal, 14)
                    .padding(.bottom, 13)
                }
                .background(
                    RoundedRectangle(cornerRadius: basketCornerRadius, style: .continuous)
                        .fill(Color.LightIcon)
                        .matchedGeometryEffect(id: "batchBasketCard", in: namespace)
                )
                .overlay(
                    RoundedRectangle(cornerRadius: basketCornerRadius, style: .continuous)
                        .stroke(Color.Outline.opacity(0.65), lineWidth: 1)
                )
                .shadow(color: Color.black.opacity(0.06), radius: 10, x: 0, y: 6)
                .padding(.horizontal, 16)
                .padding(.top, 10)
                .padding(.bottom, 10)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        }
    }

    @ViewBuilder
    private func expandedBatchLineRow(_ draft: TransactionLineDraft) -> some View {
        HStack(spacing: 10) {
            Text(draft.category?.wrappedEmoji ?? "•")
                .font(Font.satoshi(.callout, weight: .semibold))
                .frame(width: 32, height: 32)
                .background((draft.category.map { Color(hex: $0.wrappedColour).opacity(0.16) } ?? Color.SecondaryBackground), in: Circle())

            VStack(alignment: .leading, spacing: 3) {
                Text(draft.category?.wrappedName ?? "Uncategorized")
                    .font(Font.satoshi(.callout, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                    .lineLimit(1)

                Text(draft.note.isEmpty ? "No notes" : draft.note)
                    .font(Font.satoshi(.footnote, weight: .medium))
                    .foregroundColor(Color.SubtitleText)
                    .lineLimit(2)
            }

            Spacer()

            Text("\(transactionCurrency) \(formatMoney(draft.amount))")
                .font(Font.satoshi(.callout, weight: .semibold))
                .foregroundColor(Color.PrimaryText)

            Button {
                withAnimation(.easeInOut(duration: 0.18)) {
                    lineDrafts.removeAll { $0.id == draft.id }
                }
            } label: {
                Image(systemName: "xmark")
                    .font(Font.satoshi(10, weight: .bold))
                    .foregroundColor(Color.SubtitleText.opacity(0.8))
                    .frame(width: 28, height: 28)
                    .background(Color.SecondaryBackground.opacity(0.72), in: Circle())
            }
            .buttonStyle(.plain)
        }
        .padding(.vertical, 8)
    }

    private func formatMoney(_ value: Double) -> String {
        value.truncatingRemainder(dividingBy: 1) == 0
            ? String(format: "%.0f", value)
            : String(format: "%.2f", value)
    }
}

private struct TransactionAttachmentPreviewSheet: View {
    let reference: TransactionAttachmentReference
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationView {
            Group {
                if reference.kind == .pdf, let url = reference.fileURL {
                    TransactionPDFPreview(url: url)
                } else if let url = reference.fileURL,
                          let data = try? Data(contentsOf: url),
                          let image = UIImage(data: data) {
                    ZStack {
                        Color.black.opacity(0.96).ignoresSafeArea()
                        RoundedRectangle(cornerRadius: 24, style: .continuous)
                            .fill(Color.black)
                            .overlay(
                                Image(uiImage: image)
                                    .resizable()
                                    .scaledToFit()
                                    .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
                            )
                            .padding(.horizontal, 18)
                            .padding(.vertical, 28)
                    }
                } else {
                    VStack(spacing: 12) {
                        Image(systemName: "exclamationmark.triangle")
                            .font(Font.satoshi(28, weight: .semibold))
                            .foregroundColor(Color.AlertRed)
                        Text("Attachment unavailable")
                            .font(Font.satoshi(.headline, weight: .semibold))
                            .foregroundColor(Color.PrimaryText)
                    }
                }
            }
            .navigationTitle(reference.displayName)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Close") { dismiss() }
                }
            }
        }
    }
}

private struct TransactionPDFPreview: UIViewRepresentable {
    let url: URL

    func makeUIView(context: Context) -> PDFView {
        let view = PDFView()
        view.autoScales = true
        view.displayMode = .singlePageContinuous
        view.displayDirection = .vertical
        view.document = PDFDocument(url: url)
        return view
    }

    func updateUIView(_ uiView: PDFView, context: Context) {
        if uiView.document?.documentURL != url {
            uiView.document = PDFDocument(url: url)
        }
    }
}

private struct BucketPickerPopover: View {
    @Binding var bucket: Bucket?
    let buckets: [Bucket]
    @Binding var showMenu: Bool
    let darkMode: Bool
    @Namespace var animation

    var body: some View {
        VStack(alignment: .leading, spacing: 1) {
            HStack {
                Text("No Goal")
                    .font(Font.satoshi(.body, weight: .medium))
                    .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                Spacer()
                if bucket == nil {
                    Image(systemName: "checkmark")
                        .font(Font.satoshi(.footnote, weight: .medium))
                        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(5)
            .background {
                if bucket == nil {
                    RoundedRectangle(cornerRadius: 6)
                        .fill(darkMode ? Color("AlwaysDarkSecondaryBackground") : Color("AlwaysLightSecondaryBackground"))
                        .matchedGeometryEffect(id: "BUCKET_SEL", in: animation)
                }
            }
            .contentShape(Rectangle())
            .onTapGesture {
                withAnimation(.easeIn(duration: 0.15)) { bucket = nil }
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.4) { showMenu = false }
            }

            ForEach(buckets, id: \.self) { item in
                HStack {
                    Text("\(item.emoji ?? "🏷️") \(item.name ?? "Goal")")
                        .font(Font.satoshi(.body, weight: .medium))
                        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                        .lineLimit(1)
                    Spacer()
                    if bucket == item {
                        Image(systemName: "checkmark")
                            .font(Font.satoshi(.footnote, weight: .medium))
                            .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(5)
                .background {
                    if bucket == item {
                        RoundedRectangle(cornerRadius: 6)
                            .fill(darkMode ? Color("AlwaysDarkSecondaryBackground") : Color("AlwaysLightSecondaryBackground"))
                            .matchedGeometryEffect(id: "BUCKET_SEL", in: animation)
                    }
                }
                .contentShape(Rectangle())
                .onTapGesture {
                    withAnimation(.easeIn(duration: 0.15)) { bucket = item }
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.4) { showMenu = false }
                }
            }
        }
        .foregroundColor(darkMode ? Color("AlwaysLightBackground") : Color("AlwaysDarkBackground"))
        .padding(4)
        .frame(width: 180)
        .background(
            RoundedRectangle(cornerRadius: 9)
                .fill(darkMode ? Color("AlwaysDarkBackground") : Color("AlwaysLightBackground"))
                .shadow(color: darkMode ? Color.clear : Color.gray.opacity(0.25), radius: 6)
        )
        .overlay(
            RoundedRectangle(cornerRadius: 9)
                .stroke(darkMode ? Color.gray.opacity(0.1) : Color.clear, lineWidth: 1.3)
        )
    }
}

private struct CurrencyPickerSheet: View {
    @Binding var currency: String
    @Environment(\.dismiss) private var dismiss
    @State private var search = ""

    var filteredCodes: [String] {
        let codes = Locale.isoCurrencyCodes.sorted()
        guard !search.isEmpty else { return codes }
        return codes.filter { code in
            code.localizedCaseInsensitiveContains(search) ||
            (Locale.current.localizedString(forCurrencyCode: code) ?? "").localizedCaseInsensitiveContains(search)
        }
    }

    var body: some View {
        NavigationView {
            List(filteredCodes, id: \.self) { code in
                Button {
                    currency = code
                    dismiss()
                } label: {
                    HStack {
                        VStack(alignment: .leading, spacing: 2) {
                            Text(code)
                                .font(Font.satoshi(.body, weight: .semibold))
                                .foregroundColor(Color.PrimaryText)
                            if let name = Locale.current.localizedString(forCurrencyCode: code) {
                                Text(name)
                                    .font(Font.satoshi(.footnote, weight: .medium))
                                    .foregroundColor(Color.SubtitleText)
                            }
                        }
                        Spacer()
                        if code == currency {
                            Image(systemName: "checkmark")
                                .font(Font.satoshi(.footnote, weight: .semibold))
                                .foregroundColor(Color.IncomeGreen)
                        }
                    }
                }
                .buttonStyle(.plain)
            }
            .searchable(text: $search, prompt: "Search currency")
            .navigationTitle("Currency")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") { dismiss() }
                }
            }
        }
    }
}

private struct TransactionNoteSheet: View {
    @Binding var note: String
    @Environment(\.dismiss) private var dismiss
    @FocusState private var focused: Bool
    @State private var didRequestInitialFocus = false

    var body: some View {
        NavigationView {
            VStack(spacing: 12) {
                TextEditor(text: $note)
                    .font(Font.satoshi(.body, weight: .medium))
                    .foregroundColor(Color.PrimaryText)
                    .frame(height: 118)
                    .padding(12)
                    .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
                    .overlay(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .stroke(Color.Outline.opacity(0.75), lineWidth: 1)
                    )
                    .overlay(alignment: .topLeading) {
                        if note.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                            Text("Add notes or tags starting with #")
                                .font(Font.satoshi(.body, weight: .medium))
                                .foregroundColor(Color.SubtitleText.opacity(0.55))
                                .padding(.horizontal, 18)
                                .padding(.top, 20)
                                .allowsHitTesting(false)
                        }
                    }
                    .focused($focused)

                HStack(spacing: 8) {
                    Image(systemName: "sparkles")
                    Text("KIRA can use notes to explain spending later.")
                }
                .font(Font.satoshi(.footnote, weight: .semibold))
                .foregroundColor(Color.SubtitleText)

                Spacer(minLength: 0)
            }
            .padding(.horizontal, 18)
            .padding(.top, 12)
            .background(Color.PrimaryBackground.ignoresSafeArea())
            .navigationTitle("Notes and tags")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button {
                        dismiss()
                    } label: {
                        Image(systemName: "xmark")
                            .font(Font.satoshi(.callout, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                            .frame(width: 40, height: 40)
                            .background(Color.SecondaryBackground, in: Circle())
                    }
                }

                ToolbarItem(placement: .topBarTrailing) {
                    Button("Save") {
                        dismiss()
                    }
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                }
            }
        }
        .navigationViewStyle(.stack)
        .onAppear {
            guard !didRequestInitialFocus else { return }
            didRequestInitialFocus = true

            DispatchQueue.main.asyncAfter(deadline: .now() + 0.55) {
                var focusTransaction = SwiftUI.Transaction(animation: nil)
                focusTransaction.disablesAnimations = true
                withTransaction(focusTransaction) {
                    focused = true
                }
            }
        }
        .onDisappear {
            focused = false
        }
    }
}

private extension View {
    @ViewBuilder
    func compactNoteSheet() -> some View {
        if #available(iOS 16.0, *) {
            self.presentationDetents([.height(290)])
        } else {
            self
        }
    }
}

struct RecurringPickerView: View {
    @Namespace var animation
    @Binding var repeatType: Int
    @Binding var repeatCoefficient: Int
    @Binding var showMenu: Bool

    @Binding var showPicker: Bool

    let stringArray = ["none", "daily", "weekly", "monthly"]
    let stringArray2 = ["", "days", "weeks", "months"]

    @AppStorage("bottomEdge", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var bottomEdge: Double = 15

    @State private var offset: CGFloat = 0

    @State var holdingType = 0
    @State var holdingCoefficient = 0

    @AppStorage("colourScheme", store: UserDefaults(suiteName: "group.com.riskcreatives.duit"))
    var colourScheme: Int = 0

    @Environment(\.colorScheme) var systemColorScheme

    var darkMode: Bool {
        (colourScheme == 0 && systemColorScheme == .dark) || colourScheme == 2
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 1) {
            ForEach(stringArray, id: \.self) { string in
                HStack {
                    Text(LocalizedStringKey(string))
                        .font(Font.satoshi(.body, weight: .medium))
                        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                        .lineLimit(1)
                    Spacer()

                    if repeatType == (stringArray.firstIndex(of: string) ?? 0) && repeatCoefficient == 1 {
                        Image(systemName: "checkmark")
                            .font(Font.satoshi(.footnote, weight: .medium))
                            .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                        //                            .font(Font.satoshi(14, weight: .medium))
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                //                .font(Font.satoshi(18, weight: .medium))
                .padding(5)
                .background {
                    if repeatType == (stringArray.firstIndex(of: string) ?? 0) && repeatCoefficient == 1 {
                        RoundedRectangle(cornerRadius: 6)
                            .fill(
                                darkMode
                                ? Color("AlwaysDarkSecondaryBackground") : Color("AlwaysLightSecondaryBackground")
                            )
                            .matchedGeometryEffect(id: "TAB", in: animation)
                    }
                }
                .contentShape(Rectangle())
                .onTapGesture {
                    if repeatType == (stringArray.firstIndex(of: string) ?? 0) && repeatCoefficient == 1 {
                        showMenu = false
                    } else {
                        withAnimation(.easeIn(duration: 0.15)) {
                            repeatType = (stringArray.firstIndex(of: string) ?? 0)
                            repeatCoefficient = 1
                        }

                        DispatchQueue.main.asyncAfter(deadline: .now() + 0.4) {
                            showMenu = false
                        }
                    }
                }
            }

            HStack {
                if repeatCoefficient == 1 {
                    Text("custom")
                } else {
                    if repeatType == 1 {
                        Text(String(repeatCoefficient) + " " + String(localized: "\(repeatCoefficient) days"))
                    } else if repeatType == 2 {
                        Text(String(repeatCoefficient) + " " + String(localized: "\(repeatCoefficient) weeks"))
                    } else if repeatType == 3 {
                        Text(String(repeatCoefficient) + " " + String(localized: "\(repeatCoefficient) months"))
                    }
                }

                Spacer()

                if repeatCoefficient > 1 {
                    Image(systemName: "checkmark")
                        .font(Font.satoshi(.footnote, weight: .medium))
                        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                    //                        .font(Font.satoshi(14, weight: .medium))
                }
            }
            .lineLimit(1)
            .frame(maxWidth: .infinity, alignment: .leading)
            .font(Font.satoshi(.body, weight: .medium))
            .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
            //            .font(Font.satoshi(18, weight: .medium))
            .padding(5)
            .background {
                if repeatCoefficient > 1 {
                    RoundedRectangle(cornerRadius: 6)
                        .fill(
                            darkMode
                            ? Color("AlwaysDarkSecondaryBackground") : Color("AlwaysLightSecondaryBackground")
                        )
                        .matchedGeometryEffect(id: "TAB", in: animation)
                }
            }
            .contentShape(Rectangle())
            .onTapGesture {
                if repeatCoefficient == 1 {
                    repeatCoefficient = 2
                    repeatType = 2

                    holdingType = repeatType
                    holdingCoefficient = repeatCoefficient
                }

                withAnimation {
                    showPicker = true
                }
            }
            .onChange(of: showPicker) { newValue in
                // holdingType != repeatType || holdingCoefficient != repeatCoefficient
                if !newValue {
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.4) {
                        showMenu = false
                    }
                }
            }
        }
        .foregroundColor(darkMode ? Color("AlwaysLightBackground") : Color("AlwaysDarkBackground"))
        .padding(4)
        .frame(width: 150)
        .background(
            RoundedRectangle(cornerRadius: 9).fill(
                darkMode ? Color("AlwaysDarkBackground") : Color("AlwaysLightBackground")
            ).shadow(color: darkMode ? Color.clear : Color.gray.opacity(0.25), radius: 6)
        )
        .overlay(
            RoundedRectangle(cornerRadius: 9).stroke(
                darkMode ? Color.gray.opacity(0.1) : Color.clear, lineWidth: 1.3))
    }
}

struct CustomRecurringView: View {
    @Binding var repeatType: Int
    @Binding var repeatCoefficient: Int
    @Binding var showPicker: Bool

    @Environment(\.colorScheme) var systemColorScheme
    var stringArray: [String] {
        return [
            "", "\(holdingCoefficient) days", "\(holdingCoefficient) weeks",
            "\(holdingCoefficient) months"
        ]
    }

    @Environment(\.dismiss) var dismiss

    @State var holdingType = 0
    @State var holdingCoefficient = 0

    var body: some View {
        VStack(spacing: 35) {
            HStack {
                Button {
                    dismiss()
                } label: {
                    Image(systemName: "xmark")
                        .font(Font.satoshi(.subheadline, weight: .semibold))
                        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                    //                        .font(Font.satoshi(16, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                        .padding(7)
                        .background(Color.SecondaryBackground, in: Circle())
                        .contentShape(Circle())
                }

                Spacer()

                Text("Custom Interval")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                //                    .font(Font.satoshi(18, weight: .semibold))

                Spacer()

                Button {
                    repeatType = holdingType
                    repeatCoefficient = holdingCoefficient

                    UIImpactFeedbackGenerator(style: .light).impactOccurred()

                    dismiss()

                } label: {
                    Image(systemName: "checkmark")
                        .font(Font.satoshi(.subheadline, weight: .semibold))
                        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)
                    //                        .font(Font.satoshi(16, weight: .semibold))
                        .foregroundColor(Color.IncomeGreen)
                        .padding(7)
                        .background(Color.IncomeGreen.opacity(0.23), in: Circle())
                        .contentShape(Circle())
                }
            }

            HStack(spacing: 10) {
                Text("Repeats every")
                    .font(Font.satoshi(23, weight: .medium))
                    .foregroundColor(Color.PrimaryText)
                    .padding(.trailing, 3)

                VStack {
                    Button {
                        holdingCoefficient += 1
                    } label: {
                        Image(systemName: "chevron.up")
                            .font(Font.satoshi(16, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                            .padding(7)
                            .background(
                                Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 6, style: .continuous)
                            )
                            .contentShape(Circle())
                            .opacity(holdingCoefficient == 30 ? 0.25 : 1)
                    }
                    .disabled(holdingCoefficient == 30)

                    Text("\(holdingCoefficient)")
                        .font(Font.satoshi(23, weight: .medium))
                        .padding(7)
                        .background {
                            RoundedRectangle(cornerRadius: 9, style: .continuous)
                                .fill(Color.SecondaryBackground)
                                .frame(width: 40, height: 40)
                        }

                    Button {
                        holdingCoefficient -= 1
                    } label: {
                        Image(systemName: "chevron.down")
                            .font(Font.satoshi(16, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                            .padding(7)
                            .background(
                                Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 6, style: .continuous)
                            )
                            .contentShape(Circle())
                            .opacity(holdingCoefficient == 2 ? 0.25 : 1)
                    }
                    .disabled(holdingCoefficient == 2)
                }

                VStack {
                    Button {
                        holdingType -= 1
                    } label: {
                        Image(systemName: "chevron.up")
                            .font(Font.satoshi(16, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                            .padding(7)
                            .background(
                                Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 6, style: .continuous)
                            )
                            .contentShape(Circle())
                            .opacity(holdingType == 1 ? 0.25 : 1)
                    }
                    .disabled(holdingType == 1)

                    Group {
                        if holdingType == 1 {
                            Text("\(holdingCoefficient) days")
                        } else if holdingType == 2 {
                            Text("\(holdingCoefficient) weeks")
                        } else if holdingType == 3 {
                            Text("\(holdingCoefficient) months")
                        }
                    }
                    .font(Font.satoshi(23, weight: .medium))
                    .padding(7)
                    .background {
                        RoundedRectangle(cornerRadius: 9, style: .continuous)
                            .fill(Color.SecondaryBackground)
                            .frame(height: 40)
                    }

                    Button {
                        holdingType += 1
                    } label: {
                        Image(systemName: "chevron.down")
                            .font(Font.satoshi(16, weight: .semibold))
                            .foregroundColor(Color.SubtitleText)
                            .padding(7)
                            .background(
                                Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 6, style: .continuous)
                            )
                            .contentShape(Circle())
                            .opacity(holdingType == 3 ? 0.25 : 1)
                    }
                    .disabled(holdingType == 3)
                }
            }
            .padding(.bottom, 20)
        }
        .padding(13)
        .frame(maxHeight: .infinity, alignment: .top)
        .onAppear {
            holdingType = repeatType
            holdingCoefficient = repeatCoefficient
        }
    }
}

struct ButtonView: View {
    let number: Int
    let size: CGSize

    var body: some View {
        Text("\(number)")
            .font(Font.satoshi(34, weight: .regular))
            .frame(width: size.width * 0.3, height: size.height * 0.22)
            .background(Color.SecondaryBackground)
            .foregroundColor(Color.PrimaryText)
            .clipShape(RoundedRectangle(cornerRadius: 10))
    }
}
