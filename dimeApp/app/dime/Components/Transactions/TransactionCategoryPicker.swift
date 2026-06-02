//
//  TransactionCategoryPicker.swift
//  dime
//
//  Created by Rafael Soh on 20/11/23.
//

import Foundation
import CoreData
import SwiftUI

struct NewCategoryPickerView: View {
    @Binding var category: Category?
    @Binding var showPicker: Bool
    @Binding var showingCategoryView: Bool
    @FetchRequest private var categories: FetchedResults<Category>
    @Environment(\.colorScheme) var colorScheme
    @State private var searchText = ""

    let layout = [
        GridItem(.flexible(), spacing: 10),
        GridItem(.flexible(), spacing: 10),
        GridItem(.flexible(), spacing: 10)
    ]

    private var filteredCategories: [Category] {
        let source = Array(categories)
        guard !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return source }
        return source.filter {
            $0.wrappedName.localizedCaseInsensitiveContains(searchText)
                || $0.wrappedEmoji.localizedCaseInsensitiveContains(searchText)
        }
    }

    private var groupedCategories: [(String, String, [Category])] {
        let sections = [
            ("Everyday Spending", "fork.knife", ["grocery", "groceries", "restaurant", "food", "delivery", "coffee", "meal", "drink"]),
            ("Transport", "car.fill", ["transport", "fuel", "taxi", "grab", "ride", "parking", "car", "bus", "train"]),
            ("Living", "house.fill", ["rent", "utilities", "internet", "mobile", "home", "electric", "water"]),
            ("Bills", "doc.text.fill", ["subscription", "insurance", "loan", "bill"]),
            ("Shopping", "bag.fill", ["shopping", "shopee", "clothing", "electronics"]),
            ("Travel", "airplane", ["flight", "hotel", "travel", "trip", "activity"]),
            ("Other", "square.grid.2x2.fill", [])
        ]

        var assigned = Set<NSManagedObjectID>()
        var output: [(String, String, [Category])] = []

        for section in sections.dropLast() {
            let matches = filteredCategories.filter { category in
                let name = category.wrappedName.lowercased()
                return section.2.contains { name.contains($0) }
            }
            if !matches.isEmpty {
                matches.forEach { assigned.insert($0.objectID) }
                output.append((section.0, section.1, matches))
            }
        }

        let remaining = filteredCategories.filter { !assigned.contains($0.objectID) }
        if !remaining.isEmpty {
            output.append(("Other", "square.grid.2x2.fill", remaining))
        }

        return output
    }

    var body: some View {
        VStack(spacing: 12) {
            HStack {
                Button {
                    withAnimation(.easeInOut) {
                        showPicker = false
                    }
                } label: {
                    Image(systemName: "chevron.left")
                        .font(Font.satoshi(.body, weight: .bold))
                        .foregroundColor(Color.PrimaryText)
                        .frame(width: 38, height: 38)
                        .background(Color.SecondaryBackground, in: Circle())
                }
                .buttonStyle(.plain)

                Spacer()

                Text("Select category")
                    .font(Font.satoshi(.headline, weight: .bold))
                    .foregroundColor(Color.PrimaryText)

                Spacer()

                Button {
                    let impactMed = UIImpactFeedbackGenerator(style: .light)
                    impactMed.impactOccurred()
                    showPicker = false
                    showingCategoryView = true
                } label: {
                    Image(systemName: "list.bullet")
                        .font(Font.satoshi(.body, weight: .bold))
                        .foregroundColor(Color.PrimaryText)
                        .frame(width: 38, height: 38)
                        .background(Color.SecondaryBackground, in: Circle())
                }
                .buttonStyle(.plain)
            }

            HStack(spacing: 10) {
                Image(systemName: "magnifyingglass")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundColor(Color.SubtitleText)
                TextField("Search category", text: $searchText)
                    .font(Font.satoshi(.body, weight: .medium))
                    .foregroundColor(Color.PrimaryText)
            }
            .padding(.horizontal, 14)
            .frame(height: 46)
            .background(Color.SecondaryBackground, in: Capsule())
            .overlay(Capsule().stroke(Color.Outline.opacity(0.75), lineWidth: 1.2))

            if filteredCategories.isEmpty {
                VStack(spacing: 8) {
                    Image(systemName: "magnifyingglass")
                        .font(Font.satoshi(24, weight: .semibold))
                        .foregroundColor(Color.SubtitleText)
                    Text("No matching category")
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(Color.PrimaryText)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                ScrollView(showsIndicators: false) {
                    VStack(alignment: .leading, spacing: 22) {
                        kiraHint

                        ForEach(groupedCategories, id: \.0) { section in
                            VStack(alignment: .leading, spacing: 10) {
                                Label(section.0, systemImage: section.1)
                                    .font(Font.satoshi(.headline, weight: .bold))
                                    .foregroundColor(Color.SubtitleText)

                                LazyVGrid(columns: layout, spacing: 12) {
                                    ForEach(section.2) { item in
                                        categoryTile(item)
                                    }
                                }
                                .padding(12)
                                .background(Color.SecondaryBackground, in: RoundedRectangle(cornerRadius: 22, style: .continuous))
                            }
                        }
                    }
                    .padding(.bottom, 72)
                }
            }
        }
        .padding(.horizontal, 2)
        .dynamicTypeSize(...DynamicTypeSize.xxxLarge)

    }

    @ViewBuilder
    private var kiraHint: some View {
        HStack(spacing: 8) {
            Image(systemName: "sparkles")
                .font(Font.satoshi(.footnote, weight: .semibold))
            Text("Add a note and KIRA can suggest a category.")
                .font(Font.satoshi(.footnote, weight: .semibold))
                .lineLimit(2)
        }
        .foregroundColor(Color(hex: "4A5240"))
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(Color(hex: "4A5240").opacity(0.10), in: RoundedRectangle(cornerRadius: 13, style: .continuous))
    }

    @ViewBuilder
    private func categoryTile(_ item: Category) -> some View {
        Button {
            withAnimation {
                category = item
                showPicker = false
            }
        } label: {
            VStack(spacing: 8) {
                Text(item.wrappedEmoji)
                    .font(Font.satoshi(30, weight: .semibold))
                    .frame(width: 64, height: 64)
                    .background(Color(hex: item.wrappedColour).opacity(0.88), in: Circle())

                Text(item.wrappedName)
                    .font(Font.satoshi(.callout, weight: .semibold))
                    .foregroundColor(Color.PrimaryText)
                    .multilineTextAlignment(.center)
                    .lineLimit(2)
                    .minimumScaleFactor(0.82)
                    .frame(minHeight: 38, alignment: .top)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 10)
            .overlay {
                if item == category {
                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                        .strokeBorder(Color(hex: item.wrappedColour), lineWidth: 2)
                }
            }
        }
        .buttonStyle(.plain)
        .opacity(category != nil ? (category == item ? 1 : 0.58) : 1)
    }

    init(
        category: Binding<Category?>?, showPicker: Binding<Bool>, showSheet: Binding<Bool>,
        income: Bool
    ) {
        _categories = FetchRequest<Category>(
            sortDescriptors: [
                SortDescriptor(\.order, order: .reverse)
            ], predicate: NSPredicate(format: "income = %d", income))

        _category = category ?? Binding.constant(nil)
        _showPicker = showPicker
        _showingCategoryView = showSheet
    }
}
