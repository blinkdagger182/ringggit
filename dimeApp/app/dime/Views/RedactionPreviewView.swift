//
//  RedactionPreviewView.swift
//  dime
//

import SwiftUI

struct RedactionPreviewView: View {
    @ObservedObject var viewModel: RedactionFlowViewModel
    let onComplete: ([UIImage]) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var currentPage  = 0
    @State private var isGenerating = false

    // Draw-to-redact
    @State private var isDrawMode  = false
    @State private var drawStart:   CGPoint? = nil
    @State private var drawCurrent: CGPoint? = nil

    // Multi-page apply dialog
    @State private var pendingBox:     CGRect? = nil
    @State private var pendingBoxPage: Int     = 0
    @State private var showApplyDialog         = false

    var body: some View {
        NavigationView {
            ZStack {
                Color.PrimaryBackground.ignoresSafeArea()

                switch viewModel.flowState {
                case .idle:    EmptyView()
                case .loading: loadingView
                case .ready:   readyView
                case .error:   errorView
                }
            }
            .navigationTitle("Privacy Review")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        viewModel.cancelFlow()
                        dismiss()
                    }
                }
            }
        }
        .navigationViewStyle(.stack)
        .confirmationDialog("Apply redaction to", isPresented: $showApplyDialog, titleVisibility: .visible) {
            Button("This page only") {
                if let box = pendingBox {
                    viewModel.addManualBox(box, pageIndex: pendingBoxPage)
                }
                pendingBox = nil
            }
            Button("All pages") {
                if let box = pendingBox {
                    for i in 0..<viewModel.sourcePages.count {
                        viewModel.addManualBox(box, pageIndex: i)
                    }
                }
                pendingBox = nil
            }
            Button("Cancel", role: .cancel) { pendingBox = nil }
        }
        .sheet(isPresented: $viewModel.showIntro) {
            PrivateRedactionIntroSheet(
                onContinue: { viewModel.markIntroSeen() },
                onDismiss: {
                    viewModel.cancelFlow()
                    dismiss()
                }
            )
            .largeSheetPresentation()
        }
    }

    // MARK: - Loading

    private var loadingView: some View {
        VStack(spacing: 16) {
            ProgressView().scaleEffect(1.2)
            Text("Protecting your document")
                .font(Font.satoshi(.headline, weight: .semibold))
            Text("Kira is hiding private details on your iPhone before analysis.")
                .font(Font.satoshi(.subheadline))
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 40)
        }
    }

    // MARK: - Error

    private var errorView: some View {
        VStack(spacing: 20) {
            Image(systemName: "exclamationmark.shield")
                .font(Font.satoshi(44, weight: .light))
                .foregroundStyle(.secondary)

            VStack(spacing: 8) {
                Text("We couldn't protect this document")
                    .font(Font.satoshi(.headline, weight: .semibold))
                    .multilineTextAlignment(.center)
                Text("Please try again with a clearer scan or upload a different file.")
                    .font(Font.satoshi(.subheadline))
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 32)
            }

            Button { viewModel.retryDetection() } label: {
                Text("Try again")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundStyle(.white)
                    .padding(.horizontal, 32)
                    .padding(.vertical, 14)
                    .background(Color.accentColor, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
            }
        }
    }

    // MARK: - Ready

    private var readyView: some View {
        VStack(spacing: 0) {
            documentSection
                .frame(maxHeight: .infinity)
                .background(Color(.secondarySystemBackground))

            Divider()

            footerSection
                .padding(.horizontal, 20)
                .padding(.vertical, 18)
        }
    }

    // MARK: - Footer

    private var footerSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            VStack(alignment: .leading, spacing: 4) {
                Text("Private details protected")
                    .font(Font.satoshi(.headline, weight: .semibold))
                Text(isDrawMode
                     ? "Drag on the document to hide an area. Tap × on a box to remove it."
                     : "Header info is protected. Tap \"Redact more\" to hide additional areas.")
                    .font(Font.satoshi(.footnote))
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 12) {
                // Draw mode toggle
                Button {
                    withAnimation(.easeInOut(duration: 0.2)) { isDrawMode.toggle() }
                    if !isDrawMode { drawStart = nil; drawCurrent = nil }
                } label: {
                    Label(isDrawMode ? "Done" : "Redact more",
                          systemImage: isDrawMode ? "checkmark" : "hand.draw")
                        .font(Font.satoshi(.subheadline, weight: .medium))
                        .foregroundStyle(isDrawMode ? Color.white : Color.accentColor)
                        .padding(.horizontal, 14)
                        .padding(.vertical, 8)
                        .background(
                            isDrawMode ? Color.accentColor : Color.accentColor.opacity(0.1),
                            in: Capsule()
                        )
                }

                // Multi-page indicator when in draw mode
                if isDrawMode && viewModel.sourcePages.count > 1 {
                    HStack(spacing: 6) {
                        Button {
                            if currentPage > 0 { currentPage -= 1 }
                        } label: {
                            Image(systemName: "chevron.left")
                                .font(.system(size: 13, weight: .medium))
                        }
                        .disabled(currentPage == 0)

                        Text("Page \(currentPage + 1) of \(viewModel.sourcePages.count)")
                            .font(Font.satoshi(.caption))
                            .foregroundStyle(.secondary)

                        Button {
                            if currentPage < viewModel.sourcePages.count - 1 { currentPage += 1 }
                        } label: {
                            Image(systemName: "chevron.right")
                                .font(.system(size: 13, weight: .medium))
                        }
                        .disabled(currentPage == viewModel.sourcePages.count - 1)
                    }
                    .foregroundStyle(Color.primary)
                }

                Spacer()

                if viewModel.hasManualBoxes {
                    Button { viewModel.removeLastManualBox() } label: {
                        Image(systemName: "arrow.uturn.backward")
                            .font(.system(size: 14))
                            .foregroundStyle(Color.secondary)
                    }
                }
            }

            Button {
                guard !isGenerating else { return }
                isGenerating = true
                Task {
                    let redacted = await viewModel.generateRedactedCopy()
                    viewModel.showFlow = false
                    onComplete(redacted)
                }
            } label: {
                Group {
                    if isGenerating {
                        HStack(spacing: 10) {
                            ProgressView().tint(.white)
                            Text("Applying redactions…")
                                .font(Font.satoshi(.body, weight: .semibold))
                        }
                    } else {
                        Text("Use this copy")
                            .font(Font.satoshi(.body, weight: .semibold))
                    }
                }
                .foregroundStyle(.white)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 16)
                .background(
                    isGenerating ? Color.accentColor.opacity(0.7) : Color.accentColor,
                    in: RoundedRectangle(cornerRadius: 16, style: .continuous)
                )
            }
            .disabled(isGenerating)
        }
    }

    // MARK: - Document section

    private var documentSection: some View {
        let pages = viewModel.sourcePages
        return Group {
            // In draw mode (or single page) show a static view so the drag gesture
            // doesn't compete with TabView's horizontal swipe.
            if pages.count > 1 && !isDrawMode {
                TabView(selection: $currentPage) {
                    ForEach(Array(pages.enumerated()), id: \.offset) { index, image in
                        documentPage(image: image, pageIndex: index, drawEnabled: false).tag(index)
                    }
                }
                .tabViewStyle(.page(indexDisplayMode: .automatic))
            } else if let image = pages.indices.contains(currentPage) ? pages[currentPage] : pages.first {
                documentPage(image: image, pageIndex: currentPage, drawEnabled: true)
            }
        }
    }

    // MARK: - Single page with overlays + optional draw gesture

    private func documentPage(image: UIImage, pageIndex: Int, drawEnabled: Bool) -> some View {
        let autoItems   = viewModel.redactionItems.filter { $0.pageIndex == pageIndex }
        let manualItems = viewModel.manualBoxes.filter    { $0.pageIndex == pageIndex }
        let ratio = image.size.height > 0 ? image.size.width / image.size.height : 1.0

        return ZStack {
            Color(.secondarySystemBackground)

            Color.clear
                .aspectRatio(ratio, contentMode: .fit)
                .overlay(
                    Image(uiImage: image)
                        .resizable()
                        .scaledToFill()
                )
                .overlay(
                    GeometryReader { geo in
                        ZStack {
                            // Auto-detected PII boxes (header zone only)
                            ForEach(autoItems) { item in
                                let b = item.boundingBox
                                Rectangle()
                                    .fill(Color.black)
                                    .frame(
                                        width:  max(b.width  * geo.size.width,  24),
                                        height: max(b.height * geo.size.height, 10)
                                    )
                                    .position(
                                        x: b.midX * geo.size.width,
                                        y: b.midY * geo.size.height
                                    )
                            }

                            // Manually drawn boxes — tap × to remove
                            ForEach(manualItems) { box in
                                let b = box.boundingBox
                                let w = max(b.width  * geo.size.width,  20)
                                let h = max(b.height * geo.size.height, 10)
                                ZStack(alignment: .topTrailing) {
                                    Rectangle().fill(Color.black)
                                    Image(systemName: "xmark.circle.fill")
                                        .font(.system(size: 13, weight: .medium))
                                        .foregroundStyle(.white.opacity(0.85))
                                        .padding(2)
                                }
                                .frame(width: w, height: h)
                                .position(
                                    x: b.midX * geo.size.width,
                                    y: b.midY * geo.size.height
                                )
                                .onTapGesture { viewModel.removeManualBox(box.id) }
                            }

                            // Live draw preview
                            if let start = drawStart, let current = drawCurrent,
                               currentPage == pageIndex {
                                let x = min(start.x, current.x)
                                let y = min(start.y, current.y)
                                let w = abs(current.x - start.x)
                                let h = abs(current.y - start.y)
                                Rectangle()
                                    .fill(Color.black.opacity(0.4))
                                    .overlay(
                                        Rectangle()
                                            .strokeBorder(Color.white.opacity(0.6), lineWidth: 1)
                                    )
                                    .frame(width: max(w, 1), height: max(h, 1))
                                    .position(x: x + w / 2, y: y + h / 2)
                                    .allowsHitTesting(false)
                            }
                        }
                        // Draw gesture — only attached when drawEnabled to avoid
                        // conflicting with TabView's horizontal page swipe.
                        .contentShape(Rectangle())
                        .if(drawEnabled) { content in
                            content.gesture(
                                DragGesture(minimumDistance: 8)
                                    .onChanged { value in
                                        if drawStart == nil { drawStart = value.startLocation }
                                        drawCurrent = value.location
                                    }
                                    .onEnded { value in
                                        defer { drawStart = nil; drawCurrent = nil }
                                        guard let start = drawStart,
                                              geo.size.width > 0, geo.size.height > 0 else { return }
                                        let end = value.location

                                        let nx = max(0, min(1, min(start.x, end.x) / geo.size.width))
                                        let ny = max(0, min(1, min(start.y, end.y) / geo.size.height))
                                        let nw = max(0, min(1 - nx, abs(end.x - start.x) / geo.size.width))
                                        let nh = max(0, min(1 - ny, abs(end.y - start.y) / geo.size.height))

                                        guard nw > 0.02, nh > 0.005 else { return }
                                        let rect = CGRect(x: nx, y: ny, width: nw, height: nh)
                                        if viewModel.sourcePages.count > 1 {
                                            pendingBox      = rect
                                            pendingBoxPage  = pageIndex
                                            showApplyDialog = true
                                        } else {
                                            viewModel.addManualBox(rect, pageIndex: pageIndex)
                                        }
                                    }
                            )
                        }
                    }
                )
                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                .padding(12)
        }
    }
}

// MARK: - View helpers

private extension View {
    @ViewBuilder
    func largeSheetPresentation() -> some View {
        if #available(iOS 16, *) {
            self
                .presentationDetents([.large])
                .presentationDragIndicator(.visible)
        } else {
            self
        }
    }

    @ViewBuilder
    func `if`<Content: View>(_ condition: Bool, transform: (Self) -> Content) -> some View {
        if condition { transform(self) } else { self }
    }
}
