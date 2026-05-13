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

    // Draw-to-redact gesture state
    @State private var drawStart:   CGPoint? = nil
    @State private var drawCurrent: CGPoint? = nil

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
                Text("Kira prepared a safer copy for analysis. Drag on the document above to hide more.")
                    .font(Font.satoshi(.footnote))
                    .foregroundStyle(.secondary)
            }

            if viewModel.hasManualBoxes {
                Button { viewModel.removeLastManualBox() } label: {
                    Label("Undo last redaction", systemImage: "arrow.uturn.backward")
                        .font(Font.satoshi(.subheadline))
                        .foregroundStyle(Color.accentColor)
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
            if pages.count > 1 {
                TabView(selection: $currentPage) {
                    ForEach(Array(pages.enumerated()), id: \.offset) { index, image in
                        documentPage(image: image, pageIndex: index).tag(index)
                    }
                }
                .tabViewStyle(.page(indexDisplayMode: .automatic))
            } else if let first = pages.first {
                documentPage(image: first, pageIndex: 0)
            }
        }
    }

    // MARK: - Single page with overlays + draw gesture

    private func documentPage(image: UIImage, pageIndex: Int) -> some View {
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
                        // Drag gesture captures geo.size at the time the gesture ends
                        .contentShape(Rectangle())
                        .gesture(
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
                                    viewModel.addManualBox(
                                        CGRect(x: nx, y: ny, width: nw, height: nh),
                                        pageIndex: pageIndex
                                    )
                                }
                        )
                    }
                )
                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                .padding(12)
        }
    }
}

// MARK: - iOS version-safe sheet modifier

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
}
