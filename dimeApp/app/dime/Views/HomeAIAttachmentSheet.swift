//
//  HomeAIAttachmentSheet.swift
//  dime
//

import SwiftUI
import Photos
import PhotosUI
import PDFKit
import UniformTypeIdentifiers
@preconcurrency import Vision
import VisionKit

struct HomeAIAttachmentSheet: View {
    private struct ScannedDocumentDraft: Identifiable {
        let id = UUID()
        let pages: [UIImage]
    }

    @Binding var attachments: [AttachmentItem]
    let onQuickPrompt: (String) -> Void
    @Environment(\.dismiss) private var dismiss

    @State private var recentAssets: [PHAsset] = []
    @State private var assetToAttachmentID: [String: UUID] = [:]
    @State private var authStatus: PHAuthorizationStatus = .notDetermined
    @State private var showFullPicker = false
    @State private var showDocumentPicker = false
    @State private var showDocumentScanner = false
    @State private var scannedDocumentDraft: ScannedDocumentDraft?

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Handle
            Capsule()
                .fill(Color(.systemGray4))
                .frame(width: 36, height: 5)
                .frame(maxWidth: .infinity)
                .padding(.top, 10)
                .padding(.bottom, 18)

            // Photos header
            HStack(alignment: .firstTextBaseline) {
                Text("Photos")
                    .font(Font.satoshi(.headline, weight: .semibold))
                    .foregroundColor(.primary)
                Spacer()
                Button("All Photos") { showFullPicker = true }
                    .font(Font.satoshi(.subheadline))
                    .foregroundColor(.blue)
            }
            .padding(.horizontal, 20)
            .padding(.bottom, 12)

            // Horizontal photo strip
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    // Camera placeholder cell
                    ZStack {
                        RoundedRectangle(cornerRadius: 12, style: .continuous)
                            .fill(Color(.systemGray5))
                        Image(systemName: "camera.fill")
                            .font(Font.satoshi(22, weight: .medium))
                            .foregroundColor(Color(.secondaryLabel))
                    }
                    .frame(width: 90, height: 90)

                    if authStatus == .authorized || authStatus == .limited {
                        ForEach(recentAssets, id: \.localIdentifier) { asset in
                            AssetThumbnailCell(
                                asset: asset,
                                isSelected: assetToAttachmentID[asset.localIdentifier] != nil
                            ) {
                                toggleAsset(asset)
                            }
                        }
                    } else {
                        Button { requestPhotoAccess() } label: {
                            ZStack {
                                RoundedRectangle(cornerRadius: 12, style: .continuous)
                                    .fill(Color(.systemGray5))
                                VStack(spacing: 6) {
                                    Image(systemName: "photo.on.rectangle.angled")
                                        .font(Font.satoshi(20))
                                        .foregroundColor(Color(.secondaryLabel))
                                    Text(authStatus == .denied ? "Allow Access" : "Enable Photos")
                                        .font(Font.satoshi(11, weight: .medium))
                                        .foregroundColor(Color(.secondaryLabel))
                                }
                            }
                            .frame(width: 90, height: 90)
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.horizontal, 20)
            }
            .frame(height: 90)

            Divider()
                .padding(.top, 18)
                .padding(.bottom, 4)

            // Action rows
            AttachmentActionRow(
                icon: "doc.text.viewfinder",
                iconColor: .teal,
                title: "Scan Receipt or Document",
                subtitle: "Scan pages, preview, then attach"
            ) {
                showDocumentScanner = true
            }

            AttachmentActionRow(
                icon: "doc.fill",
                iconColor: .orange,
                title: "Upload PDF",
                subtitle: "Bank statement, invoice — AI reads it"
            ) {
                showDocumentPicker = true
            }

            AttachmentActionRow(
                icon: "chart.pie.fill",
                iconColor: .purple,
                title: "Where did my money go?",
                subtitle: "Breakdown by category"
            ) {
                onQuickPrompt("Where did my money go?")
                dismiss()
            }

            AttachmentActionRow(
                icon: "calendar",
                iconColor: .blue,
                title: "Show this month's spending",
                subtitle: "Summary of all transactions this month"
            ) {
                onQuickPrompt("Show this month's spending")
                dismiss()
            }

            AttachmentActionRow(
                icon: "checkmark.shield.fill",
                iconColor: .green,
                title: "Can I afford this?",
                subtitle: "Check against your current budget"
            ) {
                onQuickPrompt("Can I afford this?")
                dismiss()
            }

            Spacer(minLength: 0)
        }
        .onAppear { checkPhotoAccess() }
        .sheet(isPresented: $showFullPicker) {
            FullPhotoPickerView { images in
                for img in images {
                    attachments.append(AttachmentItem(image: img, label: "Photo"))
                }
                dismiss()
            }
        }
        .sheet(isPresented: $showDocumentPicker) {
            PDFDocumentPickerView { pages, filename, sourceText in
                guard !pages.isEmpty else { return }
                attachments.append(AttachmentItem(pdfPages: pages, filename: filename, sourceText: sourceText))
                dismiss()
            }
        }
        .fullScreenCover(isPresented: $showDocumentScanner) {
            DocumentScannerView { pages in
                guard !pages.isEmpty else { return }
                scannedDocumentDraft = ScannedDocumentDraft(pages: pages)
            }
            .ignoresSafeArea()
        }
        .fullScreenCover(item: $scannedDocumentDraft) { draft in
            ScannedDocumentPreviewView(pages: draft.pages) { pages, sourceText in
                let filename = pages.count == 1 ? "Scanned Receipt" : "Scanned Document"
                attachments.append(AttachmentItem(pdfPages: pages, filename: filename, sourceText: sourceText))
                dismiss()
            }
        }
    }

    private func checkPhotoAccess() {
        authStatus = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        if authStatus == .authorized || authStatus == .limited {
            loadRecentPhotos()
        }
    }

    private func requestPhotoAccess() {
        PHPhotoLibrary.requestAuthorization(for: .readWrite) { status in
            DispatchQueue.main.async {
                authStatus = status
                if status == .authorized || status == .limited {
                    loadRecentPhotos()
                }
            }
        }
    }

    private func loadRecentPhotos() {
        let options = PHFetchOptions()
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
        options.fetchLimit = 24
        let result = PHAsset.fetchAssets(with: .image, options: options)
        var assets: [PHAsset] = []
        result.enumerateObjects { asset, _, _ in assets.append(asset) }
        recentAssets = assets
    }

    private func toggleAsset(_ asset: PHAsset) {
        let assetID = asset.localIdentifier
        if let existingID = assetToAttachmentID[assetID] {
            assetToAttachmentID.removeValue(forKey: assetID)
            attachments.removeAll { $0.id == existingID }
        } else {
            let options = PHImageRequestOptions()
            options.deliveryMode = .highQualityFormat
            options.isNetworkAccessAllowed = true

            PHImageManager.default().requestImage(
                for: asset,
                targetSize: CGSize(width: 1400, height: 1400),
                contentMode: .aspectFit,
                options: options
            ) { image, _ in
                guard let image else { return }
                DispatchQueue.main.async {
                    let item = AttachmentItem(image: image, label: "Photo")
                    assetToAttachmentID[assetID] = item.id
                    attachments.append(item)
                }
            }
        }
    }
}

// MARK: - Asset Thumbnail Cell

private struct AssetThumbnailCell: View {
    let asset: PHAsset
    let isSelected: Bool
    let onTap: () -> Void

    @State private var thumbnail: UIImage?

    var body: some View {
        Button(action: onTap) {
            ZStack(alignment: .topTrailing) {
                Group {
                    if let thumbnail {
                        Image(uiImage: thumbnail)
                            .resizable()
                            .scaledToFill()
                    } else {
                        Color(.systemGray5)
                    }
                }
                .frame(width: 90, height: 90)
                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))

                if isSelected {
                    ZStack {
                        Circle()
                            .fill(Color.blue)
                            .frame(width: 22, height: 22)
                        Image(systemName: "checkmark")
                            .font(Font.satoshi(11, weight: .bold))
                            .foregroundColor(.white)
                    }
                    .padding(5)
                }
            }
        }
        .buttonStyle(.plain)
        .onAppear { loadThumbnail() }
    }

    private func loadThumbnail() {
        PHImageManager.default().requestImage(
            for: asset,
            targetSize: CGSize(width: 180, height: 180),
            contentMode: .aspectFill,
            options: nil
        ) { image, _ in
            thumbnail = image
        }
    }
}

// MARK: - Action Row

struct AttachmentActionRow: View {
    let icon: String
    let iconColor: Color
    let title: String
    let subtitle: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            HStack(spacing: 14) {
                ZStack {
                    RoundedRectangle(cornerRadius: 10, style: .continuous)
                        .fill(iconColor.opacity(0.14))
                        .frame(width: 40, height: 40)
                    Image(systemName: icon)
                        .font(Font.satoshi(16, weight: .medium))
                        .foregroundColor(iconColor)
                }

                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .font(Font.satoshi(.body, weight: .medium))
                        .foregroundColor(.primary)
                    Text(subtitle)
                        .font(Font.satoshi(.footnote))
                        .foregroundColor(.secondary)
                }

                Spacer()

                Image(systemName: "chevron.right")
                    .font(Font.satoshi(12, weight: .semibold))
                    .foregroundColor(Color(.tertiaryLabel))
            }
            .padding(.horizontal, 20)
            .padding(.vertical, 11)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }
}

// MARK: - Full Photo Picker (PHPickerViewController)

struct FullPhotoPickerView: UIViewControllerRepresentable {
    let onComplete: ([UIImage]) -> Void

    func makeUIViewController(context: Context) -> PHPickerViewController {
        var config = PHPickerConfiguration()
        config.selectionLimit = 10
        config.filter = .images
        let picker = PHPickerViewController(configuration: config)
        picker.delegate = context.coordinator
        return picker
    }

    func updateUIViewController(_ uiViewController: PHPickerViewController, context: Context) {}

    func makeCoordinator() -> Coordinator { Coordinator(onComplete: onComplete) }

    final class Coordinator: NSObject, PHPickerViewControllerDelegate {
        let onComplete: ([UIImage]) -> Void
        init(onComplete: @escaping ([UIImage]) -> Void) { self.onComplete = onComplete }

        func picker(_ picker: PHPickerViewController, didFinishPicking results: [PHPickerResult]) {
            picker.dismiss(animated: true)
            guard !results.isEmpty else { onComplete([]); return }

            var images: [UIImage] = []
            let group = DispatchGroup()

            for result in results {
                group.enter()
                result.itemProvider.loadObject(ofClass: UIImage.self) { object, _ in
                    if let image = object as? UIImage { images.append(image) }
                    group.leave()
                }
            }

            group.notify(queue: .main) { self.onComplete(images) }
        }
    }
}

// MARK: - PDF Document Picker

struct PDFDocumentPickerView: UIViewControllerRepresentable {
    let onComplete: ([UIImage], String, String?) -> Void

    func makeUIViewController(context: Context) -> UIDocumentPickerViewController {
        let picker = UIDocumentPickerViewController(forOpeningContentTypes: [UTType.pdf])
        picker.delegate = context.coordinator
        picker.allowsMultipleSelection = false
        picker.modalPresentationStyle = .fullScreen
        return picker
    }

    func updateUIViewController(_ uiViewController: UIDocumentPickerViewController, context: Context) {}

    func makeCoordinator() -> Coordinator { Coordinator(onComplete: onComplete) }

    final class Coordinator: NSObject, UIDocumentPickerDelegate {
        let onComplete: ([UIImage], String, String?) -> Void
        init(onComplete: @escaping ([UIImage], String, String?) -> Void) { self.onComplete = onComplete }

        func documentPicker(_ controller: UIDocumentPickerViewController, didPickDocumentsAt urls: [URL]) {
            guard let url = urls.first else { return }
            _ = url.startAccessingSecurityScopedResource()
            defer { url.stopAccessingSecurityScopedResource() }

            let filename = url.deletingPathExtension().lastPathComponent
            let result = renderPDFPagesAndExtractText(url: url)
            DispatchQueue.main.async { self.onComplete(result.images, filename, result.text) }
        }

        private func renderPDFPagesAndExtractText(url: URL) -> (images: [UIImage], text: String?) {
            guard let document = PDFDocument(url: url) else { return ([], nil) }
            var images: [UIImage] = []
            var pageTextBlocks: [String] = []
            let pageCount = min(document.pageCount, 10)

            for i in 0..<pageCount {
                guard let page = document.page(at: i) else { continue }
                let pageText = layoutPreservingText(for: page)
                if !pageText.isEmpty {
                    pageTextBlocks.append("Page \(i + 1):\n\(pageText)")
                }

                let mediaRect = page.bounds(for: .mediaBox)
                let scale: CGFloat = min(1600 / mediaRect.width, 2200 / mediaRect.height)
                let targetSize = CGSize(
                    width: mediaRect.width * scale,
                    height: mediaRect.height * scale
                )
                images.append(page.thumbnail(of: targetSize, for: .mediaBox))
            }

            let text: String?
            if !pageTextBlocks.isEmpty {
                text = pageTextBlocks.joined(separator: "\n\n")
            } else {
                text = ScannedDocumentOCR.extractTextSync(from: images)
            }
            return (images, text)
        }

        private func layoutPreservingText(for page: PDFPage) -> String {
            let pageBounds = page.bounds(for: .mediaBox)
            let rowCount = 52
            let rowHeight = pageBounds.height / CGFloat(rowCount)
            var rows: [String] = []

            for row in 0..<rowCount {
                let y = pageBounds.minY + CGFloat(row) * rowHeight
                let rect = CGRect(
                    x: pageBounds.minX,
                    y: y,
                    width: pageBounds.width,
                    height: rowHeight
                )
                guard let text = page.selection(for: rect)?.string else { continue }
                let normalized = text
                    .components(separatedBy: .newlines)
                    .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                    .filter { !$0.isEmpty }
                    .joined(separator: " | ")
                    .trimmingCharacters(in: .whitespacesAndNewlines)
                guard !normalized.isEmpty else { continue }
                rows.append(normalized)
            }

            return rows.joined(separator: "\n")
        }
    }
}

// MARK: - Document Scanner

struct DocumentScannerView: UIViewControllerRepresentable {
    let onComplete: ([UIImage]) -> Void
    @Environment(\.dismiss) private var dismiss

    func makeCoordinator() -> Coordinator {
        Coordinator(dismiss: dismiss, onComplete: onComplete)
    }

    func makeUIViewController(context: Context) -> VNDocumentCameraViewController {
        let controller = VNDocumentCameraViewController()
        controller.delegate = context.coordinator
        controller.modalPresentationStyle = .fullScreen
        return controller
    }

    func updateUIViewController(_ uiViewController: VNDocumentCameraViewController, context: Context) {}

    final class Coordinator: NSObject, VNDocumentCameraViewControllerDelegate {
        let dismiss: DismissAction
        let onComplete: ([UIImage]) -> Void

        init(dismiss: DismissAction, onComplete: @escaping ([UIImage]) -> Void) {
            self.dismiss = dismiss
            self.onComplete = onComplete
        }

        func documentCameraViewControllerDidCancel(_ controller: VNDocumentCameraViewController) {
            dismiss()
        }

        func documentCameraViewController(_ controller: VNDocumentCameraViewController, didFailWithError error: Error) {
            dismiss()
        }

        func documentCameraViewController(_ controller: VNDocumentCameraViewController, didFinishWith scan: VNDocumentCameraScan) {
            var pages: [UIImage] = []
            for index in 0..<scan.pageCount {
                pages.append(scan.imageOfPage(at: index))
            }
            dismiss()
            onComplete(pages)
        }
    }
}

// MARK: - Scan Preview

private struct ScannedDocumentPreviewView: View {
    let pages: [UIImage]
    let onConfirm: ([UIImage], String?) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var currentPage = 0
    @State private var recognizedText: String?
    @State private var isRecognizingText = false

    var body: some View {
        NavigationView {
            ZStack {
                Color.PrimaryBackground
                    .ignoresSafeArea()

                VStack(spacing: 16) {
                    TabView(selection: $currentPage) {
                        ForEach(Array(pages.enumerated()), id: \.offset) { index, image in
                            GeometryReader { proxy in
                                ZStack {
                                    RoundedRectangle(cornerRadius: 24, style: .continuous)
                                        .fill(Color(.secondarySystemBackground))

                                    Image(uiImage: image)
                                        .resizable()
                                        .scaledToFit()
                                        .frame(
                                            width: proxy.size.width - 24,
                                            height: proxy.size.height - 24
                                        )
                                }
                                .padding(.horizontal, 20)
                                .padding(.vertical, 4)
                            }
                            .tag(index)
                        }
                    }
                    .tabViewStyle(.page(indexDisplayMode: .never))
                    .frame(height: 360)

                    HStack {
                        Text("\(pages.count) \(pages.count == 1 ? "page" : "pages") scanned")
                            .font(Font.satoshi(.subheadline, weight: .medium))
                            .foregroundColor(.primary)

                        Spacer()

                        if pages.count > 1 {
                            Text("Page \(currentPage + 1) of \(pages.count)")
                                .font(Font.satoshi(.footnote))
                                .foregroundColor(.secondary)
                        }
                    }
                    .padding(.horizontal, 20)

#if DEBUG
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Text extraction")
                            .font(Font.satoshi(.headline, weight: .semibold))

                        if isRecognizingText {
                            HStack(spacing: 10) {
                                ProgressView()
                                Text("Reading scanned text...")
                                    .font(Font.satoshi(.subheadline))
                                    .foregroundColor(.secondary)
                            }
                        } else if let recognizedText, !recognizedText.isEmpty {
                            ScrollView {
                                Text(recognizedText)
                                    .font(.system(.footnote, design: .monospaced))
                                    .foregroundColor(.secondary)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                            }
                            .frame(maxHeight: 140)
                            .padding(12)
                            .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 16, style: .continuous))
                        } else {
                            Text("No text detected. You can still attach the scan and let the AI read the images directly.")
                                .font(Font.satoshi(.subheadline))
                                .foregroundColor(.secondary)
                        }
                    }
                    .padding(.horizontal, 20)
#endif

                    Spacer(minLength: 0)

                    HStack(spacing: 12) {
                        Button("Retake") {
                            dismiss()
                        }
                        .font(Font.satoshi(.body, weight: .medium))
                        .foregroundColor(.primary)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 14)
                        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 16, style: .continuous))

                        Button("Attach Scan") {
                            onConfirm(pages, recognizedText)
                        }
                        .font(Font.satoshi(.body, weight: .semibold))
                        .foregroundColor(.white)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 14)
                        .background(Color.accentColor, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
                    }
                    .padding(.horizontal, 20)
                    .padding(.bottom, 8)
                }
            }
            .navigationTitle("Preview Scan")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") { dismiss() }
                }
            }
        }
        .navigationViewStyle(.stack)
        .task {
            guard recognizedText == nil, !isRecognizingText else { return }
            isRecognizingText = true
            recognizedText = await ScannedDocumentOCR.extractText(from: pages)
            isRecognizingText = false
        }
    }
}

// MARK: - OCR

private enum ScannedDocumentOCR {
    static func extractTextSync(from pages: [UIImage]) -> String? {
        let semaphore = DispatchSemaphore(value: 0)
        var output: String?

        Task {
            output = await extractText(from: pages)
            semaphore.signal()
        }

        semaphore.wait()
        return output
    }

    static func extractText(from pages: [UIImage]) async -> String? {
        guard !pages.isEmpty else { return nil }

        var pageTexts: [String] = []
        for (index, image) in pages.enumerated() {
            guard let cgImage = image.cgImage else { continue }
            let recognized = await recognizeText(in: cgImage)
            let trimmed = recognized.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { continue }
            pageTexts.append("Page \(index + 1):\n\(trimmed)")
        }

        guard !pageTexts.isEmpty else { return nil }
        return pageTexts.joined(separator: "\n\n")
    }

    private static func recognizeText(in cgImage: CGImage) async -> String {
        await withCheckedContinuation { continuation in
            let request = VNRecognizeTextRequest { request, _ in
                let text = (request.results as? [VNRecognizedTextObservation])?
                    .compactMap { $0.topCandidates(1).first?.string }
                    .joined(separator: "\n") ?? ""
                continuation.resume(returning: text)
            }

            request.recognitionLevel = .accurate
            request.usesLanguageCorrection = true

            let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
            DispatchQueue.global(qos: .userInitiated).async {
                try? handler.perform([request])
            }
        }
    }
}
