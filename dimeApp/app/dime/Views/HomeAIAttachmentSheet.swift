//
//  HomeAIAttachmentSheet.swift
//  dime
//

import SwiftUI
import Photos
import PhotosUI
import PDFKit
import UniformTypeIdentifiers

struct HomeAIAttachmentSheet: View {
    @Binding var attachments: [AttachmentItem]
    let onQuickPrompt: (String) -> Void
    @Environment(\.dismiss) private var dismiss

    @State private var recentAssets: [PHAsset] = []
    @State private var assetToAttachmentID: [String: UUID] = [:]
    @State private var authStatus: PHAuthorizationStatus = .notDetermined
    @State private var showFullPicker = false
    @State private var showDocumentPicker = false

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
                    .font(.system(.headline, design: .rounded).weight(.semibold))
                    .foregroundColor(.primary)
                Spacer()
                Button("All Photos") { showFullPicker = true }
                    .font(.system(.subheadline, design: .rounded))
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
                            .font(.system(size: 22, weight: .medium))
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
                                        .font(.system(size: 20))
                                        .foregroundColor(Color(.secondaryLabel))
                                    Text(authStatus == .denied ? "Allow Access" : "Enable Photos")
                                        .font(.system(size: 11, design: .rounded).weight(.medium))
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
            PDFDocumentPickerView { images in
                for (i, img) in images.enumerated() {
                    let label = images.count == 1 ? "PDF" : "PDF p.\(i + 1)"
                    attachments.append(AttachmentItem(image: img, label: label))
                }
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
                            .font(.system(size: 11, weight: .bold))
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
                        .font(.system(size: 16, weight: .medium))
                        .foregroundColor(iconColor)
                }

                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .font(.system(.body, design: .rounded).weight(.medium))
                        .foregroundColor(.primary)
                    Text(subtitle)
                        .font(.system(.footnote, design: .rounded))
                        .foregroundColor(.secondary)
                }

                Spacer()

                Image(systemName: "chevron.right")
                    .font(.system(size: 12, weight: .semibold))
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
    let onComplete: ([UIImage]) -> Void

    func makeUIViewController(context: Context) -> UIDocumentPickerViewController {
        let picker = UIDocumentPickerViewController(forOpeningContentTypes: [UTType.pdf])
        picker.delegate = context.coordinator
        picker.allowsMultipleSelection = false
        return picker
    }

    func updateUIViewController(_ uiViewController: UIDocumentPickerViewController, context: Context) {}

    func makeCoordinator() -> Coordinator { Coordinator(onComplete: onComplete) }

    final class Coordinator: NSObject, UIDocumentPickerDelegate {
        let onComplete: ([UIImage]) -> Void
        init(onComplete: @escaping ([UIImage]) -> Void) { self.onComplete = onComplete }

        func documentPicker(_ controller: UIDocumentPickerViewController, didPickDocumentsAt urls: [URL]) {
            guard let url = urls.first else { return }
            _ = url.startAccessingSecurityScopedResource()
            defer { url.stopAccessingSecurityScopedResource() }

            let images = renderPDFPages(url: url)
            DispatchQueue.main.async { self.onComplete(images) }
        }

        private func renderPDFPages(url: URL) -> [UIImage] {
            guard let document = PDFDocument(url: url) else { return [] }
            var images: [UIImage] = []
            let pageCount = min(document.pageCount, 12)

            for i in 0..<pageCount {
                guard let page = document.page(at: i) else { continue }
                let mediaRect = page.bounds(for: .mediaBox)
                let scale: CGFloat = min(1600 / mediaRect.width, 2200 / mediaRect.height)
                let targetSize = CGSize(
                    width: mediaRect.width * scale,
                    height: mediaRect.height * scale
                )
                images.append(page.thumbnail(of: targetSize, for: .mediaBox))
            }

            return images
        }
    }
}
