//
//  TransactionAttachmentReference.swift
//  dime
//

import Foundation
import PDFKit
import UIKit
import UniformTypeIdentifiers

enum TransactionAttachmentKind: String, Codable {
    case image
    case pdf
}

struct TransactionAttachmentReference: Codable, Identifiable, Equatable {
    private static let prefix = "saku-attachment:"

    let id: UUID
    let kind: TransactionAttachmentKind
    let relativePath: String
    let previewRelativePath: String?
    let displayName: String
    let pageCount: Int
    let sourceLabel: String?

    var serializedReference: String {
        let encoder = JSONEncoder()
        guard let data = try? encoder.encode(self),
              let json = String(data: data, encoding: .utf8)
        else {
            return sourceLabel ?? displayName
        }

        return Self.prefix + json
    }

    var fileURL: URL? {
        TransactionAttachmentStore.attachmentsDirectoryURL?
            .appendingPathComponent(relativePath)
    }

    var previewURL: URL? {
        guard let previewRelativePath else { return fileURL }
        return TransactionAttachmentStore.attachmentsDirectoryURL?
            .appendingPathComponent(previewRelativePath)
    }

    static func decode(from rawValue: String?) -> TransactionAttachmentReference? {
        guard let rawValue,
              rawValue.hasPrefix(prefix)
        else {
            return nil
        }

        let json = String(rawValue.dropFirst(prefix.count))
        guard let data = json.data(using: .utf8) else { return nil }
        return try? JSONDecoder().decode(TransactionAttachmentReference.self, from: data)
    }

    static func legacyText(from rawValue: String?) -> String? {
        guard let rawValue else { return nil }
        guard !rawValue.hasPrefix(prefix) else { return nil }
        let trimmed = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}

enum TransactionAttachmentStore {
    static var attachmentsDirectoryURL: URL? {
        let fm = FileManager.default
        let base = fm.containerURL(forSecurityApplicationGroupIdentifier: "group.com.riskcreatives.duit")
            ?? fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first

        guard let base else { return nil }
        let directory = base.appendingPathComponent("TransactionAttachments", isDirectory: true)

        if !fm.fileExists(atPath: directory.path) {
            try? fm.createDirectory(at: directory, withIntermediateDirectories: true)
        }

        return directory
    }

    static func saveImportedFile(from sourceURL: URL) -> TransactionAttachmentReference? {
        guard let directory = attachmentsDirectoryURL else { return nil }
        let ext = sourceURL.pathExtension.lowercased()
        let filename = sourceURL.deletingPathExtension().lastPathComponent
        let baseName = sanitizedFilename(filename.isEmpty ? "Attachment" : filename)
        let id = UUID()

        if ext == "pdf" {
            let pdfRelativePath = "\(id.uuidString).pdf"
            let pdfURL = directory.appendingPathComponent(pdfRelativePath)
            let previewRelativePath = "\(id.uuidString)-preview.jpg"
            let previewURL = directory.appendingPathComponent(previewRelativePath)

            do {
                if FileManager.default.fileExists(atPath: pdfURL.path) {
                    try FileManager.default.removeItem(at: pdfURL)
                }
                try FileManager.default.copyItem(at: sourceURL, to: pdfURL)

                if let previewImage = makePDFPreview(from: pdfURL),
                   let data = previewImage.jpegData(compressionQuality: 0.82) {
                    try? data.write(to: previewURL, options: .atomic)
                }

                let pageCount = PDFDocument(url: pdfURL)?.pageCount ?? 1
                return TransactionAttachmentReference(
                    id: id,
                    kind: .pdf,
                    relativePath: pdfRelativePath,
                    previewRelativePath: FileManager.default.fileExists(atPath: previewURL.path) ? previewRelativePath : nil,
                    displayName: baseName,
                    pageCount: pageCount,
                    sourceLabel: "Attached PDF"
                )
            } catch {
                return nil
            }
        }

        guard let data = try? Data(contentsOf: sourceURL),
              let image = UIImage(data: data),
              let jpegData = image.jpegData(compressionQuality: 0.86)
        else {
            return nil
        }

        let relativePath = "\(id.uuidString).jpg"
        let destinationURL = directory.appendingPathComponent(relativePath)
        do {
            try jpegData.write(to: destinationURL, options: .atomic)
            return TransactionAttachmentReference(
                id: id,
                kind: .image,
                relativePath: relativePath,
                previewRelativePath: nil,
                displayName: baseName,
                pageCount: 1,
                sourceLabel: "Attached Image"
            )
        } catch {
            return nil
        }
    }

    static func saveAIAttachment(_ attachment: AttachmentItem) -> TransactionAttachmentReference? {
        guard let directory = attachmentsDirectoryURL,
              let jpegData = attachment.thumbnail.jpegData(compressionQuality: 0.82)
        else {
            return nil
        }

        let id = UUID()
        let relativePath = "\(id.uuidString).jpg"
        let destinationURL = directory.appendingPathComponent(relativePath)

        do {
            try jpegData.write(to: destinationURL, options: .atomic)

            return TransactionAttachmentReference(
                id: id,
                kind: attachment.pages.count > 1 ? .pdf : .image,
                relativePath: relativePath,
                previewRelativePath: nil,
                displayName: attachment.label.components(separatedBy: "·").first?.trimmingCharacters(in: .whitespacesAndNewlines) ?? "AI Attachment",
                pageCount: max(attachment.pages.count, 1),
                sourceLabel: "Added via Saku AI"
            )
        } catch {
            return nil
        }
    }

    static func thumbnail(for reference: TransactionAttachmentReference) -> UIImage? {
        if let previewURL = reference.previewURL,
           let data = try? Data(contentsOf: previewURL),
           let image = UIImage(data: data) {
            return image
        }

        if reference.kind == .pdf,
           let fileURL = reference.fileURL,
           let previewImage = makePDFPreview(from: fileURL) {
            return previewImage
        }

        guard let fileURL = reference.fileURL,
              let data = try? Data(contentsOf: fileURL)
        else {
            return nil
        }

        return UIImage(data: data)
    }

    private static func makePDFPreview(from url: URL) -> UIImage? {
        guard let document = PDFDocument(url: url),
              let page = document.page(at: 0)
        else {
            return nil
        }

        let bounds = page.bounds(for: .mediaBox)
        let scale = min(500 / max(bounds.width, 1), 700 / max(bounds.height, 1))
        let size = CGSize(width: bounds.width * scale, height: bounds.height * scale)
        return page.thumbnail(of: size, for: .mediaBox)
    }

    private static func sanitizedFilename(_ name: String) -> String {
        let trimmed = name.trimmingCharacters(in: .whitespacesAndNewlines)
        let fallback = trimmed.isEmpty ? "Attachment" : trimmed
        return fallback.replacingOccurrences(of: "/", with: "-")
    }
}
