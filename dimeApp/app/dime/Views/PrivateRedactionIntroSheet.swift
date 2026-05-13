//
//  PrivateRedactionIntroSheet.swift
//  dime
//

import SwiftUI
import UIKit
import ImageIO

// MARK: - Intro sheet

struct PrivateRedactionIntroSheet: View {
    let onContinue: () -> Void
    let onDismiss: () -> Void

    var body: some View {
        VStack(spacing: 0) {
            Capsule()
                .fill(Color(.systemGray4))
                .frame(width: 36, height: 5)
                .padding(.top, 14)
                .padding(.bottom, 28)

            RedactionGIFView()
                .frame(width: 220, height: 160)
                .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
                .padding(.bottom, 28)

            Text("Your documents stay private")
                .font(Font.satoshi(.title2, weight: .bold))
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
                .padding(.bottom, 10)

            Text("Before Kira reads anything, private details are detected and hidden — right here on your iPhone.")
                .font(Font.satoshi(.subheadline))
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .lineSpacing(3)
                .padding(.horizontal, 32)
                .padding(.bottom, 28)

            VStack(alignment: .leading, spacing: 14) {
                RedactionTrustBullet(
                    icon: "iphone.lock.fill", color: .blue,
                    text: "Scanned entirely on-device. Nothing leaves your iPhone until you approve."
                )
                RedactionTrustBullet(
                    icon: "eye.slash.fill", color: .purple,
                    text: "Names, IDs, card numbers, and contacts are redacted first."
                )
                RedactionTrustBullet(
                    icon: "checkmark.shield.fill", color: .green,
                    text: "You review what was found before anything is processed."
                )
            }
            .padding(.horizontal, 24)
            .padding(.bottom, 20)

            Text("Kira can't catch everything. Review the preview before continuing.")
                .font(Font.satoshi(.caption))
                .foregroundStyle(Color(.tertiaryLabel))
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
                .padding(.bottom, 24)

            Button(action: onContinue) {
                Text("Continue securely")
                    .font(Font.satoshi(.body, weight: .semibold))
                    .foregroundStyle(.white)
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 16)
                    .background(Color.accentColor, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
            }
            .padding(.horizontal, 20)
            .padding(.bottom, 12)

            Button("Not now", action: onDismiss)
                .font(Font.satoshi(.subheadline))
                .foregroundStyle(.secondary)
                .padding(.bottom, 32)
        }
        .background(Color.PrimaryBackground)
    }
}

// MARK: - Trust bullet row

private struct RedactionTrustBullet: View {
    let icon: String
    let color: Color
    let text: String

    var body: some View {
        HStack(alignment: .top, spacing: 14) {
            ZStack {
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .fill(color.opacity(0.12))
                    .frame(width: 38, height: 38)
                Image(systemName: icon)
                    .font(Font.satoshi(15, weight: .medium))
                    .foregroundStyle(color)
            }
            Text(text)
                .font(Font.satoshi(.subheadline))
                .foregroundStyle(.primary)
                .multilineTextAlignment(.leading)
                .fixedSize(horizontal: false, vertical: true)
            Spacer(minLength: 0)
        }
    }
}

// MARK: - Animated GIF view

struct RedactionGIFView: UIViewRepresentable {
    func makeUIView(context: Context) -> UIImageView {
        let imageView = UIImageView()
        imageView.contentMode = .scaleAspectFill
        imageView.clipsToBounds = true
        imageView.image = loadGIF() ?? makeFallback()
        return imageView
    }

    func updateUIView(_ uiView: UIImageView, context: Context) {}

    private func loadGIF() -> UIImage? {
        guard let url = Bundle.main.url(forResource: "scan document", withExtension: "gif"),
              let data = try? Data(contentsOf: url),
              let source = CGImageSourceCreateWithData(data as CFData, nil) else { return nil }

        let count = CGImageSourceGetCount(source)
        var frames: [UIImage] = []
        var totalDuration: Double = 0

        for i in 0..<count {
            guard let cgImage = CGImageSourceCreateImageAtIndex(source, i, nil) else { continue }
            var delay: Double = 0.1
            if let props = CGImageSourceCopyPropertiesAtIndex(source, i, nil) as? [CFString: Any],
               let gifProps = props[kCGImagePropertyGIFDictionary] as? [CFString: Any] {
                delay = (gifProps[kCGImagePropertyGIFUnclampedDelayTime] as? Double)
                    ?? (gifProps[kCGImagePropertyGIFDelayTime] as? Double)
                    ?? 0.1
            }
            totalDuration += max(delay, 0.02)
            frames.append(UIImage(cgImage: cgImage))
        }

        return frames.isEmpty ? nil : UIImage.animatedImage(with: frames, duration: totalDuration)
    }

    private func makeFallback() -> UIImage? {
        // Rendered placeholder if GIF not yet added to bundle
        let size = CGSize(width: 220, height: 160)
        return UIGraphicsImageRenderer(size: size).image { ctx in
            UIColor.systemBlue.withAlphaComponent(0.08).setFill()
            UIBezierPath(roundedRect: CGRect(origin: .zero, size: size), cornerRadius: 12).fill()
            let config = UIImage.SymbolConfiguration(pointSize: 48, weight: .light)
            if let icon = UIImage(systemName: "doc.viewfinder", withConfiguration: config) {
                let tinted = icon.withTintColor(.systemBlue.withAlphaComponent(0.5), renderingMode: .alwaysOriginal)
                let iconRect = CGRect(
                    x: (size.width - tinted.size.width) / 2,
                    y: (size.height - tinted.size.height) / 2,
                    width: tinted.size.width,
                    height: tinted.size.height
                )
                tinted.draw(in: iconRect)
            }
        }
    }
}
