//
//  PIIRedactionService.swift
//  dime
//
//  Zone-aware PII detection.
//
//  Strategy:
//  - Find the transaction table boundary by scanning for table header keywords
//    (DEBIT, CREDIT, BALANCE, etc.).  Everything above that line is the "header
//    zone"; everything at or below is the "transaction zone".
//  - Header zone: detect IC, passport, phone, card, account number, address, names.
//  - Transaction zone: redact nothing — dates, amounts, descriptions are preserved
//    for AI cashflow extraction.
//  - Money and date formats are always excluded from PII detection regardless of zone.
//

import Foundation
import UIKit
import Vision
import NaturalLanguage
import CoreGraphics

// MARK: - Public validators (accessible from tests via @testable import)

enum PIIRedactionValidators {

    static func isValidICDate(_ yymmdd: String) -> Bool {
        let chars = Array(yymmdd)
        guard chars.count == 6, chars.allSatisfy({ $0.isNumber }),
              let mm = Int(String(chars[2]) + String(chars[3])),
              let dd = Int(String(chars[4]) + String(chars[5])) else { return false }
        return mm >= 1 && mm <= 12 && dd >= 1 && dd <= 31
    }

    static func luhnCheck(_ digits: [Int]) -> Bool {
        guard digits.count >= 13 else { return false }
        var sum = 0
        let parity = digits.count % 2
        for (i, d) in digits.enumerated() {
            if i % 2 == parity {
                let doubled = d * 2
                sum += doubled > 9 ? doubled - 9 : doubled
            } else {
                sum += d
            }
        }
        return sum % 10 == 0
    }

    static func digitsOnly(_ text: String) -> String {
        text.filter { $0.isNumber }
    }
}

// MARK: - Service

actor PIIRedactionService {

    // MARK: - Transaction table boundary keywords
    // A line with 2+ of these → that line is the transaction table header → zone starts here.
    // "urusniaga" is included because Maybank's column header row reads
    // "BUTIR URUSNIAGA | JUMLAH URUSNIAGA | BAKI PENYATA" — two hits on one line.

    private static let tableHeaderKeywords: Set<String> = [
        "debit", "credit", "pengeluaran", "simpanan",
        "withdrawal", "deposit", "baki", "urusniaga"
    ]

    // MARK: - Context word sets (header zone only)

    private static let icBoostWords: Set<String> = [
        "ic", "nric", "mykad", "kad pengenalan", "identity", "id no", "no. id",
        "identification", "ic no", "no ic", "mypr", "mykid", "i/c", "ic:", "nric:"
    ]

    private static let passportBoostWords: Set<String> = [
        "passport", "no passport", "passport no", "travel document",
        "pasport", "passport:", "no. pasport"
    ]

    private static let accountLabelWords: Set<String> = [
        "account", "acct", "a/c", "akaun", "no akaun", "account no",
        "acc no", "account:", "akaun:", "account number", "acct no", "a/c no"
    ]

    private static let nameBoostWords: Set<String> = [
        "name", "customer", "account holder", "pemegang akaun", "nama",
        "client", "holder", "beneficiary", "payee", "recipient", "nama:"
    ]

    private static let addressKeywords: Set<String> = [
        "jalan", "jln", "lorong", "persiaran", "taman", "selangor", "kuala lumpur",
        "petaling jaya", "postcode", "poskod", "wilayah", "negeri", "pulau pinang",
        "johor", "perak", "kedah", "pahang", "terengganu", "kelantan", "sabah",
        "sarawak", "melaka", "putrajaya", "cyberjaya", "shah alam", "subang",
        "klang", "puchong", "cheras", "ampang", "bangsar", "damansara", "bandar"
    ]

    // MARK: - Format exclusion regexes (always skip — never PII)

    // Covers: 1,234.56 / 23.30- / 300.00+ / RM 1,234.56 / 0.00
    private static let moneyRegex = try! NSRegularExpression(
        pattern: #"^RM\s*\d{1,3}(,\d{3})*\.\d{2}$|^\d{1,3}(,\d{3})*\.\d{2}[+\-]?$"#
    )

    // Covers: 01/01/26, 01/01/2026, 2026-01-01, 01 Jan 2026
    private static let dateRegex = try! NSRegularExpression(
        pattern: #"^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}$|^\d{4}[\/\-]\d{2}[\/\-]\d{2}$|^\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}$"#,
        options: .caseInsensitive
    )

    // MARK: - Public API

    func detectPII(in image: UIImage, pageIndex: Int) async throws -> (items: [RedactionItem], pageText: String) {
        guard let cgImage = image.cgImage else { return ([], "") }

        let observations = try await recognizeText(in: cgImage)
        let lines = buildLines(from: observations)
        let lineTexts: [String] = lines.map { line in
            line.compactMap { $0.topCandidates(1).first?.string }.joined(separator: " ")
        }
        let fullPageText = lineTexts.joined(separator: "\n")

        // Find where the transaction table starts (UIKit Y coordinate, 0 = top)
        let transactionZoneY = findTransactionZoneStart(lines: lines, lineTexts: lineTexts)

        // Collect header-zone line entries only (above the transaction boundary)
        var headerEntries: [(obs: [VNRecognizedTextObservation], text: String, arrayIdx: Int)] = []
        for (idx, line) in lines.enumerated() {
            let lineTopY = line.first.map { 1 - $0.boundingBox.maxY } ?? 0
            if lineTopY >= transactionZoneY { break }
            headerEntries.append((line, lineTexts[idx], headerEntries.count))
        }

        let headerLineTexts = headerEntries.map(\.text)
        var raw: [RedactionItem] = []

        // Per-observation PII detection (header zone only)
        for entry in headerEntries {
            let context = contextWindow(around: entry.arrayIdx, in: headerLineTexts)
            for obs in entry.obs {
                guard let candidate = obs.topCandidates(1).first else { continue }
                let text = candidate.string
                guard !isMoneyFormat(text), !isDateFormat(text) else { continue }
                raw.append(contentsOf: detectHeaderPII(
                    text: text, context: context, candidate: candidate, pageIndex: pageIndex
                ))
            }
        }

        // Address: line-level detection in header
        raw.append(contentsOf: detectAddressLines(
            lines: headerEntries.map(\.obs),
            lineTexts: headerLineTexts,
            pageIndex: pageIndex
        ))

        // Malaysian patronymic name detection (BIN/BINTI) — runs before NLP fallback
        raw.append(contentsOf: detectMalaysianName(
            lines: headerEntries.map(\.obs),
            lineTexts: headerLineTexts,
            pageIndex: pageIndex
        ))

        // NLP name detection across header text (catches non-Malay names)
        let headerObservations = headerEntries.flatMap(\.obs)
        raw.append(contentsOf: detectNames(
            in: headerLineTexts.joined(separator: "\n"),
            lineTexts: headerLineTexts,
            observations: headerObservations,
            pageIndex: pageIndex
        ))

        return (mergeAndDeduplicate(raw), fullPageText)
    }

    /// Render redacted image — applies all auto items plus any manually drawn boxes.
    func applyRedactions(
        _ items: [RedactionItem],
        manualBoxes: [ManualRedactionBox] = [],
        to image: UIImage,
        pageIndex: Int
    ) -> UIImage {
        let autoBoxes = items.filter { $0.pageIndex == pageIndex }
        let manualForPage = manualBoxes.filter { $0.pageIndex == pageIndex }
        guard !autoBoxes.isEmpty || !manualForPage.isEmpty else { return image }

        let size = image.size
        let renderer = UIGraphicsImageRenderer(size: size)
        return renderer.image { ctx in
            image.draw(in: CGRect(origin: .zero, size: size))
            ctx.cgContext.setFillColor(UIColor.black.cgColor)
            for item in autoBoxes {
                let b = item.boundingBox
                let rect = CGRect(x: b.minX * size.width, y: b.minY * size.height,
                                  width: b.width * size.width, height: b.height * size.height)
                UIBezierPath(roundedRect: rect, cornerRadius: 3).fill()
            }
            for box in manualForPage {
                let b = box.boundingBox
                let rect = CGRect(x: b.minX * size.width, y: b.minY * size.height,
                                  width: b.width * size.width, height: b.height * size.height)
                UIBezierPath(roundedRect: rect, cornerRadius: 3).fill()
            }
        }
    }

    // MARK: - Zone Detection

    /// Returns the UIKit Y position (0 = top) where the transaction table begins.
    /// Falls back to 0.30 (top 30%) so the entire page is never treated as header zone.
    private func findTransactionZoneStart(
        lines: [[VNRecognizedTextObservation]],
        lineTexts: [String]
    ) -> CGFloat {
        for (idx, text) in lineTexts.enumerated() {
            guard let firstObs = lines[idx].first else { continue }
            let lineY = 1 - firstObs.boundingBox.maxY

            // Transaction tables never start in the top 20%
            guard lineY > 0.20 else { continue }

            let lower = text.lowercased()
            let hits = Self.tableHeaderKeywords.filter { lower.contains($0) }.count
            if hits >= 2 { return lineY }
        }
        // No table header found — conservative fallback: treat top 30% as header zone
        return 0.30
    }

    // MARK: - Format Exclusion

    private func isMoneyFormat(_ text: String) -> Bool {
        let t = text.trimmingCharacters(in: .whitespaces)
        return Self.moneyRegex.firstMatch(in: t, range: NSRange(t.startIndex..., in: t)) != nil
    }

    private func isDateFormat(_ text: String) -> Bool {
        let t = text.trimmingCharacters(in: .whitespaces)
        return Self.dateRegex.firstMatch(in: t, range: NSRange(t.startIndex..., in: t)) != nil
    }

    // MARK: - Header PII Dispatch

    private func detectHeaderPII(
        text: String,
        context: String,
        candidate: VNRecognizedText,
        pageIndex: Int
    ) -> [RedactionItem] {
        var results: [RedactionItem] = []
        results += detectIC(text: text, context: context, candidate: candidate, pageIndex: pageIndex)
        results += detectPassport(text: text, context: context, candidate: candidate, pageIndex: pageIndex)
        results += detectPhone(text: text, candidate: candidate, pageIndex: pageIndex)
        results += detectCard(text: text, candidate: candidate, pageIndex: pageIndex)
        results += detectAccountNumber(text: text, context: context, candidate: candidate, pageIndex: pageIndex)
        return results
    }

    // MARK: - IC

    private func detectIC(text: String, context: String, candidate: VNRecognizedText, pageIndex: Int) -> [RedactionItem] {
        let pattern = #"(?<!\d)(\d{6})[- ]?(\d{2})[- ]?(\d{4})(?!\d)"#
        return matchItems(pattern: pattern, in: text, candidate: candidate, pageIndex: pageIndex) { matched in
            let digits = PIIRedactionValidators.digitsOnly(matched)
            guard digits.count == 12,
                  PIIRedactionValidators.isValidICDate(String(digits.prefix(6))) else { return nil }
            let boosted = containsAny(context, in: Self.icBoostWords)
            return (type: .icPassport,
                    confidence: boosted ? 0.97 : 0.75,
                    reason: boosted ? "IC + IC context" : "IC pattern (YYMMDD-PB-NNNN)")
        }
    }

    // MARK: - Passport

    private func detectPassport(text: String, context: String, candidate: VNRecognizedText, pageIndex: Int) -> [RedactionItem] {
        guard containsAny(context, in: Self.passportBoostWords) else { return [] }
        let pattern = #"\b[A-Z]{1,2}\d{6,9}\b"#
        return matchItems(pattern: pattern, in: text, candidate: candidate, pageIndex: pageIndex) { _ in
            (type: .icPassport, confidence: 0.92, reason: "Passport number + context")
        }
    }

    // MARK: - Phone

    private func detectPhone(text: String, candidate: VNRecognizedText, pageIndex: Int) -> [RedactionItem] {
        let pattern = #"(?<!\d)(\+?6?01[0-9][-\s]?\d{7,8})(?!\d)"#
        return matchItems(pattern: pattern, in: text, candidate: candidate, pageIndex: pageIndex) { _ in
            (type: .contactInfo, confidence: 0.88, reason: "Malaysian phone number")
        }
    }

    // MARK: - Card Number

    private func detectCard(text: String, candidate: VNRecognizedText, pageIndex: Int) -> [RedactionItem] {
        let pattern = #"\b(?:\d[ \-]?){12,18}\d\b"#
        return matchItems(pattern: pattern, in: text, candidate: candidate, pageIndex: pageIndex) { matched in
            let digits = PIIRedactionValidators.digitsOnly(matched)
            guard (13...19).contains(digits.count) else { return nil }
            let ints = digits.compactMap { $0.wholeNumberValue }
            if PIIRedactionValidators.luhnCheck(ints) {
                return (type: .accountNumber, confidence: 0.95, reason: "Card number (Luhn valid)")
            }
            if matched.contains("*") {
                return (type: .accountNumber, confidence: 0.85, reason: "Masked card number")
            }
            return nil
        }
    }

    // MARK: - Account Number

    private func detectAccountNumber(text: String, context: String, candidate: VNRecognizedText, pageIndex: Int) -> [RedactionItem] {
        guard containsAny(context, in: Self.accountLabelWords) else { return [] }
        // Match straight digits OR hyphenated format (e.g. 162367-010552, 1234-5678-9012)
        let pattern = #"(?<!\d)\d{3,8}(?:-\d{2,8})+(?!\d)|(?<!\d)\d{8,16}(?!\d)"#
        return matchItems(pattern: pattern, in: text, candidate: candidate, pageIndex: pageIndex) { matched in
            let digits = PIIRedactionValidators.digitsOnly(matched)
            guard (8...18).contains(digits.count) else { return nil }
            return (type: .accountNumber, confidence: 0.88, reason: "Account number near account label")
        }
    }

    // MARK: - Address (line-level)

    private func detectAddressLines(
        lines: [[VNRecognizedTextObservation]],
        lineTexts: [String],
        pageIndex: Int
    ) -> [RedactionItem] {
        var items: [RedactionItem] = []
        for (idx, text) in lineTexts.enumerated() {
            // Skip the very top of the page — that's the bank's letterhead, not the customer's address
            let lineY = lines[idx].first.map { 1 - $0.boundingBox.maxY } ?? 0
            guard lineY > 0.10 else { continue }

            let lower = text.lowercased()
            let hits = Self.addressKeywords.filter { lower.contains($0) }.count
            guard hits >= 2 else { continue }

            let allBoxes = lines[idx].compactMap { o -> CGRect? in
                let vb = o.boundingBox
                return CGRect(x: vb.minX, y: 1 - vb.maxY, width: vb.width, height: vb.height)
            }
            guard let merged = allBoxes.reduce(nil as CGRect?, { $0?.union($1) ?? $1 }) else { continue }
            items.append(RedactionItem(
                redactionType: .address,
                pageIndex: pageIndex,
                boundingBox: merged.insetBy(dx: -0.005, dy: -0.004).clamped,
                confidenceScore: 0.80,
                detectionReason: "Address line (\(hits) keywords)"
            ))
        }
        return items
    }

    // MARK: - Malaysian patronymic name (BIN / BINTI)

    private static let binPattern = try! NSRegularExpression(
        pattern: #"\b(BIN|BINTI)\b"#, options: .caseInsensitive
    )

    private func detectMalaysianName(
        lines: [[VNRecognizedTextObservation]],
        lineTexts: [String],
        pageIndex: Int
    ) -> [RedactionItem] {
        var items: [RedactionItem] = []
        for (idx, text) in lineTexts.enumerated() {
            let lineY = lines[idx].first.map { 1 - $0.boundingBox.maxY } ?? 0
            guard lineY > 0.10 else { continue }

            let nsRange = NSRange(text.startIndex..., in: text)
            guard Self.binPattern.firstMatch(in: text, range: nsRange) != nil else { continue }

            let allBoxes = lines[idx].compactMap { o -> CGRect? in
                let vb = o.boundingBox
                return CGRect(x: vb.minX, y: 1 - vb.maxY, width: vb.width, height: vb.height)
            }
            guard let merged = allBoxes.reduce(nil as CGRect?, { $0?.union($1) ?? $1 }) else { continue }
            items.append(RedactionItem(
                redactionType: .name,
                pageIndex: pageIndex,
                boundingBox: merged.insetBy(dx: -0.005, dy: -0.004).clamped,
                confidenceScore: 0.92,
                detectionReason: "Malaysian name (BIN/BINTI)"
            ))
        }
        return items
    }

    // MARK: - NLP Names

    private func detectNames(
        in text: String,
        lineTexts: [String],
        observations: [VNRecognizedTextObservation],
        pageIndex: Int
    ) -> [RedactionItem] {
        // Title-case so the NLP tagger recognises ALL-CAPS bank statement names.
        // Matching back to observations uses .caseInsensitive so originals are found.
        let normalized = text.capitalized

        let tagger = NLTagger(tagSchemes: [.nameType])
        tagger.string = normalized

        var detectedNames: [String] = []
        tagger.enumerateTags(in: normalized.startIndex..<normalized.endIndex, unit: .word, scheme: .nameType) { tag, range in
            if tag == .personalName {
                let token = String(normalized[range])
                if token.count >= 4, token.first?.isLetter == true {
                    detectedNames.append(token)
                }
            }
            return true
        }

        let allContext = lineTexts.joined(separator: " ").lowercased()
        let hasNameCtx = containsAny(allContext, in: Self.nameBoostWords)
        var items: [RedactionItem] = []

        for name in detectedNames {
            for obs in observations {
                guard let candidate = obs.topCandidates(1).first,
                      let nameRange = candidate.string.range(of: name, options: .caseInsensitive),
                      let wordObs = try? candidate.boundingBox(for: nameRange) else { continue }

                let vBox = wordObs.boundingBox
                let uiBox = CGRect(x: vBox.minX, y: 1 - vBox.maxY, width: vBox.width, height: vBox.height)
                    .insetBy(dx: -0.006, dy: -0.004).clamped

                items.append(RedactionItem(
                    redactionType: .name,
                    pageIndex: pageIndex,
                    boundingBox: uiBox,
                    confidenceScore: hasNameCtx ? 0.85 : 0.65,
                    detectionReason: hasNameCtx ? "Personal name + name context" : "NLP personal name"
                ))
                break
            }
        }
        return items
    }

    // MARK: - Line Grouping

    private func buildLines(from observations: [VNRecognizedTextObservation]) -> [[VNRecognizedTextObservation]] {
        let sorted = observations.sorted { (1 - $0.boundingBox.maxY) < (1 - $1.boundingBox.maxY) }
        var lines: [[VNRecognizedTextObservation]] = []
        var current: [VNRecognizedTextObservation] = []
        var lastY: CGFloat = -1

        for obs in sorted {
            let y = 1 - obs.boundingBox.maxY
            if lastY < 0 || abs(y - lastY) < 0.008 {
                current.append(obs)
            } else {
                if !current.isEmpty {
                    lines.append(current.sorted { $0.boundingBox.minX < $1.boundingBox.minX })
                }
                current = [obs]
            }
            lastY = y
        }
        if !current.isEmpty {
            lines.append(current.sorted { $0.boundingBox.minX < $1.boundingBox.minX })
        }
        return lines
    }

    private func contextWindow(around idx: Int, in lineTexts: [String]) -> String {
        let start = max(0, idx - 2)
        let end   = min(lineTexts.count - 1, idx + 2)
        return lineTexts[start...end].joined(separator: " ").lowercased()
    }

    // MARK: - Merge & Deduplicate

    private func mergeAndDeduplicate(_ items: [RedactionItem]) -> [RedactionItem] {
        let sorted = items.sorted {
            if $0.confidenceScore != $1.confidenceScore { return $0.confidenceScore > $1.confidenceScore }
            return $0.redactionType.mergePriority > $1.redactionType.mergePriority
        }
        var result: [RedactionItem] = []
        for item in sorted {
            let overlaps = result.contains {
                $0.pageIndex == item.pageIndex &&
                $0.boundingBox.intersection(item.boundingBox).area > item.boundingBox.area * 0.45
            }
            if !overlaps { result.append(item) }
        }
        return result
    }

    // MARK: - Vision OCR

    private func recognizeText(in cgImage: CGImage) async throws -> [VNRecognizedTextObservation] {
        try await withCheckedThrowingContinuation { continuation in
            let request = VNRecognizeTextRequest { req, err in
                if let err { continuation.resume(throwing: err); return }
                continuation.resume(returning: (req.results as? [VNRecognizedTextObservation]) ?? [])
            }
            request.recognitionLevel = .accurate
            request.usesLanguageCorrection = true

            let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
            DispatchQueue.global(qos: .userInitiated).async {
                do { try handler.perform([request]) }
                catch { continuation.resume(throwing: error) }
            }
        }
    }

    // MARK: - Match Helper

    private func matchItems(
        pattern: String,
        in text: String,
        candidate: VNRecognizedText,
        pageIndex: Int,
        confidence closureForMatch: (String) -> (type: RedactionType, confidence: Double, reason: String)?
    ) -> [RedactionItem] {
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return [] }
        var items: [RedactionItem] = []
        let nsRange = NSRange(text.startIndex..., in: text)

        for match in regex.matches(in: text, range: nsRange) {
            guard let swiftRange = Range(match.range, in: text) else { continue }
            let matched = String(text[swiftRange])
            guard let (type, confidence, reason) = closureForMatch(matched), confidence > 0.05 else { continue }
            guard let wordObs = try? candidate.boundingBox(for: swiftRange) else { continue }

            let vb = wordObs.boundingBox
            let uiBox = CGRect(x: vb.minX, y: 1 - vb.maxY, width: vb.width, height: vb.height)
                .insetBy(dx: -0.006, dy: -0.004).clamped

            items.append(RedactionItem(
                redactionType: type,
                pageIndex: pageIndex,
                boundingBox: uiBox,
                confidenceScore: confidence,
                detectionReason: reason
            ))
        }
        return items
    }

    private func containsAny(_ text: String, in words: Set<String>) -> Bool {
        let lowered = text.lowercased()
        return words.contains { lowered.contains($0) }
    }
}

// MARK: - CGRect helpers

private extension CGRect {
    var area: CGFloat { width * height }
    var clamped: CGRect {
        let x = max(0, minX); let y = max(0, minY)
        return CGRect(x: x, y: y,
                      width: max(0, min(width, 1 - x)),
                      height: max(0, min(height, 1 - y)))
    }
}
