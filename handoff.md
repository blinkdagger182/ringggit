# Kira Redaction Feature — Handoff Document

**Project:** dime (iOS personal finance app, Malaysian market)  
**Feature:** On-device PII redaction before sending bank statement images to AI  
**Status:** Substantially working. Name + address redaction fixed but not yet device-tested.

---

## Goal

When a user attaches a bank statement PDF or scanned document to chat with Kira (the AI assistant), the app must:

1. Show a privacy intro sheet (always, not just first time)
2. Run Vision OCR on-device
3. Auto-detect and black-out PII in the **header zone only** — name, IC, account number, address, phone
4. Leave the **transaction zone completely untouched** — dates, amounts, merchant names must reach the AI intact for cashflow extraction
5. Let the user manually draw additional redaction boxes if they want
6. Pass the redacted page images + raw OCR text to the AI

**Normal users should only see:** "Protecting your document" → "Private details protected"

---

## Architecture

### Files involved

| File | Role |
|------|------|
| `dimeApp/app/dime/Utilities/PIIRedactionService.swift` | Core detection + rendering (`actor`) |
| `dimeApp/app/dime/Models/RedactionItem.swift` | Data model for detected PII items |
| `dimeApp/app/dime/Models/RedactionFlowViewModel.swift` | Orchestration `@MainActor` class |
| `dimeApp/app/dime/Models/RedactionItem.swift` | `ManualRedactionBox` struct lives here too (check `RedactionFlowViewModel.swift`) |
| `dimeApp/app/dime/Views/RedactionPreviewView.swift` | SwiftUI review screen with draw-to-redact |
| `dimeApp/app/dime/Views/PrivateRedactionIntroSheet.swift` | First-time privacy trust modal |
| `dimeApp/app/dime/Views/HomeAIAttachmentSheet.swift` | Entry point — calls `redactionVM.startRedaction(...)` |

### Flow

```
HomeAIAttachmentSheet
  → user picks PDF / scan
  → redactionVM.startRedaction(pages:filename:sourceText:)
      → showFlow = true
      → showIntro = true (after 0.15s)
  → PrivateRedactionIntroSheet shown (always)
  → user taps "Continue securely"
      → markIntroSeen() → beginDetection()
          → PIIRedactionService.detectPII(in:pageIndex:) per page
          → flowState = .ready
  → RedactionPreviewView shown
      → user sees preview with black boxes on PII
      → user can draw more boxes (draw mode)
      → user taps "Use this copy"
          → generateRedactedCopy() → applyRedactions(...)
          → redactedPages passed back to HomeAIAttachmentSheet
          → AttachmentItem added with redacted images + OCR text
```

---

## Detection Strategy

### Zone detection

`findTransactionZoneStart()` scans OCR lines top-to-bottom for a line containing **2+ of these keywords**:

```
"debit", "credit", "pengeluaran", "simpanan", "withdrawal", "deposit", "baki", "urusniaga"
```

That line's Y position becomes the zone boundary. Everything above = header zone. Everything at/below = transaction zone (redact nothing).

**Guard:** only checks lines at Y > 0.20 (transaction tables never start in top 20%).  
**Fallback:** if no keyword line found, use Y = 0.30 (conservative: top 30% = header).

Verified on Maybank Islamic `jan2026.pdf` (12 pages): zone boundary lands at Y ≈ 0.234, correctly splitting header from transactions.

### Header PII detectors

All run on header-zone observations only:

| Detector | Method | Notes |
|----------|--------|-------|
| Malaysian IC | Regex `YYMMDD-PB-NNNN` + date validity check | Boosts confidence if "ic"/"mykad" in context |
| Passport | Regex `[A-Z]{1,2}\d{6,9}` | Only fires if "passport" in context window |
| Phone | Regex `+60` / `01X` Malaysian format | — |
| Card number | Regex + Luhn validation | Header only — cards don't appear in transactions |
| Account number | Regex hyphenated `\d{3,8}(-\d{2,8})+` or `\d{8,16}` | Only fires if "akaun"/"account" in context |
| Address | Line-level: 2+ address keywords on same line | Y > 0.10 guard to skip bank's own letterhead |
| Malaysian name | BIN/BINTI word-boundary regex on line | Redacts entire line; Y > 0.10 guard |
| NLP name | `NLTagger(.nameType)` on `.capitalized` header text | Fallback for non-Malay names |

### Money + date exclusion

Before any detection, each observation's text is checked against:
- Money regex: `RM 1,234.56`, `1,234.56+`, `23.30-`, etc.
- Date regex: `01/01/26`, `2026-01-01`, `01 Jan 2026`

These are **always skipped** regardless of zone.

---

## Critical Bugs Fixed (history)

### 1. `buildLines` threshold too large (0.025 → 0.008)

**Bug:** `buildLines()` groups Vision OCR observations by Y proximity with threshold 0.025 (2.5% of page height). For PDF-rendered bank statements at 2x scale, this cascades — adjacent lines within 2.5% of each other get merged into one massive "line". Page 1 of a 12-page statement collapsed to 2 observations total.

**Effect:** `findTransactionZoneStart` couldn't find the column header row (no single line had 2+ keywords), fell back to 0.30. But the second mega-line at Y=0.196 was below 0.30, so all transaction data got classified as "header" and PII-scanned. Massive false positives.

**Fix:** Changed threshold `0.025` → `0.008` in `buildLines()`. Now page 1 splits into ~60 individual lines. Zone boundary correctly found at Y=0.234. Transaction zone has zero false positives.

```swift
// PIIRedactionService.swift:456
if lastY < 0 || abs(y - lastY) < 0.008 {
```

### 2. `findTransactionZoneStart` returning nil → whole page blacked out

**Bug (earlier):** Return type was `CGFloat?`, call site used `if let` and fell through when nil, treating the entire page as header zone.

**Fix:** Changed to non-optional `CGFloat` with `return 0.30` fallback.

### 3. Bank letterhead address being redacted

**Bug:** "15th Floor, Tower A, Dataran Maybank, 1, Jalan Maarof, 59000 Kuala Lumpur" contains "jalan" + "kuala lumpur" = 2 keywords → triggered address detection.

**Fix:** Added `guard lineY > 0.10` in `detectAddressLines` to skip top 10% of page (bank letterhead zone).

### 4. ALL-CAPS customer name not detected by NLP

**Bug:** `NLTagger` with `.nameType` fails on ALL-CAPS text ("MUHAMMAD AZHAN RIZHAN BIN RUSLAN"). Previous code also had `token != token.uppercased()` filter that excluded all uppercase tokens.

**Fix:** Run `text.capitalized` on input before NLP. Removed the uppercase filter.

### 5. Malaysian name not fully detected by NLP

**Bug:** NLP detects "Muhammad" but not "Azhan", "Rizhan" (uncommon names in English NLP training data). Result: only 1-2 individual word boxes redacted, not the full name.

**Fix:** Added `detectMalaysianName()` — scans header lines for `\b(BIN|BINTI)\b` word-boundary regex. If matched, the **entire line** is redacted as a name (confidence 0.92). BIN/BINTI are Malay patronymics that only appear in a person's full name.

### 6. Hyphenated account number not detected

**Bug:** Old pattern `\d{8,16}` requires consecutive digits. Maybank format is `162367-010552` (hyphenated).

**Fix:** Pattern updated to `(?<!\d)\d{3,8}(?:-\d{2,8})+(?!\d)|(?<!\d)\d{8,16}(?!\d)`.

### 7. DragGesture conflicting with TabView page swipe

**Bug:** Draw-to-redact gesture and TabView horizontal swipe fight each other on multi-page documents.

**Fix:** Introduced draw mode toggle. When `isDrawMode = true`, hide TabView and show a single static page view where the drag gesture is active. In normal mode, TabView is shown without drag gesture.

### 8. iOS 16 API errors (`presentationDetents`)

**Fix:** Wrapped in `if #available(iOS 16, *)` inside a `largeSheetPresentation()` View extension (deployment target is iOS 15.0).

---

## Known False Positives (accepted)

In the transaction zone (correctly excluded from detection), some fund transfer descriptions contain the customer's own name abbreviated: `"MUHAMMAD AZHAN RIZH*"` — this is **intentional** and must NOT be redacted (the AI needs it for categorization).

On page 1, these transaction-zone false positives were present before the 0.008 threshold fix and are now zero.

---

## What Has NOT Been Done

### UX decisions deferred (see `memory/project_redaction_refactor_handoff.md`)

Three UX decisions were explicitly deferred by the user to a future session:

1. **Success state:** After redacted images generated — auto-dismiss after ~1.5s, or show "Private details protected" with explicit "Continue" button?
2. **Intro sheet:** Keep showing every time (current behavior) or only first time?
3. **Debug view:** `PrivacyRedactionDebugView` exists in design but is not wired up. Where should it appear in DEBUG builds? (Replacing the success screen? Separate button?)

### Chinese characters in multi-language headers (page 10)

Page 10 has "URUSNIAGA AKAUN/ 戶口往支項 /ACCOUNT TRANSACTIONS" — Chinese text. Vision OCR reads Chinese fine but the current `tableHeaderKeywords` set is Malay/English only. This works because "urusniaga" still appears on the same line. No action needed unless tested on CIMB/RHB which may use different column header language.

### Non-Malay names (Chinese, Indian)

`detectMalaysianName` (BIN/BINTI) won't catch Chinese names ("LIM QIU SHI") or Indian names without patronymic ("NADIRAH BINTI YAHYA" — wait, BINTI would catch that). Chinese names in headers: if the account holder is Chinese-Malaysian, their name won't have BIN/BINTI. NLP fallback (`detectNames`) may or may not catch them. Not yet tested.

### Address Y guard edge cases

Y > 0.10 guard was lowered from 0.12 to 0.10 to catch pages where the customer address block starts slightly higher. The bank's own address ("15th Floor, Tower A…") is at Y ≈ 0.070 which still falls below the guard and won't be redacted. If someone builds a custom Xcode scheme with a different DPI or page scale, this Y=0.070 could shift. Not a practical concern.

---

## Test Script

A standalone Swift test script exists at `/tmp/test_redaction.swift`. It is NOT part of the Xcode project. Run with:

```sh
swift /tmp/test_redaction.swift
```

It renders a page of `/Users/rizhanruslan/Downloads/jan2026.pdf` to a `CGImage`, runs Vision OCR, simulates `buildLines` + `findTransactionZoneStart` + all detectors, and prints:
- Zone boundary Y
- Header lines with Y positions and text
- First 10 transaction lines
- All PII detections in header with type and Y
- False positives check in transaction zone

Change `pdf.page(at: 0)` to `pdf.page(at: N)` to test page N+1.

**Current state:** All thresholds in the test script match the service (0.008). Results on page 10:
- Zone: 0.234
- Detections: Name(BIN/BINTI) Y=0.113, Address Y=0.126, Address Y=0.141, Account Y=0.156
- Transaction zone false positives: 0

---

## Environment

- **Platform:** iOS 15.0 deployment target (SwiftUI + UIKit)
- **Frameworks:** Vision (OCR), NaturalLanguage (NLP), CoreGraphics, PDFKit
- **Font:** `Font.satoshi(_:weight:)` — custom, always use this not system fonts
- **Colors:** `Color.PrimaryBackground` — custom, use this not `.background`
- **Architecture:** `actor PIIRedactionService` (thread-safe); `@MainActor RedactionFlowViewModel: ObservableObject`
- **Vision coordinate system:** Y=0 at BOTTOM. Always convert: `uiY = 1 - vision.boundingBox.maxY`
- **SourceKit errors** "No such module 'UIKit'" are false — macOS SourceKit can't resolve iOS frameworks. Build on device/simulator to confirm real errors.

---

## Immediate Next Steps

1. **Build on device** and attach `jan2026.pdf` — verify name + address now redacted on all pages
2. **Test Chinese-Malaysian name** in header (no BIN/BINTI) — confirm NLP fallback works or add a separate detector
3. **Answer the 3 deferred UX decisions** (see above) to complete the auto-flow refactor
4. Wire up `PrivacyRedactionDebugView` in DEBUG builds
