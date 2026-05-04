# Patch Notes

## 2026-01-26
- Switched Maybank import to use a local mock payload that mirrors the planned server response.
- Replaced PDFKit/OCR parsing logic with a table-row parser for the mock format.
- Kept upload flow intact (PDF selection still required, content ignored in mock mode).

What to test:
- Import flow still starts from the upload icon.
- Imported transactions show full descriptions from the mock table.
- Summary rows (TOTAL CREDIT/DEBIT/EDIT, ENDING BALANCE) are excluded.

Known Limitations:
- PDF content is ignored until server parsing is wired in.

## 2026-01-26
- Updated Insights time filter options: last 30 days, last 6 months, last 12 months, this month, this year.
- Added rolling period graph views and data aggregation for new ranges.
- Insights auto-falls back to last 6 months when last 30 days has no data.
- Insights auto-selects expenses when the selected range has no income data.
- Added automatic fix for two-digit year dates (e.g., 25 -> 2025) on imported transactions.
- Fixed accessibility formatting warning in LogView (string formatted with numeric formatter).

What to test:
- Insights menu shows the new five options only.
- Each option renders a graph and list for the correct range.

## 2026-01-26
- Added Maybank PDF statement import (PDFKit extraction with Vision OCR fallback).
- Added Maybank statement parser and on-device import pipeline into Core Data.
- Added upload icon in `TransactionView` to trigger PDF import.
- Added `accountSource` and `reference` fields to `Transaction` Core Data entity.

Migration Notes:
- Core Data model updated with two new optional fields; no user action required.

Known Limitations:
- OCR accuracy depends on scan quality; some descriptions may require manual cleanup.
- No automated tests added (no existing test target in repo).
