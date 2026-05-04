# duit - Maybank PDF Import

## Requirements
- Support Maybank statement imports via the upload entry in `TransactionView`.
- Parsing is offloaded (server planned); for now the app uses a local mock statement payload that matches the server output format.
- Imported transactions must appear in the existing LogView (today/this week/this month + upcoming recurring).
- No cloud dependencies for the current implementation (mock data is on-device).
- Import entry point is in `TransactionView` as an upload icon following current UI/UX style.
- Insights time filter must support: last 30 days, last 6 months, last 12 months, this month, this year.

## Data Types
### Canonical import transaction
- `date: Date`
- `description: String`
- `amount: Double`
- `debitCredit: Enum` (`debit` or `credit`)
- `accountSource: String?`
- `reference: String?`

### Core Data Transaction (new fields)
- `accountSource: String?` (Maybank account metadata)
- `reference: String?` (statement reference/trace id)

## File Formats
- Input: PDF statements (`.pdf`) from Maybank (upload only, content ignored in mock mode).
- Import payload (mock/server format): markdown-like table with columns `ENTRY DATE`, `TRANSACTION DESCRIPTION`, `TRANSACTION AMOUNT`, `STATEMENT BALANCE`.

## Parsing Rules
- Only parse table rows starting with `|` after the `ACCOUNT TRANSACTIONS` section.
- Columns are mapped as: date, description, amount, balance.
- Date formats supported: `dd/MM/yy`, `dd/MM/yyyy`.
- Amount parsing:
  - Accept comma thousands separators.
  - Trailing `+` means credit, trailing `-` means debit.
- Ignore summary rows (BEGINNING BALANCE, ENDING BALANCE, TOTAL CREDIT, TOTAL DEBIT, TOTAL EDIT).
- `accountSource`: default to `Maybank` in mock mode.

## Acceptance Criteria
- User can tap the upload icon in `TransactionView` to select a PDF.
- If PDF text extraction is insufficient, OCR is used automatically.
- Parsed transactions are persisted to Core Data and visible in LogView without app restart.
- Import runs fully on-device (no network usage).
