# duit - Maybank PDF Import Architecture

## Modules
- `TransactionView` (UI): Entry point for PDF import via upload icon and file picker.
- `MaybankStatementImporter` (Import layer): Loads a local mock payload that mirrors the planned server response.
- `MaybankStatementMockParser` (Parsing layer): Converts mock payload table rows into canonical transactions.
- `DataController` (Persistence layer): Inserts parsed transactions into Core Data.
- `LogView` (Dashboard): Displays imported transactions via existing fetch requests.
- `InsightsView` (Analytics): Displays charts for last 30 days, last 6/12 months, this month, and this year.

## Data Flow
1) `TransactionView` triggers file import (`.pdf`).
2) `MaybankStatementImporter` ignores the PDF contents and loads a local mock statement payload.
3) `MaybankStatementMockParser` converts the mock table rows into `MaybankStatementTransaction`.
4) `DataController.importMaybankTransactions` persists data to Core Data.
5) `LogView` updates automatically via fetch requests.

## Types Passed Between Components
- `URL` (PDF file) -> importer (currently ignored).
- `String` (mock/server payload text) -> parser.
- `[MaybankStatementTransaction]` -> data controller.

## Boundaries
- Import/parsing logic is isolated in `app/dime/Import/MaybankStatementImporter.swift`.
- UI only coordinates file selection and progress feedback.
- Persistence uses existing Core Data container and data flow; no cloud dependencies for mock mode.
