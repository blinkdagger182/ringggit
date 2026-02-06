import re
from typing import Callable

import fitz
import pandas as pd


COMMON_STRINGS_TO_REMOVE = [
    "URUSNIAGA AKAUN/ 戶口進支項 /ACCOUNT TRANSACTIONS",
    "TARIKH MASUK",
    "BUTIR URUSNIAGA",
    "JUMLAH URUSNIAGA",
    "BAKI PENYATA",
    "進支日期",
    "進支項說明",
    "银碼",
    "結單存餘",
    "URUSNIAGA AKAUN/ 戶口進支項/ACCOUNT TRANSACTIONS",
    "TARIKH NILAI",
    "仄過賬日期",
    "戶號",
]


def _read_pdf_text(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return "".join(page.get_text() for page in doc)
    finally:
        doc.close()


def _remove_sections(lines: list[str], start_marker: str, end_marker: str) -> list[str]:
    new_lines: list[str] = []
    in_section = False
    for line in lines:
        if start_marker in line:
            in_section = True
            continue
        if end_marker in line:
            in_section = False
            continue
        if not in_section:
            new_lines.append(line)
    return new_lines


def _determine_flow(transaction_amount: str) -> str:
    if transaction_amount.endswith("+"):
        return "deposit"
    if transaction_amount.endswith("-"):
        return "withdrawal"
    return "unknown"


def _parse_m2u_debit(pdf_bytes: bytes, filename: str) -> pd.DataFrame:
    text = _read_pdf_text(pdf_bytes)
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    year_statement = None
    date_pattern = re.compile(r"\d{2}/\d{2}/\d{2}")

    for i, line in enumerate(lines):
        if "STATEMENT DATE" in line:
            for j in range(i, min(i + 5, len(lines))):
                match = date_pattern.search(lines[j])
                if match:
                    year_statement = match.group(0).split("/")[-1]
                    break
            break

    if not year_statement:
        for line in lines:
            match = date_pattern.search(line)
            if match:
                year_statement = match.group(0).split("/")[-1]
                break

    if not year_statement:
        match = re.search(r"(\d{4})(?=\d{2})", filename)
        if match:
            year_statement = match.group(1)[2:]

    if not year_statement:
        raise ValueError("Could not find statement year")

    lines = _remove_sections(lines, "Malayan Banking Berhad (3813-K)", "denoted by DR")
    lines = _remove_sections(lines, "FCN", "PLEASE BE INFORMED TO CHECK YOUR BANK ACCOUNT BALANCES REGULARLY")
    lines = _remove_sections(lines, "ENTRY DATE", "STATEMENT BALANCE")
    lines = _remove_sections(lines, "ENDING BALANCE :", "TOTAL CREDIT :")

    strings_to_remove = [
        "URUSNIAGA AKAUN/",
        "戶口進支項",
        "/ACCOUNT TRANSACTIONS",
        "TARIKH MASUK",
        "TARIKH NILAI",
        "BUTIR URUSNIAGA",
        "JUMLAH URUSNIAGA",
        "BAKI PENYATA",
        "進支日期",
        "仄過賬日期",
        "進支項說明",
        "银碼",
        "結單存餘",
        "BEGINNING BALANCE",
    ]

    filtered_lines = [line for line in lines if not any(s in line for s in strings_to_remove)]

    date_pattern = re.compile(r"\d{2}/\d{2}")
    amount_pattern = re.compile(r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?(?:[+-])?|\d+(?:\.\d{2})?(?:[+-])?)")
    structured_data: list[dict[str, object]] = []
    current_entry: dict[str, object] | None = None
    description_lines: list[str] = []

    for line in filtered_lines:
        line = line.strip()
        if date_pattern.match(line):
            if current_entry and description_lines:
                current_entry["Transaction Description"] = " ".join(description_lines).strip()
                structured_data.append(current_entry)
            current_entry = {
                "Entry Date": line,
                "Transaction Description": "",
                "Transaction Amount": None,
                "Statement Balance": None,
            }
            description_lines = []
            continue

        if not current_entry:
            continue

        amounts = amount_pattern.findall(line)
        is_amount = bool(amounts and any(amt.replace(",", "").replace(".", "").replace("+", "").replace("-", "").isdigit() for amt in amounts))
        if is_amount:
            amount_str = amounts[0]
            if ("+" in line or "-" in line) and not current_entry["Transaction Amount"]:
                current_entry["Transaction Amount"] = amount_str
                continue
            if current_entry["Transaction Amount"] and not current_entry["Statement Balance"]:
                current_entry["Statement Balance"] = amount_str
                continue

        description_lines.append(line)

    if current_entry and description_lines:
        current_entry["Transaction Description"] = " ".join(description_lines).strip()
        structured_data.append(current_entry)

    df = pd.DataFrame(structured_data)
    if df.empty:
        raise ValueError("No transactions were extracted from the PDF")

    df["Entry Date"] = pd.to_datetime(df["Entry Date"] + "/" + year_statement, format="%d/%m/%y", dayfirst=True)

    def clean_amount(val: object) -> str | None:
        if pd.isna(val) or val in (None, ""):
            return None
        clean_val = re.sub(r"[^\d.,+-]", "", str(val))
        return clean_val or None

    df["Transaction Amount"] = df["Transaction Amount"].apply(clean_amount)
    df["Statement Balance"] = df["Statement Balance"].apply(clean_amount)
    df["flow"] = df["Transaction Amount"].apply(lambda x: "inflow" if x and "+" in str(x) else "outflow" if x else None)
    df["Transaction Amount"] = df["Transaction Amount"].apply(lambda x: float(re.sub(r"[^\d.]", "", str(x))) if x else None)
    df["Statement Balance"] = df["Statement Balance"].apply(lambda x: float(re.sub(r"[^\d.]", "", str(x))) if x else None)
    return df.dropna(subset=["Transaction Amount"])


def _parse_maybank_credit(pdf_bytes: bytes, filename: str) -> pd.DataFrame:
    text = _read_pdf_text(pdf_bytes)
    year = None
    for candidate in re.findall(r"\d{4}", filename):
        if 2010 < int(candidate) < 2050:
            year = candidate
            break
    if not year:
        year = str(pd.Timestamp.now().year)

    lines = text.split("\n")
    data = [line for line in lines if not any(s in line for s in COMMON_STRINGS_TO_REMOVE)]

    final_structured_data: list[list[str]] = []
    i = 0
    while i < len(data):
        if i + 1 < len(data) and "/" in data[i] and len(data[i]) == 5 and "/" in data[i + 1] and len(data[i + 1]) == 5:
            transaction_date = data[i]
            posting_date = data[i + 1]
            i += 2

            description: list[str] = []
            amount = ""
            while i < len(data) and not ("/" in data[i] and len(data[i]) == 5):
                clean_line = data[i].strip()
                amount_match = re.match(r"^(\d{1,3}(?:,\d{3})*(\.\d{2})?)(CR)?$", clean_line, re.IGNORECASE)
                if amount_match:
                    amount = amount_match.group(1)
                    if amount_match.group(3):
                        amount = "-" + amount
                    i += 1
                    break
                description.append(clean_line)
                i += 1

            final_structured_data.append([posting_date, transaction_date, ", ".join(description), amount, year])
        else:
            i += 1

    if not final_structured_data:
        raise ValueError("No Maybank credit transactions extracted")

    df = pd.DataFrame(final_structured_data, columns=["Posting Date", "Transaction Date", "Transaction Description", "Amount", "Year"])
    df["Amount"] = df["Amount"].str.replace(",", "").replace("", None).astype(float)
    df["Year"] = df["Year"].astype("Int64")
    return df[["Year", "Posting Date", "Transaction Date", "Transaction Description", "Amount"]]


def _parse_maybank_debit(pdf_bytes: bytes, _: str) -> pd.DataFrame:
    text = _read_pdf_text(pdf_bytes)
    lines = text.split("\n")
    lines = _remove_sections(lines, "Maybank Islamic Berhad", "Please notify us of any change of address in writing.")
    lines = _remove_sections(lines, "15th Floor, Tower A, Dataran Maybank, 1, Jalan Maarof, 59000 Kuala Lumpur", "請通知本行在何地址更换。")
    lines = _remove_sections(lines, "ENTRY DATE", "STATEMENT BALANCE")
    lines = _remove_sections(lines, "ENDING BALANCE :", "TOTAL DEBIT :")

    transactions = [line for line in lines if not any(s in line for s in COMMON_STRINGS_TO_REMOVE)]
    structured_data: list[dict[str, str]] = []
    temp_entry: dict[str, str] = {}
    date_pattern = re.compile(r"\d{2}/\d{2}/\d{2}")

    for line in transactions:
        if date_pattern.match(line):
            if temp_entry:
                structured_data.append(temp_entry)
            temp_entry = {"Entry Date": line, "Transaction Description": "", "Transaction Amount": "", "Statement Balance": ""}
        elif temp_entry.get("Transaction Amount") and temp_entry.get("Statement Balance", "") == "":
            temp_entry["Statement Balance"] = line.strip()
        elif temp_entry.get("Transaction Amount", "") == "":
            temp_entry["Transaction Amount"] = line.strip()
        elif temp_entry:
            temp_entry["Transaction Description"] += line.strip() + ", "

    if temp_entry:
        structured_data.append(temp_entry)

    for entry in structured_data:
        entry["Transaction Description"] = entry["Transaction Description"].rstrip(", ")

    df = pd.DataFrame(structured_data)
    if df.empty:
        raise ValueError("No Maybank debit transactions extracted")

    df["Entry Date"] = pd.to_datetime(df["Entry Date"], format="%d/%m/%y", dayfirst=True).dt.date
    df["Statement Balance 2"] = df["Transaction Description"].str.extract(r"(\d+,\d+\.\d+)")[0]
    df["Statement Balance 2"] = df["Statement Balance 2"].str.replace(",", "").astype(float)
    df["Transaction Description"] = df["Transaction Description"].str.replace(r"\d+,\d+\.\d+, ", "", regex=True)
    df["Transaction Description"] = df["Transaction Description"].str.replace(r", (\d{1,3}(?:,\d{3})*(?:\.\d{2}))$", "", regex=True)

    df = df[["Entry Date", "Transaction Amount", "Transaction Description", "Statement Balance", "Statement Balance 2"]]
    df = df.rename(columns={"Transaction Amount": "Transaction Type", "Statement Balance": "Transaction Amount", "Statement Balance 2": "Statement_Balance"})
    df.loc[df["Transaction Type"] == "CASH WITHDRAWAL", "Transaction Description"] = "CASH WITHDRAWAL"
    df.loc[df["Transaction Type"] == "DEBIT ADVICE", "Transaction Description"] = "Card Annual Fee"
    df.loc[df["Transaction Type"] == "PROFIT PAID", "Transaction Description"] = "PROFIT PAID"
    df["flow"] = df["Transaction Amount"].apply(_determine_flow)
    df["Transaction Amount"] = df["Transaction Amount"].str.replace("+", "", regex=False).str.replace("-", "", regex=False)
    df["Transaction Amount"] = df["Transaction Amount"].str.replace(",", "").astype(float)
    return df


def _remove_close_dates(data: list[str]) -> list[str]:
    valid_dates_indices: list[int] = []
    i = 0
    while i < len(data):
        if re.match(r"\d{2}/\d{2}/\d{4}", data[i]):
            valid_dates_indices.append(i)
            i += 4
        else:
            i += 1
    return [data[idx] for idx in range(len(data)) if idx in valid_dates_indices or not re.match(r"\d{2}/\d{2}/\d{4}", data[idx])]


def _is_pure_number(s: str) -> bool:
    s = s.replace(" ", "")
    return s.isnumeric() and not any(c in s for c in ".,")


def _parse_cimb_debit(pdf_bytes: bytes, _: str) -> pd.DataFrame:
    text = _read_pdf_text(pdf_bytes)
    lines = text.split("\n")
    lines = _remove_sections(lines, "Page / Halaman", "ISLAMIC BBB-PPPP")

    filtered_lines = [line for line in lines if not any(s in line for s in COMMON_STRINGS_TO_REMOVE)]
    data = _remove_close_dates(filtered_lines)
    data = [item for item in data if not _is_pure_number(item)]
    data = [item if item != "99 SPEEDMART-2133" else "ninetynine speed mart" for item in data]

    final_structured_data: list[dict[str, str]] = []
    i = 0
    while i < len(data):
        transaction: dict[str, str] = {}
        if data[i] == "OPENING BALANCE":
            transaction["Date"] = "-"
            transaction["Transaction Type/Description"] = "Opening Balance"
            i += 1
            transaction["Balance After Transaction"] = "-"
            transaction["Amount"] = data[i].strip()
            transaction["Beneficiary/Payee Name"] = "-"
            final_structured_data.append(transaction)
            i += 1
            continue

        if re.match(r"\d{2}/\d{2}/\d{4}", data[i]):
            transaction["Date"] = data[i]
            i += 1
            description_lines: list[str] = []
            while i < len(data) and not re.match(r"\d{2}/\d{2}/\d{4}", data[i]) and not re.match(r"^-?\d", data[i].strip()):
                if data[i].strip():
                    description_lines.append(data[i].strip())
                i += 1

            transaction["Transaction Type/Description"] = ", ".join(description_lines)
            if i < len(data) and re.match(r"^-?\d", data[i].strip()):
                transaction["Amount"] = data[i].strip()
                i += 1

            balance_line = data[i].strip() if i < len(data) else ""
            while not balance_line and i < len(data):
                i += 1
                balance_line = data[i].strip() if i < len(data) else ""

            transaction["Balance After Transaction"] = balance_line
            transaction["Beneficiary/Payee Name"] = description_lines[0] if description_lines else "-"
            final_structured_data.append(transaction)
            continue

        i += 1

    if not final_structured_data:
        raise ValueError("No CIMB debit transactions extracted")

    df = pd.DataFrame(final_structured_data)
    df["Transaction Description2"] = df["Transaction Type/Description"].apply(lambda x: " ".join(x.split()[1:]))
    df["Transaction Description"] = df["Transaction Description2"] + ", " + df["Beneficiary/Payee Name"]
    df.drop(columns=["Transaction Type/Description", "Beneficiary/Payee Name"], inplace=True)

    for i in range(1, len(df)):
        prev_balance = pd.to_numeric(df.loc[i - 1, "Balance After Transaction"], errors="coerce")
        curr_balance = pd.to_numeric(df.loc[i, "Balance After Transaction"], errors="coerce")
        df.loc[i, "output"] = "deposit" if curr_balance > prev_balance else "withdrawal"

    df["Transaction Description2"] = df["Transaction Description2"].replace("Balance", "Opening Balance")
    df["Transaction Description"] = df["Transaction Description"].replace("Balance, -", "Opening Balance")
    df[["Date", "Transaction Type"]] = df["Date"].str.extract(r"(\S+)\s(.*)")
    return df[["Date", "Transaction Type", "Transaction Description", "Transaction Description2", "Amount", "Balance After Transaction", "output"]]


def _parse_rhb_flex(pdf_bytes: bytes, _: str) -> pd.DataFrame:
    text = _read_pdf_text(pdf_bytes)
    transactions: list[dict[str, object]] = []
    current_transaction: dict[str, object] | None = None

    for raw_line in text.split("\n"):
        line = raw_line.strip()
        date_match = re.match(r"(\d{2}-\d{2}-\d{4}|\d{2}-\d{2}-\d{2})", line)
        if date_match:
            if current_transaction is not None:
                transactions.append(current_transaction)
            current_transaction = {"Date": date_match.group(1), "Lines": []}
        elif current_transaction is not None:
            current_transaction["Lines"].append(line)

    if current_transaction is not None:
        transactions.append(current_transaction)

    rows: list[dict[str, str]] = []
    transaction_types = [
        "DUITNOW QR POS CR",
        "INWARD IBG",
        "RFLX",
        "DUITNOW",
        "RPP INWARD INST TRF",
        "LOCAL CHQ",
        "REFLEX-FUNDS TFR DR",
        "MB FUND",
        "CASH DEPOSIT",
        "RPP INWARD",
        "REFLEX-FUNDS TFR",
        "REFLEX- FUNDS TFR DR",
        "RFLX INSTANT TRF DR",
        "RFLX INSTANT TRF SC",
    ]

    for txn in transactions:
        combined_text = " ".join(txn["Lines"]).strip()
        description = ""
        amount_dr = ""
        amount_cr = ""

        amount_match = re.search(r"([\d,]+\.\d{2})\s*(DR|CR|\+|\-)?$", combined_text)
        if amount_match:
            amount = amount_match.group(1).replace(",", "")
            sign = amount_match.group(2)
            if sign in ["DR", "-"]:
                amount_dr = amount
            elif sign in ["CR", "+"] or sign is None:
                amount_cr = amount
            combined_text = combined_text[: amount_match.start()].strip()

        for t_type in transaction_types:
            if t_type in combined_text:
                description = t_type
                combined_text = combined_text.replace(description, "").strip()
                break

        rows.append(
            {
                "Date": txn["Date"],
                "Description": description,
                "Sender/Beneficiary": combined_text,
                "Amount (DR)": amount_dr,
                "Amount (CR)": amount_cr,
            }
        )

    if not rows:
        raise ValueError("No RHB Flex transactions extracted")

    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce").fillna(pd.to_datetime(df["Date"], format="%d-%m-%y", errors="coerce"))
    df["Date"] = df["Date"].dt.strftime("%d-%m-%y")

    def process_sender_beneficiary(s: str) -> pd.Series:
        s = s.strip()
        balance = ""
        new_sender_beneficiary = s
        recipient_reference = ""
        match = re.match(r"^([\d,]+\.\d{2}\+)\s*(.*)", s)
        if not match:
            return pd.Series([balance, new_sender_beneficiary, recipient_reference])

        balance = match.group(1)
        remaining_text = match.group(2)
        words = remaining_text.split()
        new_sender_beneficiary = " ".join(words[:3])
        recipient_reference = " ".join(words[3:])

        cleaned_tokens: list[str] = []
        for token in recipient_reference.split():
            if len(token) >= 8 and re.search(r"[A-Za-z]", token) and re.search(r"\d", token):
                continue
            if re.match(r"^\d{3}$", token):
                continue
            if re.match(r"^\d{8,}$", token):
                continue
            cleaned_tokens.append(token)
        recipient_reference = " ".join(cleaned_tokens)

        unwanted_patterns = [
            r"06/\s*\d+\s*/\s*-\s*",
            r"/\s*\d{3,}\s*/\s*-\s*",
            r"www\.rhbgroup\.com.*",
            r"For Any Enquiries.*",
            r"Date Branch Description.*",
            r"Reference 1 / Recipient's Reference.*",
            r"Reference 2 / Other Payment Details.*",
            r"RefNum.*",
            r"Amount \(DR\).*",
            r"Amount \(CR\).*",
            r"Balance Sender's / Beneficiary's Name.*",
            r"Sender's / Beneficiary's Name.*",
        ]
        for pattern in unwanted_patterns:
            recipient_reference = re.sub(pattern, "", recipient_reference, flags=re.IGNORECASE)
        recipient_reference = " ".join(recipient_reference.split())
        return pd.Series([balance, new_sender_beneficiary, recipient_reference])

    df[["Balance", "Sender/Beneficiary", "Recipient Reference"]] = df["Sender/Beneficiary"].apply(process_sender_beneficiary)
    df["Recipient Reference"] = df["Recipient Reference"].shift(1)
    df["Amount (DR)"] = df["Amount (DR)"].shift(1)
    df["Amount (CR)"] = df["Amount (CR)"].shift(1)
    return df.reset_index(drop=True)


MODE_HANDLERS: dict[str, Callable[[bytes, str], pd.DataFrame]] = {
    "maybank_debit": _parse_maybank_debit,
    "maybank_credit": _parse_maybank_credit,
    "cimb_debit": _parse_cimb_debit,
    "m2u_current_account_debit": _parse_m2u_debit,
    "rhb_flex": _parse_rhb_flex,
}
