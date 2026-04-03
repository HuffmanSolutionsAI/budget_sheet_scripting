import os
import re
import csv
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import joblib
import numpy as np

# --------------------------
# Configuration
# --------------------------
STATEMENTS_DIR = "statements"
CONF_THRESHOLD = 0.8
SHEET_NAME = "All Transactions"
SPREADSHEET_KEY = "1UoIaxcnFor39N_KcIwnq8JWaauPzrDWEv_S5PFcca1M"


# Chase Transactions
def process_chase_credit_card_transactions(file_name):
    # --------------------------
    # Read CSV
    # --------------------------
    df_csv = pd.read_csv(file_name)

    df_csv = df_csv.rename(columns={
        "Transaction Date": "Date",
        "Description": "Description",
        "Amount": "Amount"
    })

    # Ensure the columns exist (avoid KeyError)
    for col in ["Category", "Type"]:
        if col not in df_csv.columns:
            df_csv[col] = ""

    cat = df_csv["Category"].fillna("").astype(str).str.strip()
    typ = df_csv["Type"].fillna("").astype(str).str.strip()

    df_csv["Description"] = (
        df_csv["Description"].astype(str).str.strip()
        + np.where(cat != "", " - " + cat, "")
        + np.where(typ != "", " - " + typ, "")
    )

    df_csv["Account"] = "Chase_Credit_Card"
    df_csv = df_csv[["Date", "Description", "Amount", "Account"]]

    df_csv["Date"] = pd.to_datetime(df_csv["Date"], errors="coerce")
    df_csv["Amount"] = (
        df_csv["Amount"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .astype(float)
    )

    df_csv = df_csv.dropna(subset=["Date", "Amount", "Description"])
    return df_csv

def process_bofa_checking_transactions(file_name):
    header_row_idx = 4  # adjust if your CSV format changes

    # df_csv2 = pd.read_csv(
    #     file_name,
    #     header=header_row_idx,
    #     engine="python",
    #     dtype=str,
    #     quoting=csv.QUOTE_NONE,
    #     skip_blank_lines=False
    # )


    # Read CSV using Python engine, ignore quotes (malformed quotes handled)
    df_csv = pd.read_csv(file_name, skiprows=6)

    # Keep only the columns we care about
    df_csv = df_csv[["Date", "Description", "Amount"]]

    # Add Account column
    df_csv["Account"] = "BOFA_Checking"

    # Clean Amount column: remove $, commas, spaces, convert to float
    df_csv["Amount"] = (
        df_csv["Amount"]
        .astype(str)
        .str.replace('"', '', regex=False)   # remove extra quotes
        .str.replace(r"[\$,]", "", regex=True)  # remove $ and ,
        .str.replace(" ", "", regex=False)      # remove spaces
        .astype(float)
    )

    # Parse Dates
    df_csv["Date"] = pd.to_datetime(df_csv["Date"], errors="coerce")

    # Drop rows missing critical info
    df_csv = df_csv.dropna(subset=["Date", "Amount", "Description"])

    return df_csv

def process_bofa_credit_card_transactions(file_name):
    # --------------------------
    # Read CSV
    # --------------------------
    df_csv = pd.read_csv(file_name)

    df_csv = df_csv.rename(columns={
        "Posted Date": "Date"
    })

    df_csv["Description"] = df_csv.apply(
        lambda row: " - ".join(
            filter(None, [
                str(row["Reference Number"]) if not pd.isna(row["Reference Number"]) else "",
                str(row["Payee"]) if not pd.isna(row["Payee"]) else "",
                str(row["Address"]) if not pd.isna(row["Address"]) else ""
            ])
        ),
        axis=1
    )

    df_csv["Account"] = "BOFA_Credit_Card"
    df_csv = df_csv[["Date", "Description", "Amount", "Account"]]

    df_csv["Date"] = pd.to_datetime(df_csv["Date"], errors="coerce")
    df_csv["Amount"] = (
        df_csv["Amount"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .astype(float)
    )

    df_csv = df_csv.dropna(subset=["Date", "Amount", "Description"])
    return df_csv

def parse_venmo_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, skiprows=2)

    df = df[
        df["ID"].notna() &
        df["Datetime"].notna() &
        df["Amount (total)"].notna()
    ].copy()

    df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")

    return df.reset_index(drop=True)

def parse_venmo_amount(s):
    """
    Convert a Venmo amount string to float.
    Handles:
      - '- $13.00', '- 13.00', '-\xa013.00' (NBSP)
      - '+ $170.00'
      - '$1,727.09'
    """
    s = str(s).strip()  # remove leading/trailing whitespace
    if s == "":
        return 0.0

    # Detect sign
    sign = 1
    if s.startswith("-"):
        sign = -1
    elif s.startswith("+"):
        sign = 1

    # Remove everything except digits and dot
    num = re.sub(r"[^\d.]", "", s)

    if num == "":
        return 0.0

    return sign * float(num)

# Come back to this
def process_venmo_transactions(file_name):
    # --------------------------
    # Read CSV
    # --------------------------
    df = parse_venmo_csv(file_name)

    # --------------------------
    # Rename columns
    # --------------------------
    df = df.rename(columns={
        "Datetime": "Date"
    })

    # --------------------------
    # Normalize Amount
    # --------------------------
    df["Amount"] = df["Amount (total)"].apply(parse_venmo_amount)


    # --------------------------
    # Build Description (vectorized)
    # --------------------------
    df["Description"] = (
        "Type " + df["Type"].fillna("")
        + ", From: " + df["From"].fillna("")
        + ", To: " + df["To"].fillna("")
        + ", Note: " + df["Note"].fillna("")
    )

    # --------------------------
    # Clean Date
    # --------------------------
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    df["Account"] = "Venmo"

    # --------------------------
    # Final column order
    # --------------------------
    return df[["Date", "Description", "Amount", "Account"]]

def process_citi_bank_credit_card_transactions(file_name):
    # --------------------------
    # Read CSV
    # --------------------------
    df = pd.read_csv(file_name)

    df["Account"] = "Citi_Credit_Card"

    df["Debit"] = (
        df["Debit"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .replace("", np.nan)
        .astype(float)
    )

    df["Credit"] = (
        df["Credit"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .replace("", np.nan)
        .astype(float)
    )

    # Use Debit if present, otherwise Credit
    df["Amount"] = df["Debit"].fillna(df["Credit"])

    # Reverse sign
    df["Amount"] = -df["Amount"]

    df = df[["Date", "Description", "Amount", "Account"]]

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Amount"] = (
        df["Amount"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .astype(float)
    )

    df = df.dropna(subset=["Date", "Amount", "Description"])
    return df

def parse_paystub(csv_path: str) -> dict[str, pd.DataFrame]:
    """
    Parse a vertically-stacked paystub CSV into logical DataFrames.

    Returns:
        dict[str, pd.DataFrame]
        Keys include:
        - company_info
        - payslip_info
        - totals
        - earnings
        - taxes
        - post_tax_deductions
        - employer_benefits
        - subject_wages
        - payment_info
    """

    # --------------------------
    # Read raw CSV (no headers)
    # --------------------------
    df = pd.read_excel(csv_path, header=None)
    df = df.fillna("")

    # --------------------------
    # Section header labels
    # --------------------------
    SECTION_TITLES = {
        "Company Information": "company_info",
        "Payslip Information": "payslip_info",
        "Current and YTD Totals": "totals",
        "Earnings": "earnings",
        "Employee Taxes Withheld": "taxes",
        "Pre-Tax Deductions": "pre_tax_deductions",
        "Post-Tax Deductions": "post_tax_deductions",
        "Employer Paid Benefits": "employer_benefits",
        "Subject Wages": "subject_wages",
        "Payment Information": "payment_info",
    }

    # --------------------------
    # Locate section start rows
    # --------------------------
    section_rows = {}

    for idx, value in df[0].items():
        if value in SECTION_TITLES:
            section_rows[value] = idx

    # Preserve order
    sorted_sections = sorted(
        section_rows.items(),
        key=lambda x: x[1]
    )

    results = {}

    # --------------------------
    # Extract each section
    # --------------------------
    for i, (title, start_row) in enumerate(sorted_sections):
        key = SECTION_TITLES[title]

        # Header is always next row
        header_row = start_row + 1
        data_start = header_row + 1

        # End is next section start or EOF
        if i + 1 < len(sorted_sections):
            end_row = sorted_sections[i + 1][1]
        else:
            end_row = len(df)

        table = df.iloc[data_start:end_row].copy()
        headers = df.iloc[header_row].tolist()

        # Trim empty columns
        valid_cols = [i for i, h in enumerate(headers) if h != ""]
        headers = [headers[i] for i in valid_cols]
        table = table.iloc[:, valid_cols]

        table.columns = headers
        table = table.reset_index(drop=True)

        # Drop fully empty rows
        table = table[
            table.apply(lambda r: any(str(v).strip() for v in r), axis=1)
        ]

        results[key] = table

    # --------------------------
    # Numeric cleanup
    # --------------------------
    NUMERIC_COL_HINTS = [
        "Amount", "YTD", "Hours", "Rate", "Gross", "Net"
    ]

    for df_name, df_table in results.items():
        for col in df_table.columns:
            if any(hint in col for hint in NUMERIC_COL_HINTS):
                df_table[col] = (
                    df_table[col]
                    .astype(str)
                    .str.replace(",", "")
                    .replace("", None)
                )
                df_table[col] = pd.to_numeric(df_table[col], errors="coerce")

    return results

def extract_pay_date(tables: dict) -> pd.Timestamp:
    """
    Extracts the Check Date from payslip_info.
    Example: '03/20/2026' → 2026-03-20
    """
    # Use Check Date from payslip_info (the actual payment date)
    if "payslip_info" in tables:
        payslip_df = tables["payslip_info"]
        if "Check Date" in payslip_df.columns:
            check_date = payslip_df["Check Date"].iloc[0]
            if check_date:
                return pd.to_datetime(check_date)
    
    # Fallback: use the last date from Earnings (pay period end)
    earnings_df = tables["earnings"]
    dates_col = earnings_df["Dates"].dropna().astype(str)
    for val in dates_col:
        if "-" in val:
            end_date = val.split("-")[-1].strip()
            return pd.to_datetime(end_date)

    raise ValueError("Could not extract pay date from tables")

def paystub_to_transactions(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    pay_date = extract_pay_date(tables)
    transactions = []

    def add_txn(desc, amount):
        transactions.append({
            "Date": pay_date,
            "Description": desc,
            "Amount": round(float(amount), 2)
        })

    def add_rows(
        df,
        sign=1,
        prefix="",
        allowed_descriptions=None,
        excluded_descriptions=None
    ):
        for _, row in df.iterrows():
            desc = str(row.get("Description", "")).strip()
            amount = row.get("Amount")

            if not desc or pd.isna(amount) or amount == 0:
                continue

            if allowed_descriptions and desc not in allowed_descriptions:
                continue

            if excluded_descriptions and desc in excluded_descriptions:
                continue

            add_txn(
                f"{prefix}{desc}",
                sign * amount
            )

    # --------------------------
    # Earnings (+) — EXCLUDE TERM LIFE
    # --------------------------
    add_rows(
        tables["earnings"],
        sign=+1,
        prefix="Paystub Income: ",
        excluded_descriptions={
            "Imputed Income - Group Term Life",
            "Memo-ER Medical"
        }
    )

    # --------------------------
    # Employer Benefits (+ + OFFSET)
    # --------------------------
    BENEFITS_ALLOWLIST = {
        "401(k) Match",
        "Health Savings Account"
    }

    if "employer_benefits" in tables:
        for _, row in tables["employer_benefits"].iterrows():
            desc = str(row.get("Description", "")).strip()
            amount = row.get("Amount")

            if desc not in BENEFITS_ALLOWLIST or pd.isna(amount) or amount == 0:
                continue

            # Positive asset contribution
            add_txn(
                f"Employer Benefit: {desc}",
                +amount
            )

            # Negative offset (cash-neutral)
            add_txn(
                f"Employer Benefit Offset: {desc}",
                -amount
            )

    # --------------------------
    # Taxes (-)
    # --------------------------
    add_rows(
        tables["taxes"],
        sign=-1,
        prefix="Tax: ",
        excluded_descriptions={
            "Pre-Tax Deductions",
            "Description",
            ""
        }
    )

    # --------------------------
    # Post-tax deductions (-)
    # --------------------------
    if "post_tax_deductions" in tables:
        add_rows(
            tables["post_tax_deductions"],
            sign=-1,
            prefix="Post-Tax Deduction: "
        )

    return pd.DataFrame(transactions)

def process_paystub_transactions(file_name):
    # --------------------------
    # Read CSV
    # --------------------------
    tables = parse_paystub(file_name)
    df_txns = paystub_to_transactions(tables)
    df_txns["Account"] = "Paystub"
    print(df_txns)

    # assert round(
    #     paystub_to_transactions(tables)["Amount"].sum(), 2
    # ) == round(
    #     tables["totals"]
    #         .query("`Balance Period` == 'Current'")["Net Pay"]
    #         .iloc[0],
    #     2
    # ), "Paystub parsing imbalanced."

    return df_txns

def process_statement(account, file_name):

    # --------------------------
    # Google Sheets auth
    # --------------------------
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(
        "transactions-project-483516-f68f182fae44.json",
        scopes=scopes
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_KEY).worksheet(SHEET_NAME)

    # --------------------------
    # Load model + encoder
    # --------------------------
    pipeline = joblib.load("transaction_model.pkl")
    le = joblib.load("label_encoder.pkl")

    # --------------------------
    # Load Category → Group mapping
    # --------------------------
    cat_ws = client.open_by_key(SPREADSHEET_KEY).worksheet("Categories")
    cat_data = cat_ws.get_all_records()
    cat_to_group = {row["Category"]: row["Group"] for row in cat_data if row["Category"]}


    if account == "BOFA_Checking":
        df_csv = process_bofa_checking_transactions(file_name)
    elif account == "BOFA_Credit_Card":
        df_csv = process_bofa_credit_card_transactions(file_name)
    elif account == "Chase_Credit_Card":
        df_csv = process_chase_credit_card_transactions(file_name)
    elif account == "Citi_Credit_Card":
        df_csv = process_citi_bank_credit_card_transactions(file_name)
    elif account == "Venmo":
        df_csv = process_venmo_transactions(file_name)
    elif account == "Paystub":
        df_csv = process_paystub_transactions(file_name)
    else:
        df_csv = pd.DataFrame()
        print("Invalid ACCOUNT name")

    # --------------------------
    # Fetch existing transactions
    # --------------------------
    existing_rows = sheet.get_all_records()
    df_existing = pd.DataFrame(existing_rows)

    # --------------------------
    # Duplicate detection (NON-DESTRUCTIVE)
    # --------------------------
    def build_dup_key(df):
        # Normalize date
        date_part = (
            pd.to_datetime(df["Date"], errors="coerce")
            .dt.strftime("%Y-%m-%d")
        )

        # Normalize description (ONLY for key)
        desc_part = (
            df["Description"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        # Normalize amount (handle $, commas, blanks)
        amount_part = (
            df["Amount"]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .str.strip()
        )

        amount_part = pd.to_numeric(amount_part, errors="coerce").round(2)

        account_part = df["Account"]

        return (
            date_part
            + "|"
            + desc_part
            + "|"
            + amount_part.astype(str)
            + "|"
            + account_part
        )


    df_csv["_dup_key"] = build_dup_key(df_csv)
    df_existing["_dup_key"] = build_dup_key(df_existing)

    df_new = df_csv[~df_csv["_dup_key"].isin(df_existing["_dup_key"])].copy()

    print(f"Found {len(df_new)} new transactions to add.")

    if df_new.empty:
        print("No new transactions to process.")
    else:

        # --------------------------
        # Feature engineering
        # --------------------------
        df_new["abs_amount"] = df_new["Amount"].abs()
        df_new["is_income"] = (df_new["Amount"] > 0).astype(int)
        df_new["weekday"] = df_new["Date"].dt.weekday
        df_new["is_weekend"] = df_new["weekday"].isin([5, 6]).astype(int)
        df_new["Month"] = df_new["Date"].dt.month.astype(str)

        X_pred = df_new[
            ["Description", "Account", "Month",
            "abs_amount", "is_income", "weekday", "is_weekend"]
        ].copy()

        X_pred["Description"] = X_pred["Description"].astype(str)
        X_pred["Account"] = X_pred["Account"].astype(str)
        X_pred["abs_amount"] = X_pred["abs_amount"].astype(float)
        X_pred["is_income"] = X_pred["is_income"].astype(int)
        X_pred["weekday"] = X_pred["weekday"].astype(int)
        X_pred["is_weekend"] = X_pred["is_weekend"].astype(int)

        # --------------------------
        # Predict categories
        # --------------------------
        try:
            probs = pipeline.predict_proba(X_pred)
            pred_idx = np.argmax(probs, axis=1)
            pred_categories = le.inverse_transform(pred_idx)
            confidences = probs[np.arange(len(probs)), pred_idx]

            df_new["Category"] = [
                cat if conf >= CONF_THRESHOLD else ""
                for cat, conf in zip(pred_categories, confidences)
            ]
            df_new["Confidence"] = confidences.round(3)
            df_new["Group"] = df_new["Category"].map(cat_to_group).fillna("")

        except:
            print("No empty categories to predict.")
            df_new["Group"] = ""

        # --------------------------
        # Append to Google Sheets (header-aware)
        # --------------------------
        headers = sheet.row_values(1)
        col_map = {h: i for i, h in enumerate(headers)}

        required_headers = {
            "Date", "Description", "Amount",
            "Account", "Category", "Confidence"
        }


        missing = required_headers - set(headers)
        if missing:
            raise ValueError(f"Missing required sheet columns: {missing}")

        rows_to_append = []

        for _, row in df_new.iterrows():
            sheet_row = [""] * len(headers)

            sheet_row[col_map["Date"]] = row["Date"].strftime("%Y-%m-%d")
            sheet_row[col_map["Description"]] = row["Description"]  # UNMODIFIED
            sheet_row[col_map["Amount"]] = row["Amount"]
            sheet_row[col_map["Account"]] = row["Account"]
            sheet_row[col_map["Category"]] = row["Category"]
            sheet_row[col_map["Confidence"]] = row["Confidence"]
            if "Group" in col_map:
                sheet_row[col_map["Group"]] = row["Group"]

            rows_to_append.append(sheet_row)

        sheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"Appended {len(rows_to_append)} new transactions.")


# Loop through all files in the folder
for file_name in os.listdir(STATEMENTS_DIR):
    file_path = os.path.join(STATEMENTS_DIR, file_name)
    
    # Skip directories, only process files
    if not os.path.isfile(file_path):
        continue

    # Determine account based on file name
    account = None
    fname_lower = file_name.lower()  # make it case-insensitive

    if "venmo" in fname_lower:
        account = "Venmo"
    if "_9389" in fname_lower:
        account = "BOFA_Credit_Card"
    elif "jake_huffman" in fname_lower:
        account = "Paystub"
    elif "chase" in fname_lower:
        account = "Chase_Credit_Card"
    elif "stmt" in fname_lower:
        account = "BOFA_Checking"
    elif "since" in fname_lower:
        account = "Citi_Credit_Card"
    elif "date range" in fname_lower:
        account = "Citi_Credit_Card"

    # Process the statement if account identified
    if account:
        print(f"Processing {file_name} as {account} statement...")
        process_statement(account, file_path)
    else:
        print(f"Could not categorize file: {file_name}")