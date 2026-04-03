import os
import re
import csv
from datetime import datetime, date

import pandas as pd
import numpy as np
from flask import (
    Blueprint, render_template, request, jsonify, redirect, url_for, flash,
    current_app,
)
from sqlalchemy import extract, func
from werkzeug.utils import secure_filename

from models import db, Transaction, Budget, CategoryGroupMapping

main_bp = Blueprint("main", __name__)
api_bp = Blueprint("api", __name__)

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

GROUPS_ORDER = [
    "Income", "Necessities", "Discretionary", "Fixed Expenses",
    "Taxes", "Savings & Investments", "Work", "Transfer", "Other",
]

ALLOWED_EXTENSIONS = {"csv", "xlsx", "xls"}


def _allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# ── Page routes ──────────────────────────────────────────────────────────────

@main_bp.route("/")
def dashboard():
    year = request.args.get("year", date.today().year, type=int)
    return render_template("dashboard.html", year=year, months=MONTHS)


@main_bp.route("/transactions")
def transactions_page():
    return render_template("transactions.html")


@main_bp.route("/budgets")
def budgets_page():
    year = request.args.get("year", date.today().year, type=int)
    return render_template("budgets.html", year=year, months=MONTHS)


@main_bp.route("/categories")
def categories_page():
    return render_template("categories.html", groups_order=GROUPS_ORDER)


@main_bp.route("/upload")
def upload_page():
    return render_template("upload.html")


# ── API: Transactions ────────────────────────────────────────────────────────

@api_bp.route("/transactions", methods=["GET"])
def get_transactions():
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    account = request.args.get("account")
    category = request.args.get("category")
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)
    search = request.args.get("search")

    q = Transaction.query

    if account:
        q = q.filter(Transaction.account == account)
    if category:
        q = q.filter(Transaction.category == category)
    if year:
        q = q.filter(extract("year", Transaction.date) == year)
    if month:
        q = q.filter(extract("month", Transaction.date) == month)
    if search:
        q = q.filter(Transaction.description.ilike(f"%{search}%"))

    q = q.order_by(Transaction.date.desc())
    result = q.paginate(page=page, per_page=per_page, error_out=False)

    return jsonify({
        "transactions": [t.to_dict() for t in result.items],
        "total": result.total,
        "pages": result.pages,
        "page": result.page,
    })


@api_bp.route("/transactions", methods=["POST"])
def create_transaction():
    data = request.get_json()
    dt = datetime.strptime(data["date"], "%Y-%m-%d").date()
    dup_key = Transaction.build_dup_key(
        dt, data["description"], data["amount"], data["account"]
    )

    if Transaction.query.filter_by(dup_key=dup_key).first():
        return jsonify({"error": "Duplicate transaction"}), 409

    txn = Transaction(
        date=dt,
        description=data["description"],
        amount=float(data["amount"]),
        account=data["account"],
        category=data.get("category", ""),
        confidence=float(data.get("confidence", 0)),
        dup_key=dup_key,
    )
    db.session.add(txn)
    db.session.commit()
    return jsonify(txn.to_dict()), 201


@api_bp.route("/transactions/<int:txn_id>", methods=["PUT"])
def update_transaction(txn_id):
    txn = Transaction.query.get_or_404(txn_id)
    data = request.get_json()

    if "date" in data:
        txn.date = datetime.strptime(data["date"], "%Y-%m-%d").date()
    if "description" in data:
        txn.description = data["description"]
    if "amount" in data:
        txn.amount = float(data["amount"])
    if "account" in data:
        txn.account = data["account"]
    if "category" in data:
        txn.category = data["category"]
    if "confidence" in data:
        txn.confidence = float(data["confidence"])
    if "subcategory" in data:
        txn.subcategory = data["subcategory"]

    txn.dup_key = Transaction.build_dup_key(
        txn.date, txn.description, txn.amount, txn.account
    )
    db.session.commit()
    return jsonify(txn.to_dict())


@api_bp.route("/transactions/<int:txn_id>", methods=["DELETE"])
def delete_transaction(txn_id):
    txn = Transaction.query.get_or_404(txn_id)
    db.session.delete(txn)
    db.session.commit()
    return jsonify({"deleted": txn_id})


# ── API: CSV Upload ──────────────────────────────────────────────────────────

@api_bp.route("/upload", methods=["POST"])
def upload_csv():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    account = request.form.get("account", "")

    if not file.filename or not _allowed_file(file.filename):
        return jsonify({"error": "Invalid file type"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(current_app.config["UPLOAD_FOLDER"], filename)
    file.save(filepath)

    try:
        df = _parse_statement(account, filepath)
    except Exception as e:
        return jsonify({"error": f"Parse error: {e}"}), 400
    finally:
        os.remove(filepath)

    # Load category-to-group mapping
    mappings = {
        m.category: m.group
        for m in CategoryGroupMapping.query.all()
    }

    added = 0
    skipped = 0

    for _, row in df.iterrows():
        dt = row["Date"].date() if hasattr(row["Date"], "date") else row["Date"]
        dup_key = Transaction.build_dup_key(
            dt, row["Description"], row["Amount"], row["Account"]
        )

        if Transaction.query.filter_by(dup_key=dup_key).first():
            skipped += 1
            continue

        txn = Transaction(
            date=dt,
            description=row["Description"],
            amount=float(row["Amount"]),
            account=row["Account"],
            category=row.get("Category", ""),
            confidence=float(row.get("Confidence", 0)),
            dup_key=dup_key,
        )
        db.session.add(txn)
        added += 1

    db.session.commit()
    return jsonify({"added": added, "skipped": skipped})


def _parse_statement(account, filepath):
    """Parse a CSV/Excel statement into a standardized DataFrame."""
    if account == "Chase_Credit_Card":
        return _parse_chase(filepath)
    elif account == "BOFA_Checking":
        return _parse_bofa_checking(filepath)
    elif account == "BOFA_Credit_Card":
        return _parse_bofa_credit_card(filepath)
    elif account == "Citi_Credit_Card":
        return _parse_citi(filepath)
    elif account == "Venmo":
        return _parse_venmo(filepath)
    elif account == "Paystub":
        return _parse_paystub(filepath)
    else:
        # Generic CSV: expects Date, Description, Amount columns
        df = pd.read_csv(filepath)
        df["Account"] = account or "Unknown"
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df["Amount"] = pd.to_numeric(
            df["Amount"].astype(str).str.replace(r"[\$,]", "", regex=True),
            errors="coerce",
        )
        df = df.dropna(subset=["Date", "Amount", "Description"])
        return df[["Date", "Description", "Amount", "Account"]]


def _parse_chase(filepath):
    df = pd.read_csv(filepath)
    df = df.rename(columns={"Transaction Date": "Date"})
    for col in ["Category", "Type"]:
        if col not in df.columns:
            df[col] = ""
    cat = df["Category"].fillna("").astype(str).str.strip()
    typ = df["Type"].fillna("").astype(str).str.strip()
    df["Description"] = (
        df["Description"].astype(str).str.strip()
        + np.where(cat != "", " - " + cat, "")
        + np.where(typ != "", " - " + typ, "")
    )
    df["Account"] = "Chase_Credit_Card"
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Amount"] = pd.to_numeric(
        df["Amount"].astype(str).str.replace(r"[\$,]", "", regex=True),
        errors="coerce",
    )
    df = df.dropna(subset=["Date", "Amount", "Description"])
    return df[["Date", "Description", "Amount", "Account"]]


def _parse_bofa_checking(filepath):
    df = pd.read_csv(filepath, skiprows=6)
    df = df[["Date", "Description", "Amount"]]
    df["Account"] = "BOFA_Checking"
    df["Amount"] = pd.to_numeric(
        df["Amount"].astype(str).str.replace('"', "").str.replace(r"[\$,]", "", regex=True).str.replace(" ", ""),
        errors="coerce",
    )
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "Amount", "Description"])
    return df[["Date", "Description", "Amount", "Account"]]


def _parse_bofa_credit_card(filepath):
    df = pd.read_csv(filepath)
    df = df.rename(columns={"Posted Date": "Date"})
    df["Description"] = df.apply(
        lambda row: " - ".join(
            filter(None, [
                str(row.get("Reference Number", "")) if not pd.isna(row.get("Reference Number")) else "",
                str(row.get("Payee", "")) if not pd.isna(row.get("Payee")) else "",
                str(row.get("Address", "")) if not pd.isna(row.get("Address")) else "",
            ])
        ), axis=1
    )
    df["Account"] = "BOFA_Credit_Card"
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Amount"] = pd.to_numeric(
        df["Amount"].astype(str).str.replace(r"[\$,]", "", regex=True),
        errors="coerce",
    )
    df = df.dropna(subset=["Date", "Amount", "Description"])
    return df[["Date", "Description", "Amount", "Account"]]


def _parse_citi(filepath):
    df = pd.read_csv(filepath)
    df["Account"] = "Citi_Credit_Card"
    df["Debit"] = pd.to_numeric(
        df["Debit"].astype(str).str.replace(r"[\$,]", "", regex=True),
        errors="coerce",
    )
    df["Credit"] = pd.to_numeric(
        df["Credit"].astype(str).str.replace(r"[\$,]", "", regex=True),
        errors="coerce",
    )
    df["Amount"] = -(df["Debit"].fillna(df["Credit"]))
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "Amount", "Description"])
    return df[["Date", "Description", "Amount", "Account"]]


def _parse_venmo(filepath):
    df = pd.read_csv(filepath, skiprows=2)
    df = df[df["ID"].notna() & df["Datetime"].notna() & df["Amount (total)"].notna()].copy()
    df = df.rename(columns={"Datetime": "Date"})

    def parse_venmo_amount(s):
        s = str(s).strip()
        if not s:
            return 0.0
        sign = -1 if s.startswith("-") else 1
        num = re.sub(r"[^\d.]", "", s)
        return sign * float(num) if num else 0.0

    df["Amount"] = df["Amount (total)"].apply(parse_venmo_amount)
    df["Description"] = (
        "Type " + df["Type"].fillna("")
        + ", From: " + df["From"].fillna("")
        + ", To: " + df["To"].fillna("")
        + ", Note: " + df["Note"].fillna("")
    )
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Account"] = "Venmo"
    return df[["Date", "Description", "Amount", "Account"]]


def _parse_paystub(filepath):
    from openpyxl import load_workbook
    df_raw = pd.read_excel(filepath, header=None).fillna("")

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

    section_rows = {}
    for idx, value in df_raw[0].items():
        if value in SECTION_TITLES:
            section_rows[value] = idx

    sorted_sections = sorted(section_rows.items(), key=lambda x: x[1])
    tables = {}

    for i, (title, start_row) in enumerate(sorted_sections):
        key = SECTION_TITLES[title]
        header_row = start_row + 1
        data_start = header_row + 1
        end_row = sorted_sections[i + 1][1] if i + 1 < len(sorted_sections) else len(df_raw)

        table = df_raw.iloc[data_start:end_row].copy()
        headers = df_raw.iloc[header_row].tolist()
        valid_cols = [j for j, h in enumerate(headers) if h != ""]
        headers = [headers[j] for j in valid_cols]
        table = table.iloc[:, valid_cols]
        table.columns = headers
        table = table.reset_index(drop=True)
        table = table[table.apply(lambda r: any(str(v).strip() for v in r), axis=1)]
        tables[key] = table

    NUMERIC_HINTS = ["Amount", "YTD", "Hours", "Rate", "Gross", "Net"]
    for df_table in tables.values():
        for col in df_table.columns:
            if any(hint in col for hint in NUMERIC_HINTS):
                df_table[col] = pd.to_numeric(
                    df_table[col].astype(str).str.replace(",", "").replace("", None),
                    errors="coerce",
                )

    # Extract pay date
    pay_date = None
    if "payslip_info" in tables and "Check Date" in tables["payslip_info"].columns:
        pay_date = pd.to_datetime(tables["payslip_info"]["Check Date"].iloc[0])
    if pay_date is None and "earnings" in tables:
        dates_col = tables["earnings"]["Dates"].dropna().astype(str)
        for val in dates_col:
            if "-" in val:
                pay_date = pd.to_datetime(val.split("-")[-1].strip())
                break

    if pay_date is None:
        raise ValueError("Could not extract pay date from paystub")

    transactions = []

    def add_txn(desc, amount):
        transactions.append({"Date": pay_date, "Description": desc, "Amount": round(float(amount), 2)})

    def add_rows(df, sign=1, prefix="", excluded=None):
        for _, row in df.iterrows():
            desc = str(row.get("Description", "")).strip()
            amount = row.get("Amount")
            if not desc or pd.isna(amount) or amount == 0:
                continue
            if excluded and desc in excluded:
                continue
            add_txn(f"{prefix}{desc}", sign * amount)

    add_rows(tables.get("earnings", pd.DataFrame()), sign=1, prefix="Paystub Income: ",
             excluded={"Imputed Income - Group Term Life", "Memo-ER Medical"})

    if "employer_benefits" in tables:
        for _, row in tables["employer_benefits"].iterrows():
            desc = str(row.get("Description", "")).strip()
            amount = row.get("Amount")
            if desc not in {"401(k) Match", "Health Savings Account"} or pd.isna(amount) or amount == 0:
                continue
            add_txn(f"Employer Benefit: {desc}", +amount)
            add_txn(f"Employer Benefit Offset: {desc}", -amount)

    add_rows(tables.get("taxes", pd.DataFrame()), sign=-1, prefix="Tax: ",
             excluded={"Pre-Tax Deductions", "Description", ""})

    if "post_tax_deductions" in tables:
        add_rows(tables["post_tax_deductions"], sign=-1, prefix="Post-Tax Deduction: ")

    df_txns = pd.DataFrame(transactions)
    df_txns["Account"] = "Paystub"
    return df_txns


# ── API: Budgets ─────────────────────────────────────────────────────────────

@api_bp.route("/budgets", methods=["GET"])
def get_budgets():
    year = request.args.get("year", date.today().year, type=int)
    budgets = Budget.query.filter_by(year=year).all()
    return jsonify([b.to_dict() for b in budgets])


@api_bp.route("/budgets", methods=["POST"])
def upsert_budget():
    data = request.get_json()
    year = int(data["year"])
    month = int(data["month"])
    category = data["category"]
    amount = float(data["amount"])

    budget = Budget.query.filter_by(
        year=year, month=month, category=category
    ).first()

    if budget:
        budget.amount = amount
    else:
        budget = Budget(year=year, month=month, category=category, amount=amount)
        db.session.add(budget)

    db.session.commit()
    return jsonify(budget.to_dict())


@api_bp.route("/budgets/bulk", methods=["POST"])
def bulk_upsert_budgets():
    entries = request.get_json()
    results = []

    for data in entries:
        year = int(data["year"])
        month = int(data["month"])
        category = data["category"]
        amount = float(data["amount"])

        budget = Budget.query.filter_by(
            year=year, month=month, category=category
        ).first()

        if budget:
            budget.amount = amount
        else:
            budget = Budget(year=year, month=month, category=category, amount=amount)
            db.session.add(budget)

        results.append(budget)

    db.session.commit()
    return jsonify([b.to_dict() for b in results])


# ── API: Category → Group Mappings ───────────────────────────────────────────

@api_bp.route("/categories", methods=["GET"])
def get_categories():
    mappings = CategoryGroupMapping.query.order_by(
        CategoryGroupMapping.group, CategoryGroupMapping.category
    ).all()
    return jsonify([m.to_dict() for m in mappings])


@api_bp.route("/categories", methods=["POST"])
def create_category():
    data = request.get_json()
    existing = CategoryGroupMapping.query.filter_by(category=data["category"]).first()
    if existing:
        return jsonify({"error": "Category already exists"}), 409

    mapping = CategoryGroupMapping(
        category=data["category"], group=data["group"]
    )
    db.session.add(mapping)
    db.session.commit()
    return jsonify(mapping.to_dict()), 201


@api_bp.route("/categories/<int:mapping_id>", methods=["PUT"])
def update_category(mapping_id):
    mapping = CategoryGroupMapping.query.get_or_404(mapping_id)
    data = request.get_json()

    if "category" in data:
        mapping.category = data["category"]
    if "group" in data:
        mapping.group = data["group"]

    db.session.commit()
    return jsonify(mapping.to_dict())


@api_bp.route("/categories/<int:mapping_id>", methods=["DELETE"])
def delete_category(mapping_id):
    mapping = CategoryGroupMapping.query.get_or_404(mapping_id)
    db.session.delete(mapping)
    db.session.commit()
    return jsonify({"deleted": mapping_id})


# ── API: Dashboard / Budget vs Actual ────────────────────────────────────────

@api_bp.route("/dashboard", methods=["GET"])
def dashboard_data():
    year = request.args.get("year", date.today().year, type=int)

    # Get category → group mappings
    mappings = {
        m.category: m.group for m in CategoryGroupMapping.query.all()
    }

    # Actual spending by category and month
    actuals_query = (
        db.session.query(
            Transaction.category,
            extract("month", Transaction.date).label("month"),
            func.sum(Transaction.amount).label("total"),
        )
        .filter(extract("year", Transaction.date) == year)
        .filter(Transaction.category != "")
        .group_by(Transaction.category, extract("month", Transaction.date))
        .all()
    )

    actuals = {}
    for cat, month, total in actuals_query:
        if cat not in actuals:
            actuals[cat] = {}
        actuals[cat][int(month)] = round(total, 2)

    # Budgets by category and month
    budgets_query = Budget.query.filter_by(year=year).all()
    budgets = {}
    for b in budgets_query:
        if b.category not in budgets:
            budgets[b.category] = {}
        budgets[b.category][b.month] = b.amount

    # Build grouped response
    all_categories = set(list(actuals.keys()) + list(budgets.keys()) + list(mappings.keys()))
    groups = {}

    for cat in all_categories:
        group = mappings.get(cat, "Other")
        if group not in groups:
            groups[group] = {}
        groups[group][cat] = {
            "actual": actuals.get(cat, {}),
            "budget": budgets.get(cat, {}),
        }

    # Sort groups by GROUPS_ORDER
    ordered = {}
    for g in GROUPS_ORDER:
        if g in groups:
            ordered[g] = dict(sorted(groups[g].items()))
    for g in sorted(groups.keys()):
        if g not in ordered:
            ordered[g] = dict(sorted(groups[g].items()))

    return jsonify({"year": year, "groups": ordered})


@api_bp.route("/accounts", methods=["GET"])
def get_accounts():
    accounts = db.session.query(Transaction.account).distinct().all()
    return jsonify([a[0] for a in accounts])
