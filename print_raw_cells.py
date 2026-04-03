import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_KEY = "1UoIaxcnFor39N_KcIwnq8JWaauPzrDWEv_S5PFcca1M"
CREDS_FILE = r"C:\Users\hokie\OneDrive\Desktop\git\budget_sheet_scripting\transactions-project-483516-f68f182fae44.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

spreadsheet = client.open_by_key(SPREADSHEET_KEY)

for sheet_name in ["Transactions Overview 2025", "Budget 2025"]:
    print(f"\n{'='*60}")
    print(f"SHEET: {sheet_name}")
    print(f"{'='*60}")
    worksheet = spreadsheet.worksheet(sheet_name)
    all_rows = worksheet.get_all_values()
    for i, row in enumerate(all_rows, start=1):
        print(f"Row {i}: {row}")

print("\nDone.")
