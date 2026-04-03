#!/usr/bin/env python3
"""
Apply formatting to Budget sheets.
Run this after update_sheets.py to add colors.
"""

import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import CellFormat, TextFormat, Color, format_cell_range, set_column_widths
import time

SPREADSHEET_KEY = "1UoIaxcnFor39N_KcIwnq8JWaauPzrDWEv_S5PFcca1M"
CREDENTIALS_FILE = "transactions-project-483516-f68f182fae44.json"

GROUPS_ORDER = ['Income', 'Necessities', 'Discretionary', 'Fixed Expenses', 'Taxes', 
                'Savings & Investments', 'Work', 'Transfer', 'Other']

print("Connecting...")
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_KEY)

# Get categories
ws_cat = sheet.worksheet('Categories')
cat_data = ws_cat.get_all_values()
cat_to_group = {}
for row in cat_data[1:]:
    if row[0] and row[1]:
        cat_to_group[row[0]] = row[1]

group_cats = {g: [] for g in GROUPS_ORDER}
for cat, grp in cat_to_group.items():
    if grp in group_cats:
        group_cats[grp].append(cat)
for g in group_cats:
    group_cats[g].sort()

# Formats
header_fmt = CellFormat(
    backgroundColor=Color(0.2, 0.4, 0.8),
    textFormat=TextFormat(bold=True, foregroundColor=Color(1, 1, 1)),
    horizontalAlignment='center'
)

group_header_fmt = CellFormat(
    backgroundColor=Color(0.7, 0.7, 0.7),
    textFormat=TextFormat(bold=True, fontSize=12)
)

total_fmt = CellFormat(
    backgroundColor=Color(0.85, 0.85, 0.85),
    textFormat=TextFormat(bold=True)
)

def apply_format(ws_name):
    ws = sheet.worksheet(ws_name)
    print(f"Formatting {ws_name}...", end=" ", flush=True)
    
    # Get data
    data = ws.get_all_values()
    
    # Header
    try:
        format_cell_range(ws, '1:1', header_fmt)
        time.sleep(1)
    except Exception as e:
        print(f"header error: {e}")
    
    # Find rows
    group_rows = []
    total_rows = []
    for i, row in enumerate(data):
        if row and row[0]:
            if row[0] in GROUPS_ORDER:
                group_rows.append(i+1)
            elif row[0] == 'Total':
                total_rows.append(i+1)
    
    # Apply group headers with delay
    for row in group_rows:
        try:
            format_cell_range(ws, f'A{row}:N{row}', group_header_fmt)
            time.sleep(2)
        except Exception as e:
            time.sleep(3)
    
    # Apply totals with delay
    for row in total_rows:
        try:
            format_cell_range(ws, f'A{row}:N{row}', total_fmt)
            time.sleep(2)
        except Exception as e:
            time.sleep(3)
    
    # Column widths
    try:
        set_column_widths(ws, [('A', 150), ('B', 90), ('C', 90), ('D', 90), ('E', 90), 
                              ('F', 90), ('G', 90), ('H', 90), ('I', 90), ('J', 90), 
                              ('K', 90), ('L', 90), ('M', 90), ('N', 100)])
    except:
        pass
    
    print("done!")

apply_format('Transactions Overview 2026')
time.sleep(5)
apply_format('Budget 2026')
time.sleep(5)
apply_format('Budget vs Actual 2026')

print("\n✅ Formatting complete!")
