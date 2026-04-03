#!/usr/bin/env python3
"""
Update sheets with formulas for auto-calculation.
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

SPREADSHEET_KEY = "1UoIaxcnFor39N_KcIwnq8JWaauPzrDWEv_S5PFcca1M"
CREDENTIALS_FILE = "transactions-project-483516-f68f182fae44.json"

GROUPS_ORDER = ['Income', 'Necessities', 'Discretionary', 'Fixed Expenses', 'Taxes', 
                'Savings & Investments', 'Work', 'Transfer', 'Other']
MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 
          'July', 'August', 'September', 'October', 'November', 'December']

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
    else:
        group_cats['Other'].append(cat)
for g in group_cats:
    group_cats[g].sort()

print(f"Building with {len(cat_to_group)} categories")

# Get existing Budget values
ws_budget = sheet.worksheet('Budget 2026')
budget_data = ws_budget.get_all_values()
user_values = {}
for row in budget_data[1:]:
    if row and row[0] and row[0].strip() not in GROUPS_ORDER + ['', 'Total']:
        cat = row[0].strip()
        user_values[cat] = row[1:14]

print(f"Preserving {len(user_values)} user budget values")

# Build Transactions Overview with formulas
print("\nBuilding Transactions Overview 2026...")

ws = sheet.worksheet('Transactions Overview 2026')

# Write header
header_row = ['Category'] + MONTHS + ['Total']
ws.update('1:1', [header_row])

current_row = 2

for group_name in GROUPS_ORDER:
    categories = group_cats.get(group_name, [])
    if not categories:
        continue
    
    # Spacing row
    ws.update(f'A{current_row}', [['']])
    current_row += 1
    
    # Group header
    ws.update(f'A{current_row}', [[group_name]])
    current_row += 1
    
    # Category rows with formulas
    for cat in categories:
        row_data = [cat]
        for month_idx in range(12):
            # SUMIFS formula
            formula = f'=SUMIFS(\'All Transactions\'!$C:$C,\'All Transactions\'!$E:$E,"{cat}",\'All Transactions\'!$A:$A,">="&DATE(2026,{month_idx+1},1),\'All Transactions\'!$A:$A,"<"&DATE(2026,{month_idx+2},1))'
            row_data.append(formula)
        
        # Total formula
        row_data.append(f'=SUM(B{current_row}:M{current_row})')
        
        ws.update(f'A{current_row}:N{current_row}', [row_data])
        current_row += 1
    
    # Group total row
    total_row = ['Total']
    for col in range(2, 14):
        col_letter = chr(64 + col)
        start = current_row - len(categories)
        formula = f'=SUM({col_letter}{start}:{col_letter}{current_row-1})'
        total_row.append(formula)
    ws.update(f'A{current_row}:N{current_row}', [total_row])
    current_row += 1

print(f"  Transactions Overview built")

# Build Budget with user values + formula totals
print("\nBuilding Budget 2026...")

ws = sheet.worksheet('Budget 2026')

# Write header
ws.update('1:1', [header_row])
current_row = 2

for group_name in GROUPS_ORDER:
    categories = group_cats.get(group_name, [])
    if not categories:
        continue
    
    ws.update(f'A{current_row}', [['']])
    current_row += 1
    
    ws.update(f'A{current_row}', [[group_name]])
    current_row += 1
    
    for cat in categories:
        values = user_values.get(cat, [''] * 13)
        row_data = [cat] + values
        ws.update(f'A{current_row}:N{current_row}', [row_data])
        current_row += 1
    
    # Group total with SUM formula
    total_row = ['Total']
    for col in range(2, 14):
        col_letter = chr(64 + col)
        start = current_row - len(categories)
        formula = f'=SUM({col_letter}{start}:{col_letter}{current_row-1})'
        total_row.append(formula)
    ws.update(f'A{current_row}:N{current_row}', [total_row])
    current_row += 1

print(f"  Budget built")

# Build Budget vs Actual with formulas
print("\nBuilding Budget vs Actual 2026...")

# Get row positions
ws_trans = sheet.worksheet('Transactions Overview 2026')
trans_data = ws_trans.get_all_values()
trans_positions = {}
for i, row in enumerate(trans_data):
    if row and row[0] and row[0].strip() not in GROUPS_ORDER + ['', 'Total']:
        trans_positions[row[0].strip()] = i + 1

ws_budget = sheet.worksheet('Budget 2026')
budget_data = ws_budget.get_all_values()
budget_positions = {}
for i, row in enumerate(budget_data):
    if row and row[0] and row[0].strip() not in GROUPS_ORDER + ['', 'Total']:
        budget_positions[row[0].strip()] = i + 1

ws = sheet.worksheet('Budget vs Actual 2026')

ws.update('1:1', [header_row])
current_row = 2

for group_name in GROUPS_ORDER:
    categories = group_cats.get(group_name, [])
    if not categories:
        continue
    
    ws.update(f'A{current_row}', [['']])
    current_row += 1
    
    ws.update(f'A{current_row}', [[group_name]])
    current_row += 1
    
    for cat in categories:
        t_row = trans_positions.get(cat, current_row)
        b_row = budget_positions.get(cat, current_row)
        
        row_data = [cat]
        for col in range(2, 14):
            col_letter = chr(64 + col)
            formula = f"='Budget 2026'!{col_letter}{b_row}-'Transactions Overview 2026'!{col_letter}{t_row}"
            row_data.append(formula)
        
        # Total
        formula = f"='Budget 2026'!N{b_row}-'Transactions Overview 2026'!N{t_row}"
        row_data.append(formula)
        
        ws.update(f'A{current_row}:N{current_row}', [row_data])
        current_row += 1
    
    # Group total
    total_row = ['Total']
    for col in range(2, 14):
        col_letter = chr(64 + col)
        start = current_row - len(categories)
        formula = f'=SUM({col_letter}{start}:{col_letter}{current_row-1})'
        total_row.append(formula)
    ws.update(f'A{current_row}:N{current_row}', [total_row])
    current_row += 1

print(f"  Budget vs Actual built")

print("\n✅ Done!")
