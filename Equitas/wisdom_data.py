import os
import gspread
import pandas as pd
import pyodbc
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import re
from datetime import datetime

# Google Sheets Authentication
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CLIENT_SECRET_FILE = r"C:\Users\Harshini P\Automation\dahsboards\Equitas\client_secret.json"
TOKEN_FILE = 'token.json'

creds = None
if os.path.exists(TOKEN_FILE):
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
    with open(TOKEN_FILE, 'w') as token_file:
        token_file.write(creds.to_json())

# Authenticate Google Sheets API
gc = gspread.authorize(creds)

# SQL Server Connection
DB_SERVER = "192.168.5.236"
DB_USER = "cxpsadm"
DB_PASSWORD = "c_xps123"
DB_NAME = "cxpsadm"

conn_str = f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}"
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server successfully.")
except pyodbc.DatabaseError as e:
    print("Error connecting to SQL Server:", e)
    exit()

# Column Mapping
column_mapping = {
    "Customer-Name": "CUSTOMER_NAME",
    "Solution": "SOLUTION",
    "Total No of Customers": "TOTAL_CUSTOMERS",
    "Total No of Accounts": "TOTAL_ACCOUNTS",
    "Total No of Transaction(Per month)": "TOTAL_TRANSACTIONS",
    "Channel": "CHANNEL",
    "Scenario Name": "SCENARIO_NAME",
    "Fraud Typology - EFM": "MONEY_LAUNDERING_TYPE",
    "Last Modified Scenario Date": "SCENARIO_LAST_MODIFIED_DATE",
    "Type of SCN": "TYPE_OF_SCN",
    "Total Alert for": "TOTAL_ALERT",
    "Total no of Alert Closed": "TOTAL_ALERT_CLOSED",
    "Total no of Open Alerts": "TOTAL_OPEN_ALERTS",
    "Total No of Frauds detected": "TOTAL_FRAUDS_DETECTED",
    "No of False positive/SCN": "FALSE_POSITIVE",
    "Total sum of Saved Amount": "TOTAL_AMOUNT_SAVED",
    "Data Month": "DATA_MONTH"
}

# Function to get the latest sheet
def get_latest_sheet():
    """Fetch all sheet names and determine the latest month."""
    all_spreadsheets = gc.list_spreadsheet_files()
    month_mapping = {month: index for index, month in enumerate(
        ["Jan", "Feb", "March", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}
    
    valid_sheets = []
    pattern = re.compile(r"Clari5_Wisdom_data-([A-Za-z]+)-25")

    for sheet in all_spreadsheets:
        match = pattern.search(sheet['name'])
        if match:
            month_str = match.group(1)
            if month_str in month_mapping:
                valid_sheets.append((sheet['name'], month_mapping[month_str]))

    if not valid_sheets:
        print("\033[1mNo valid sheets found.\033[0m")
        return None

    latest_sheet = max(valid_sheets, key=lambda x: x[1])
    print(f"\033[1mLatest sheet identified: {latest_sheet[0]}\033[0m")
    return latest_sheet[0]

# Fetch and process the latest sheet
latest_sheet_name = get_latest_sheet()

def fetch_and_process_sheet(sheet_name, worksheet_name):
    """Fetch data from Google Sheets and process it into a DataFrame."""
    try:
        spreadsheet = gc.open(sheet_name)
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: The spreadsheet '{sheet_name}' was not found.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: The sheet '{worksheet_name}' was not found in '{sheet_name}'.")
        return None
    
    headers = [col.strip() for col in worksheet.row_values(1)]
    data = worksheet.get_all_values()[1:]

    if not data:
        print(f"\033[1m❌ No data found in the sheet '{worksheet_name}' of '{sheet_name}'. Database update skipped.\033[0m")
        return None

    df = pd.DataFrame(data, columns=headers)
    df = df[[col for col in column_mapping if col in df.columns]].rename(columns=column_mapping)
    return df

# Insert data to SQL Server
if latest_sheet_name:
    df = fetch_and_process_sheet(latest_sheet_name, "EQUITAS")

    if df is None or df.empty:
        print("No data hence Skipping update.")
    else:
        columns = list(df.columns)
        placeholders = ", ".join(["?" for _ in columns])
        sql_query = f"INSERT INTO EQUITAS ({', '.join(columns)}) VALUES ({placeholders})"

        data_tuples = df.apply(tuple, axis=1).tolist()

        try:
            cursor.executemany(sql_query, data_tuples)
            conn.commit()
            print("✅ Data inserted into SQL Server successfully!")
        except pyodbc.Error as e:
            conn.rollback()
            print(f"❌ Error inserting data: {e}")

cursor.close()
conn.close()
