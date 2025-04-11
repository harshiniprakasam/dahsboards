import os
import gspread
import pandas as pd
import pyodbc
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import re


# Google Sheets Authentication
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CLIENT_SECRET_FILE = r"C:\Users\harsh\Automation\Equitas\client_secret.json"
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
    "Bank": "Bank",
    "Support guys": "Support_guys",
    "Customers Count": "Customers_Count",
    "Accounts Count": "Accounts_Count",
    "Transaction Count": "Transaction_Count",
    "Total Alerts per month": "Total_Alerts_per_month",
    "Total No of Fraud detected": "Total_No_of_Fraud_detected",
    "Total No of False Positive": "Total_No_of_False_Positive",
    "Total No of Open Alerts": "Total_No_of_Open_Alerts",
    "Total No of Closed alerts": "Total_No_of_Closed_alerts",
    "Saved Amount": "Saved_Amount",
    "Lost Amount": "Lost_Amount",
    "Tran Amount": "Tran_Amount",
    "Credit Amount": "Credit_Amount",
    "Debit Amount": "Debit_Amount",
    "Sector": "Sector",
    "Scenario count": "Scenario_count",
    "Alias":"ALIAS"
}

def get_latest_sheet():
    all_spreadsheets = gc.list_spreadsheet_files()
    month_mapping = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }
    valid_sheets = []
    pattern = re.compile(r"Clari5_Wisdom_data-([A-Za-z]+)-25")

    for sheet in all_spreadsheets:
        match = pattern.search(sheet['name'])
        if match:
            month_str = match.group(1)
            if month_str in month_mapping:
                valid_sheets.append((sheet['name'], month_mapping[month_str]))

    if not valid_sheets:
        print("\033[1mNo valid Clari5_Wisdom_data sheets found.\033[0m")
        return None

    latest_sheet = max(valid_sheets, key=lambda x: x[1])
    print(f"\033[1mLatest sheet identified: {latest_sheet[0]}\033[0m")
    return latest_sheet[0]

def fetch_google_sheet(sheet_name, worksheet_name):
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
        print(f"\033[1m\u274c No data found in '{worksheet_name}' of '{sheet_name}'. Database update skipped.\033[0m")
        return None

    df = pd.DataFrame(data, columns=headers)
    df = df[[col for col in column_mapping if col in df.columns]].rename(columns=column_mapping)
    return df

# Fetch the latest available Clari5_Wisdom_data sheet
latest_sheet_name = get_latest_sheet()
worksheet_name = "Consolidate sheet"

if latest_sheet_name:
    df = fetch_google_sheet(latest_sheet_name, worksheet_name)

    if df is not None:
        df.fillna('', inplace=True)

        try:
            cursor.execute("DELETE FROM CONSOLIDATE_DATA")
            conn.commit()
            print("\u2705 Existing data in 'CONSOLIDATE_DATA' table cleared.")
        except pyodbc.Error as e:
            conn.rollback()
            print("\u274c Error clearing table:", e)

        columns = list(df.columns)
        placeholders = ', '.join(['?' for _ in columns])
        sql_query = f"INSERT INTO CONSOLIDATE_DATA ({', '.join(columns)}) VALUES ({placeholders})"

        data_tuples = df.apply(tuple, axis=1).tolist()
        print("Inserting new data into SQL Server...")

        try:
            cursor.executemany(sql_query, data_tuples)
            conn.commit()
            print("\u2705 New data inserted successfully into 'CONSOLIDATE_DATA'.")
        except pyodbc.Error as e:
            conn.rollback()
            print("\u274c Error inserting data:", e)

cursor.close()
conn.close()
