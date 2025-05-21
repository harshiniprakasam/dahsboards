import os
import gspread
import pandas as pd
import mysql.connector
from mysql.connector import Error
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import re
from datetime import datetime

# Google Sheets Authentication
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CLIENT_SECRET_FILE = r"C:\Users\harsh\Automation\EQUITAS\client_secret.json"  # Adjust path if needed

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

# MySQL Database Connection
DB_SERVER = "localhost"
DB_NAME = "TEST"
DB_USER = "root"
DB_PASSWORD = "#Harshu@123"

try:
    conn = mysql.connector.connect(
        host=DB_SERVER,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    print("Connected to MySQL Database successfully.")
except Error as e:
    print("Error connecting to MySQL:", e)
    exit()

# Column Mapping for IOB
column_mapping = {
    "Month number": "month_number",
    "Data Month": "data_month",
    "Customer Name": "customer_name",
    "Solution": "solution",
    "Total No of Customers": "total_no_of_customers",
    "Total No of Accounts": "total_no_of_accounts",
    "Total No of Transaction(Per month)": "total_no_of_transaction_per_month",
    "Channel": "channel",
    "Scenario No": "scenario_no",
    "Scenario Name": "scenario_name",
    "Fraud Typology - EFM": "fraud_typology_efm",
    "Last Modified Scenario Date": "last_modified_scenario_date",
    "Type of SCN": "type_of_scn",
    "Total Alert for": "total_alert_for",
    "Total no of Alert Closed": "total_no_of_alert_closed",
    "Total no of Open Alerts": "total_no_of_open_alerts",
    "Total No of Frauds detected": "total_no_of_frauds_detected",
    "No of False positive/SCN": "no_of_false_positive_scn",
    "Total sum of Saved Amount": "total_sum_of_saved_amount"
}

# Function to get the latest sheet
def get_latest_sheet():
    all_spreadsheets = gc.list_spreadsheet_files()
    month_mapping = {month: index for index, month in enumerate(
        ["Jan", "Feb", "March", "April", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}
    
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

if latest_sheet_name:
    df = fetch_and_process_sheet(latest_sheet_name, "IOB")

    if df is None or df.empty:
        print("No data hence Skipping update.")
    else:
        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql_query = f"INSERT INTO IOB ({', '.join(columns)}) VALUES ({placeholders})"

        data_tuples = df.apply(tuple, axis=1).tolist()

        try:
            cursor.executemany(sql_query, data_tuples)
            conn.commit()
            print("✅ Data inserted into IOB table successfully!")
        except Error as e:
            conn.rollback()
            print(f"❌ Error inserting data: {e}")

cursor.close()
conn.close()
