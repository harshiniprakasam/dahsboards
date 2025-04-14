import os
import gspread
import pandas as pd
import pyodbc
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import re

# Load environment variables from .env file
load_dotenv()

# Google Sheets Authentication
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CLIENT_SECRET_FILE = os.getenv("CLIENT_SECRET_FILE")
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
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

conn_str = f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}"
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server successfully.")
except pyodbc.Error as e:
    print("Error connecting to SQL Server:", e)
    exit()

# Fetch SQL Table Column Names & Data Types
column_data_types = {}
cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'CANARA'")
for col_name, data_type in cursor.fetchall():
    column_data_types[col_name] = data_type.upper()

# Column Mapping
column_mapping = {
    "Data Month": "Data_Month",
    "Month number": "Month_number",
    "Customer Name": "Customer_Name",
    "Solution": "Solution",
    "Total No of Customers": "Total_No_of_Customers",
    "Total No of Accounts": "Total_No_of_Accounts",
    "Total No of Transaction": "Total_No_of_Transaction",
    "Channel": "Channel",
    "Alert closure": "Alert_closure",
    "Scenario Name": "Scenario_Name",
    "Category": "Category",
    "Weightage": "Weightage_Score_of_SCN",
    "Type of SCN": "Type_of_SCN",
    "Scenario Last Modified date": "Scenario_Last_Modified_date",
    "TotalAlert": "TotalAlert",
    "Resolved": "Resolved",
    "Total no of Alert Closed": "Total_no_of_Alert_Closed",
    "Total no of Open Alerts": "Total_no_of_Open_Alerts",
    "Total no of Reopen Alerts": "Total_no_of_Reopen_Alerts",
    "Total No of Suspicious": "Total_no_of_Suspicious",
    "Total No of Frauds detected": "Total_No_of_Frauds_detected",
    "No of False positive": "No_of_False_positive_SCN",
    "Total Amount Saved": "Total_Amount_Saved",
    "Total Amount Lost": "Total_Amount_Lost",
    "Bank Size": "Bank_Size"
}

# Function to get latest Clari5 sheet
def get_latest_sheet():
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

# Fetch and process data from the latest sheet
def fetch_and_process_sheet(sheet_name, worksheet_name):
    try:
        spreadsheet = gc.open(sheet_name)
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Spreadsheet '{sheet_name}' not found.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{worksheet_name}' not found in '{sheet_name}'.")
        return None

    headers = [col.strip().replace("*", "").replace("/", "_") for col in worksheet.row_values(1)]
    data = worksheet.get_all_values()[1:]
    df = pd.DataFrame(data, columns=headers)
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    return df

# Convert data to match SQL column types
def convert_data(value, dtype):
    if pd.isna(value) or (isinstance(value, str) and value.strip() == ''):
        return None
    if dtype in ['INT', 'BIGINT']:
        try:
            return int(str(value).replace(',', '').strip())
        except ValueError:
            return None
    if dtype in ['DECIMAL', 'FLOAT', 'NUMERIC']:
        try:
            return float(str(value).replace(',', '').strip())
        except ValueError:
            return None
    if dtype in ['DATETIME', 'DATE']:
        try:
            return pd.to_datetime(value, errors='coerce')
        except Exception:
            return None
    if dtype in ['VARCHAR', 'TEXT', 'NVARCHAR']:
        return str(value).strip()
    return value

# Main logic
latest_sheet = get_latest_sheet()
if latest_sheet:
    df = fetch_and_process_sheet(latest_sheet, "CANARA")
    if df is None:
        print(f"⚠️ Could not load data from {latest_sheet}. Skipping update.")
    elif df.empty:
        print(f"⚠️ Skipping update — sheet '{latest_sheet}' is empty.")
    else:
        df.fillna('', inplace=True)
        df = df.sort_values(by=['Data_Month'])

        for col in df.columns:
            df[col] = df[col].map(lambda x: convert_data(x, column_data_types.get(col, 'VARCHAR')))
        df = df[column_data_types.keys()]
        for col in df.columns:
            if column_data_types.get(col, '') in ['INT', 'BIGINT', 'DECIMAL', 'FLOAT', 'NUMERIC']:
                df[col] = df[col].fillna(0)

        columns = list(df.columns)
        placeholders = ', '.join(['?' for _ in columns])
        sql_query = f"INSERT INTO CANARA ({', '.join(columns)}) VALUES ({placeholders})"
        data_tuples = df.apply(tuple, axis=1).tolist()
        print("Inserting data from:", latest_sheet)

        try:
            cursor.executemany(sql_query, data_tuples)
            conn.commit()
            print("✅ Data inserted into SQL Server successfully!")
        except pyodbc.Error as e:
            conn.rollback()
            print(f"❌ Error inserting data from {latest_sheet}: {e}")

cursor.close()
conn.close()
