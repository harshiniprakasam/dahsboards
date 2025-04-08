import os
import gspread
import pandas as pd
import mysql.connector
from mysql.connector import Error
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

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

# Fetch SQL Table Column Names & Data Types
column_data_types = {}
cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'CANARA'")
for col_name, data_type in cursor.fetchall():
    column_data_types[col_name] = data_type.upper()

print("Column Data Types Fetched:", column_data_types)

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

    headers = [col.strip().replace("*", "").replace("/", "_") for col in worksheet.row_values(1)]
    data = worksheet.get_all_values()[1:]
    df = pd.DataFrame(data, columns=headers)
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    return df

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
    if dtype in ['VARCHAR', 'TEXT']:
        return str(value).strip()
    return value

sheets_to_process = ["Clari5_Wisdom_data-Jan-25", "Clari5_Wisdom_data-Feb-25"]

for sheet in sheets_to_process:
    df = fetch_and_process_sheet(sheet, "CANARA")
    if df is not None:
        df.fillna('', inplace=True)
        df = df.sort_values(by=['Data_Month'])
        for col in df.columns:
            df[col] = df[col].map(lambda x: convert_data(x, column_data_types.get(col, 'VARCHAR')))
        df = df[column_data_types.keys()]
        for col in df.columns:
            if column_data_types.get(col, '') in ['INT', 'BIGINT', 'DECIMAL', 'FLOAT', 'NUMERIC']:
                df[col] = df[col].fillna(0)
        columns = list(df.columns)
        placeholders = ', '.join(['%s'] * len(columns))
        sql_query = f"INSERT INTO CANARA ({', '.join(columns)}) VALUES ({placeholders})"
        data_tuples = df.apply(tuple, axis=1).tolist()
        print("Inserting data from:", sheet)
        try:
            cursor.executemany(sql_query, data_tuples)
            conn.commit()
            print("Data inserted into MySQL successfully for:", sheet)
        except Error as e:
            conn.rollback()
            print(f"Error inserting data from {sheet}: {e}")

cursor.close()
conn.close()
