import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import matplotlib.ticker as mtick
from matplotlib.backends.backend_pdf import PdfPages
import base64
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from google_auth_oauthlib.flow import InstalledAppFlow
import pickle

# Database Connection Details
DB_SERVER = "localhost"
DB_DATABASE = "TEST"
DB_USERNAME = "root"
DB_PASSWORD = "#Harshu@123"

# Encode password to handle special characters
encoded_password = quote_plus(DB_PASSWORD)

# Create MySQL Engine with connection pool settings
try:
    engine = create_engine(f"mysql+mysqlconnector://{DB_USERNAME}:{encoded_password}@{DB_SERVER}/{DB_DATABASE}",
                           pool_recycle=3600, pool_size=10, max_overflow=5)
except Exception as e:
    print("Error creating database engine:", e)
    engine = None

# SQL Queries
query_main = """
SELECT
    e.DATA_MONTH AS Month,
    SUM(CAST(e.TOTAL_ALERT AS SIGNED)) AS Transaction_Count,
    SUM(CAST(e.TOTAL_ALERT_CLOSED AS SIGNED)) AS Alerts_Closed,
    SUM(CAST(e.TOTAL_OPEN_ALERTS AS SIGNED)) AS Alerts_Generated
FROM EQUITAS e
JOIN (
    SELECT DISTINCT DATA_MONTH
    FROM EQUITAS
    ORDER BY STR_TO_DATE(DATA_MONTH, '%b-%Y') DESC
    LIMIT 2
) subquery ON e.DATA_MONTH = subquery.DATA_MONTH
GROUP BY e.DATA_MONTH
ORDER BY STR_TO_DATE(e.DATA_MONTH, '%b-%Y') DESC;
"""

query_totals = """
SELECT 
    SUM(CAST(TOTAL_CUSTOMERS AS SIGNED)) AS Total_Customers, 
    SUM(CAST(TOTAL_ACCOUNTS AS SIGNED)) AS Total_Accounts
FROM EQUITAS
WHERE DATA_MONTH = (SELECT DISTINCT DATA_MONTH FROM EQUITAS ORDER BY STR_TO_DATE(DATA_MONTH, '%b-%Y') DESC LIMIT 1);
"""

query_top_scenarios = """
SELECT SCENARIO_NAME, SUM(CAST(TOTAL_ALERT AS SIGNED)) AS Alert_Count
FROM EQUITAS
WHERE DATA_MONTH = (SELECT DISTINCT DATA_MONTH FROM EQUITAS ORDER BY STR_TO_DATE(DATA_MONTH, '%b-%Y') DESC LIMIT 1)
GROUP BY SCENARIO_NAME
ORDER BY Alert_Count DESC
LIMIT 4;
"""

query_low_scenarios = """
SELECT SCENARIO_NAME, SUM(CAST(TOTAL_ALERT AS SIGNED)) AS Alert_Count
FROM EQUITAS
WHERE DATA_MONTH = (SELECT DISTINCT DATA_MONTH FROM EQUITAS ORDER BY STR_TO_DATE(DATA_MONTH, '%b-%Y') DESC LIMIT 1)
GROUP BY SCENARIO_NAME
HAVING Alert_Count < 5
ORDER BY Alert_Count ASC
LIMIT 5;
"""

query_scatter = """
SELECT 
    CAST(REPLACE(Accounts_Count, ',', '') AS SIGNED) AS Total_Accounts, 
    CAST(REPLACE(total_Alerts_per_month, ',', '') AS SIGNED) AS Total_Alerts
FROM CONSOLIDATE_DATA
WHERE 
    Accounts_Count REGEXP '^[0-9,]+$' 
    AND total_Alerts_per_month REGEXP '^[0-9,]+$';
"""

if engine:
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query_main), conn)
            totals_df = pd.read_sql(text(query_totals), conn)
            df_top_scenarios = pd.read_sql(text(query_top_scenarios), conn)
            df_low_scenarios = pd.read_sql(text(query_low_scenarios), conn)
            df_scatter = pd.read_sql(text(query_scatter), conn)

        df["Month"] = pd.to_datetime(df["Month"])
        df = df.sort_values(by="Month")
        df["Month"] = df["Month"].dt.strftime('%b %Y')

        df["Transaction_Count"] /= 1_000_000
        df["Alerts_Generated"] /= 1_000_000
        df["Alerts_Closed"] /= 1_000_000

        total_customers = totals_df.loc[0, "Total_Customers"] / 1_000_000
        total_accounts = totals_df.loc[0, "Total_Accounts"] / 1_000_000

        sns.set_theme(style="whitegrid")
        fig1, axes = plt.subplots(4, 2, figsize=(18, 18), gridspec_kw={'height_ratios': [1, 3, 3, 3]})
        fig1.patch.set_facecolor('#f0f4f7')
        fig1.suptitle("Enterprise Fraud Management Dashboard", fontsize=18, fontweight="bold", color="darkblue")
        plt.subplots_adjust(hspace=0.8, wspace=1.2)

        summary_data = [("Total Customers", f"{total_customers:.0f}M"), ("Total Accounts", f"{total_accounts:.0f}M")]
        for idx, (title, value) in enumerate(summary_data):
            ax = axes[0, idx]
            ax.set_xticks([])
            ax.set_yticks([])
            ax.set_facecolor("#f0f4f7")
            ax.text(0.5, 0.9, title, fontsize=14, ha="center", fontweight="bold")
            ax.text(0.5, 0., value, fontsize=28, ha="center", fontweight="bold", color="darkblue")
            for spine in ax.spines.values():
                spine.set_visible(False)

        plot_configs = [
            ("Transaction_Count", "Transaction Count", axes[1, 0]),
            ("Alerts_Closed", "Alerts Closed", axes[2, 0]),
            ("Alerts_Generated", "Alerts Generated", axes[3, 0])
        ]

        colors = ["#004080", "#0073e6", "#3399ff"]
        for idx, (column, title, ax) in enumerate(plot_configs):
            sns.barplot(x="Month", y=column, data=df, ax=ax, color=colors[idx])
            ax.set_title(title, fontsize=14, fontweight="bold", color="black")
            ax.set_xlabel("Month")
            ax.set_ylabel("Count (Millions)")

        # Get the latest month dynamically from the data
        latest_month = df["Month"].iloc[-1] if not df.empty else "Unknown"

        sns.barplot(y="SCENARIO_NAME", x="Alert_Count", data=df_top_scenarios, ax=axes[2, 1], palette="Blues_r")
        axes[2, 1].set_title(f"Top 4 High-Performing Scenarios ({latest_month})")

        if not df_low_scenarios.empty:
         sns.barplot(y="SCENARIO_NAME", x="Alert_Count", data=df_low_scenarios, ax=axes[1, 1], palette="Reds_r")
         axes[1, 1].set_title(f"Low Performing Scenarios ({latest_month})")


        fig1.delaxes(axes[3, 1])

        if not df_scatter.empty:
            fig2, ax2 = plt.subplots(figsize=(12, 6))
            scatter = ax2.scatter(df_scatter["Total_Accounts"], df_scatter["Total_Alerts"], c=df_scatter["Total_Alerts"], cmap="coolwarm", s=df_scatter["Total_Alerts"] / 300 + 20, edgecolor="black", alpha=0.75)
            plt.colorbar(scatter, label="Total Alerts per Month")
            plt.title("Total Alerts vs Total Accounts")
            plt.xlabel("Total Accounts")
            plt.ylabel("Total Alerts per Month")
            ax2.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x / 1e6:.1f}M'))
            plt.grid(True, linestyle="--", alpha=0.5)

        # Save Figures to PDF
        pdf_filename = "Fraud_Management_Dashboard.pdf"
        with PdfPages(pdf_filename) as pdf:
            pdf.savefig(fig1, bbox_inches="tight")  # Save first figure
            if not df_scatter.empty:
                pdf.savefig(fig2, bbox_inches="tight")  # Save scatter plot figure
            print(f"PDF saved successfully as {pdf_filename}")

        plt.show()

    # === Gmail API: Sending Email ===
        TOKEN_PATH = r"C:\Users\harsh\Automation\Equitas\Reports\cred.json"

        def authenticate_gmail():
            SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
            creds = None
            if os.path.exists("cred.json"):
                with open("cred.json", "rb") as token:
                    creds = pickle.load(token)

            if not creds or not creds.valid:
                CLIENT_SECRET_FILE = r"C:\Users\harsh\Automation\Equitas\Reports\client_secret.json"
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)

                creds = flow.run_local_server(port=0)
                with open("cred.json", "wb") as token:
                    pickle.dump(creds, token)
            return creds

        def send_email():
            recipient ="harshiniprakasam@gmail.com"
            subject = "Monthly Fraud Management Report"
            body = "Dear Team,\n\nThis is an auto-generated email with a PDF attachment containing the report generated using test data.\n\nBest regards,\nHarshini"

            creds = authenticate_gmail()
            service = build("gmail", "v1", credentials=creds)

            message = MIMEMultipart()
            message["to"] = recipient
            message["subject"] = subject
            message.attach(MIMEText(body, "plain"))

            with open(pdf_filename, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={pdf_filename}")
                message.attach(part)

            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            try:
                service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
                print(f"Email sent successfully to {recipient} with {pdf_filename}")
            except HttpError as error:
                print(f"An error occurred: {error}")

        send_email()    

    except Exception as e:
        print("⚠️ Error during execution:", e)
    finally:
        if 'engine' in locals():
            engine.dispose()

