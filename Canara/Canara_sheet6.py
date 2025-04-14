import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pyodbc
import matplotlib.gridspec as gridspec
import warnings
import os
from dotenv import load_dotenv

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Load environment variables from .env file
load_dotenv()

# Connect to the database
conn_str = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}"
)

query = """
SELECT 
    Data_Month,
    Total_Amount_Saved,
    Total_Amount_Lost,
    Total_No_of_Frauds_detected
FROM Canara
WHERE Data_Month IN ('Sep-24','Oct-24','Nov-24','Dec-24','Jan-25','Feb-25')
"""

try:
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)

    # Clean and convert data
    df.dropna(inplace=True)
    df["Total_Amount_Saved"] = df["Total_Amount_Saved"].str.replace(",", "").str.replace("M", "").astype(float)
    df["Total_Amount_Lost"] = df["Total_Amount_Lost"].str.replace(",", "").str.replace("M", "").astype(float)
    df["Total_No_of_Frauds_detected"] = df["Total_No_of_Frauds_detected"].str.replace(",", "").astype(int)

    # Set month order
    month_order = ["Sep-24", "Oct-24", "Nov-24", "Dec-24", "Jan-25", "Feb-25"]
    df["Data_Month"] = pd.Categorical(df["Data_Month"], categories=month_order, ordered=True)
    df = df.sort_values("Data_Month")

    # Group and aggregate
    df_grouped = df.groupby("Data_Month", sort=False, observed=False).agg({
        "Total_Amount_Saved": "sum",
        "Total_Amount_Lost": "sum",
        "Total_No_of_Frauds_detected": "sum"
    }).reset_index()

    # Plotting with gridspec
    fig = plt.figure(figsize=(14, 8))
    fig.patch.set_facecolor('white')
    gs = gridspec.GridSpec(2, 2, height_ratios=[1, 1])
    plt.subplots_adjust(hspace=0.5, wspace=0.3)

    title_font = {'fontsize': 13, 'fontweight': 'bold'}

    # Saved Amount (Top Left)
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.invert_xaxis()
    ax1.plot(df_grouped["Data_Month"], df_grouped["Total_Amount_Saved"], marker='o', color='royalblue')
    ax1.set_title("Monthly Saved Amount Trend (Last Six Months)", **title_font)
    ax1.set_ylabel("Saved Amount")
    ax1.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{x/1e6:.0f}M"))
    ax1.grid(True, linestyle='--', alpha=0.5)
    for i, val in enumerate(df_grouped["Total_Amount_Saved"]):
        ax1.text(i, val, f"{val/1e6:.1f}M", ha='center', va='bottom', fontsize=9)

    # Lost Amount (Top Right)
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.invert_xaxis()
    ax2.plot(df_grouped["Data_Month"], df_grouped["Total_Amount_Lost"], marker='o', color='dodgerblue')
    ax2.set_title("Monthly Lost Amount Trend (Last Six Months)", **title_font)
    ax2.set_ylabel("Available Loss Amt")
    ax2.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{x/1e6:.0f}M"))
    ax2.grid(True, linestyle='--', alpha=0.5)
    for i, val in enumerate(df_grouped["Total_Amount_Lost"]):
        ax2.text(i, val, f"{val/1e6:.1f}M", ha='center', va='bottom', fontsize=9)

    # Fraud Detected (Bottom Full Width)
    ax3 = fig.add_subplot(gs[1, :])
    ax3.invert_xaxis()
    ax3.plot(df_grouped["Data_Month"], df_grouped["Total_No_of_Frauds_detected"], marker='o', color='deepskyblue')
    ax3.set_title("Monthly Fraud Detected Trend (Last Six Months)", **title_font)
    ax3.set_ylabel("Fraud Detected")
    ax3.grid(True, linestyle='--', alpha=0.5)
    for i, val in enumerate(df_grouped["Total_No_of_Frauds_detected"]):
        ax3.text(i, val, str(val), ha='center', va='bottom', fontsize=9)

    # Save the figure as a PNG file in the "pngs" folder
    output_dir = "pngs"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "Canara_sheet6.png")
    plt.savefig(output_path)
    print(f"Dashboard saved as: {output_path}")

except Exception as e:
    print("Error:", e)

finally:
    if 'conn' in locals():
        conn.close()
