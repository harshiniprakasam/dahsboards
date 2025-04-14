import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# SQL Server connection string using environment variables
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

conn_str = f"DRIVER={{SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}"

def fetch_data(query):
    """Fetch data from SQL Server database and preprocess."""
    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            print(f"Warning: No data returned for query: {query}")
            return None

        # Convert second column to numeric, replace NaNs with 0, and filter out zero rows
        df[df.columns[1]] = pd.to_numeric(df[df.columns[1]], errors='coerce').fillna(0)
        df = df[df[df.columns[1]] > 0]

        if df.empty:
            print("Warning: All values are zero. Skipping chart.")
            return None

        print(f"Data fetched for chart:\n{df}")
        return df

    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        return None

def create_donut_chart(ax, values, labels, title):
    """Create a donut chart with safe handling for edge cases."""
    if values.isnull().all() or values.sum() == 0:
        ax.text(0, 0, "No Data", ha='center', va='center', fontsize=12, fontweight='bold')
        ax.set_xticks([]); ax.set_yticks([])
        return

    try:
        colors = sns.color_palette("husl", len(values)).as_hex()
        wedges, _, _ = ax.pie(
            values,
            labels=None,
            autopct=lambda p: f'{p:.1f}%' if p > 2 else '',
            colors=colors,
            startangle=140,
            wedgeprops={'edgecolor': 'white'},
            pctdistance=0.85
        )

        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        ax.add_patch(centre_circle)
        ax.set_title(title, fontsize=14, fontweight='bold')

        total_value = values.sum()
        formatted_total = f"{total_value / 1_000_000:.0f}M" if total_value >= 1_000_000 else f"{total_value:,.0f}"
        ax.text(0, 0, formatted_total, ha='center', va='center', fontsize=14, fontweight='bold')

        legend_patches = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=c, markersize=10) for c in colors]
        ax.legend(legend_patches, labels, title="Channel", loc="center left", bbox_to_anchor=(1, 0.5), fontsize=10)

    except Exception as e:
        ax.text(0, 0, f"Error: {str(e)}", ha='center', va='center', fontsize=10, color='red')
        ax.set_xticks([]); ax.set_yticks([])
        print(f"Error while creating chart for {title}: {e}")

def generate_dashboard():
    """Fetch data and generate CXO dashboard for Canara Bank (Feb 2025)."""
    queries = {
        "Transaction Volume": """
            SELECT Channel, COALESCE(SUM(TRY_CAST(Total_No_of_Transaction AS FLOAT)), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Alerts Per Million Transaction Volume": """
            SELECT Channel, COALESCE(SUM(TRY_CAST(TotalAlert AS FLOAT)), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Alert Closure": """
            SELECT Channel, COALESCE(SUM(TRY_CAST(Total_no_of_Alert_Closed AS FLOAT)), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Fraud Detected": """
            SELECT Channel, COALESCE(SUM(TRY_CAST(Total_no_of_Frauds_detected AS FLOAT)), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Saved Amount": """
            SELECT Channel, COALESCE(SUM(TRY_CAST(Total_Amount_Saved AS FLOAT)), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Avoidable Lost Amount": """
            SELECT Channel, COALESCE(SUM(TRY_CAST(Total_Amount_Lost AS FLOAT)), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """
    }

    # Create the "pngs" folder if it doesn't exist
    output_dir = "pngs"
    os.makedirs(output_dir, exist_ok=True)

    # Increase the figure size
    fig, axes = plt.subplots(2, 3, figsize=(24, 14))  # Increased figure size
    fig.suptitle('Enterprise Fraud Management - Canara Bank CXO Dashboard (February 2025)', fontsize=18, fontweight='bold')

    for ax, (title, query) in zip(axes.flatten(), queries.items()):
        df = fetch_data(query)
        if df is not None:
            create_donut_chart(ax, df.iloc[:, 1], df.iloc[:, 0], title)
        else:
            ax.text(0.5, 0.5, "No Data", ha='center', va='center', fontsize=12, fontweight='bold')
            ax.set_xticks([]); ax.set_yticks([])

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    # Save the figure as a PNG file in the "pngs" folder
    output_path = os.path.join(output_dir, "Canara_sheet1.png")
    plt.savefig(output_path, dpi=300)  # Save with high resolution
    print(f"Dashboard saved as: {output_path}")

# Run dashboard generation
if __name__ == "__main__":
    generate_dashboard()
