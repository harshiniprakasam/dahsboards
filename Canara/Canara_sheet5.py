import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pyodbc
import matplotlib.ticker as mtick
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection string
conn_str = (
    f"DRIVER={{SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}"
)

# SQL query
query = """
SELECT 
    Bank,
    ALIAS,
    CAST(REPLACE(Accounts_Count, ',', '') AS BIGINT) AS Total_Accounts,
    CAST(REPLACE(Transaction_count, ',', '') AS BIGINT) AS Transaction_count,
    CAST(REPLACE(Total_Alerts_per_month, ',', '') AS BIGINT) AS Total_Alerts,
    Scenario_count
FROM CONSOLIDATE_DATA
WHERE 
    ISNUMERIC(REPLACE(Accounts_Count, ',', '')) = 1 AND
    ISNUMERIC(REPLACE(Transaction_count, ',', '')) = 1 AND
    ISNUMERIC(REPLACE(Total_Alerts_per_month, ',', '')) = 1 AND
    ISNUMERIC(Scenario_count) = 1;
"""

def place_text_without_overlap(ax, x, y, text, placed_positions, xlim, ylim):
    offset_step = 0.015 * (ylim[1] - ylim[0])
    max_attempts = 10
    attempt = 0
    new_y = y
    while attempt < max_attempts:
        too_close = any(abs(new_y - existing_y) < offset_step for existing_y in placed_positions.get(x, []))
        if not too_close and ylim[0] <= new_y <= ylim[1]:
            break
        new_y += offset_step  # Try shifting up
        attempt += 1
    placed_positions.setdefault(x, []).append(new_y)
    
    ha = 'left' if x < (xlim[0] + xlim[1]) / 2 else 'right'
    offset_x = 0.01 * (xlim[1] - xlim[0]) * (1 if ha == 'left' else -1)

    ax.text(x + offset_x, new_y, text,
            fontsize=9, ha=ha, va='center', color='black', weight='bold')

try:
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)

    if not df.empty:
        df["Scenario_count"] = df["Scenario_count"].astype(int)
        df["Total_Accounts"] = df["Total_Accounts"].astype(int)
        df["Transaction_count"] = df["Transaction_count"].astype(int)

        df = df[
            (df["Scenario_count"] > 0) &
            (df["Total_Accounts"] > 0) &
            (df["Transaction_count"] > 0)
        ]

        first_10_banks = df["Bank"].dropna().unique()[:10]
        df = df[df["Bank"].isin(first_10_banks)]
        df.loc[df["Bank"].str.upper() == "CANARA", "ALIAS"] = "Canara"
        print("Banks considered in the chart:")
        print(", ".join(first_10_banks))


        unique_aliases = df["ALIAS"].unique()
        palette = sns.color_palette("tab10", len(unique_aliases))
        color_map = dict(zip(unique_aliases, palette))
        df["Color"] = df["ALIAS"].map(color_map)

        fig, axes = plt.subplots(1, 2, figsize=(18, 8))
        fig.suptitle("Density Distribution of Alerts Per Million Accounts By Scenarios\nacross Clari5 Clients",
                     fontsize=16, fontweight='bold')

        y_formatter = mtick.FuncFormatter(lambda x, _: f'{x/1e3:.0f}k')

        # --- Plot 1 ---
        ax1 = axes[0]
        ax1.scatter(
            df["Total_Accounts"],
            df["Total_Alerts"],
            s=df["Total_Alerts"] / 20 + 50,
            c=df["Color"],
            edgecolors="black",
            alpha=0.8
        )
        ax1.set_title("Accounts vs Alerts", fontsize=13, fontweight='bold')
        ax1.set_xlabel("Accounts Count", fontsize=11, fontweight='bold')
        ax1.set_ylabel("Total Alerts", fontsize=11, fontweight='bold')
        ax1.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x/1e6:.0f}M'))
        ax1.yaxis.set_major_formatter(y_formatter)
        ax1.set_facecolor('white')

        xlim1 = ax1.get_xlim()
        ylim1 = ax1.get_ylim()
        placed_y_1 = {}

        for _, row in df.iterrows():
            place_text_without_overlap(ax1, row["Total_Accounts"], row["Total_Alerts"], row["ALIAS"], placed_y_1, xlim1, ylim1)

        # --- Plot 2 ---
        ax2 = axes[1]
        ax2.scatter(
            df["Transaction_count"],
            df["Total_Alerts"],
            s=df["Total_Alerts"] / 20 + 50,
            c=df["Color"],
            edgecolors="black",
            alpha=0.8
        )
        ax2.set_title("Transactions vs Alerts", fontsize=13, fontweight='bold')
        ax2.set_xlabel("Transaction Count", fontsize=11, fontweight='bold')
        ax2.set_ylabel("Total Alerts", fontsize=11, fontweight='bold')
        ax2.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x/1e6:.0f}M'))
        ax2.yaxis.set_major_formatter(y_formatter)
        ax2.set_facecolor('white')

        xlim2 = ax2.get_xlim()
        ylim2 = ax2.get_ylim()
        placed_y_2 = {}

        for _, row in df.iterrows():
            place_text_without_overlap(ax2, row["Transaction_count"], row["Total_Alerts"], row["ALIAS"], placed_y_2, xlim2, ylim2)

        # Legend
        handles = []
        labels = []
        for alias in unique_aliases:
            scenario_count = df[df["ALIAS"] == alias]["Scenario_count"].iloc[0]
            label = f"{alias}-{scenario_count}"
            handles.append(ax1.scatter([], [], c=color_map[alias], label=label, s=100))
            labels.append(label)

        fig.legend(
            handles,
            labels,
            title="Bank-Scenario count",
            title_fontsize=10,
            fontsize=9,
            loc='upper center',
            ncol=len(unique_aliases),
            bbox_to_anchor=(0.5, 0.9),
            frameon=False
        )

        plt.tight_layout(rect=[0, 0, 1, 0.9])

        # Save the figure as a PNG file in the "pngs" folder
        output_dir = "pngs"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "Canara_sheet5.png")
        plt.savefig(output_path)
        print(f"Dashboard saved as: {output_path}")

except Exception as e:
    print("Error:", e)

finally:
    if conn:
        conn.close()
