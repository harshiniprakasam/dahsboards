import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import FancyBboxPatch
from matplotlib import patheffects
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

# SQL Server Database Connection
def fetch_data(query):
    try:
        conn = pyodbc.connect(conn_str)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except pyodbc.Error as e:
        print(f"⚠️ Error: {e}")
        return None

# Query
query = """
SELECT Data_Month, 
       Total_No_of_Transaction, 
       TotalAlert, 
       Total_no_of_Alert_Closed,
       Total_No_of_Frauds_detected,
       Channel,
       Alert_closure,
       Type_of_SCN,
       Total_Amount_Lost,
       Total_Amount_Saved
FROM canara
"""
df = fetch_data(query)

# Data Preprocessing
numeric_columns = ["Total_No_of_Transaction", "TotalAlert", 
                   "Total_no_of_Alert_Closed", "Total_No_of_Frauds_detected", 
                   "Total_Amount_Lost", "Total_Amount_Saved"]
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
df["Total_no_of_Alert_Closed_M"] = df["Total_no_of_Alert_Closed"] / 1e6
df["Total_Amount_Lost_M"] = df["Total_Amount_Lost"] / 1e6
df["Loss_Type"] = df["Type_of_SCN"].apply(lambda x: "Monitoring Loss" if x == "Monitoring" else "Avoidable Loss")
df_loss = df.groupby(["Data_Month", "Loss_Type"])["Total_Amount_Lost_M"].sum().unstack().fillna(0).reset_index()
df_summary = df.groupby("Data_Month")[["Total_Amount_Lost", "Total_Amount_Saved"]].sum().reset_index()
df_summary[["Total_Amount_Lost", "Total_Amount_Saved"]] /= 1e6

# Plotting Setup
sns.set_style("white")
fig, axes = plt.subplots(2, 3, figsize=(24, 14))
plt.subplots_adjust(hspace=0.4, wspace=0.3)

# 1. Transaction Volume
df["Total_No_of_Transaction_M"] = df["Total_No_of_Transaction"] / 1e6
sns.barplot(x="Data_Month", y="Total_No_of_Transaction_M", ax=axes[0, 0], palette="Blues_r", data=df)
axes[0, 0].set_title("Transaction Volume (in Millions)", fontsize=14, fontweight="bold", pad=20)
axes[0, 0].set_xlabel("Month", labelpad=10)
axes[0, 0].set_ylabel("Transaction Count (M)")
axes[0, 0].set_xticklabels(axes[0, 0].get_xticklabels(), rotation=0)

# 2. Alerts per Million
df["Alerts_per_Million"] = df["TotalAlert"] / (df["Total_No_of_Transaction"] / 1e6)
sns.barplot(x="Data_Month", y="Alerts_per_Million", ax=axes[0, 1], palette="Reds_r", data=df)
axes[0, 1].set_title("Alerts Generated per Million Transactions", fontsize=14, fontweight="bold", pad=20)
axes[0, 1].set_xlabel("Month", labelpad=10)
axes[0, 1].set_ylabel("Alerts per Million")
axes[0, 1].set_xticklabels(axes[0, 1].get_xticklabels(), rotation=0)

# 3. Alerts Closed by Channel
channel_alerts = df.pivot_table(index="Data_Month", columns="Alert_closure", values="Total_no_of_Alert_Closed_M", aggfunc="sum").fillna(0)
channel_alerts.plot(kind="bar", stacked=True, colormap="viridis", ax=axes[1, 0])
axes[1, 0].set_title("#Alerts Closed by Channels (in Millions)", fontsize=14, fontweight="bold", pad=30, loc='center')
axes[1, 0].legend(title=None, fontsize=10, loc="upper center", bbox_to_anchor=(0.5, 1.07), ncol=3, frameon=False)
axes[1, 0].set_xlabel("Month", labelpad=10)
axes[1, 0].set_ylabel("Closed Alert Count (M)")
axes[1, 0].set_xticklabels(axes[1, 0].get_xticklabels(), rotation=0)

# 4. Frauds by Channel
channel_fraud = df.pivot_table(index="Data_Month", columns="Alert_closure", values="Total_No_of_Frauds_detected", aggfunc="sum").fillna(0)
channel_fraud.plot(kind="bar", stacked=True, colormap="coolwarm", ax=axes[1, 1])
axes[1, 1].set_title("#Fraud by Channels", fontsize=14, fontweight="bold", pad=30, loc='center')
axes[1, 1].legend(title=None, fontsize=10, loc="upper center", bbox_to_anchor=(0.5, 1.07), ncol=3, frameon=False)
axes[1, 1].set_xlabel("Month", labelpad=10)
axes[1, 1].set_ylabel("Fraud Count")
axes[1, 1].set_xticklabels(axes[1, 1].get_xticklabels(), rotation=0)

# 5. Avoidable Loss Chart
ax = axes[0, 2]
df_loss.plot(x="Data_Month", kind="bar", stacked=False, ax=ax, color=["deepskyblue", "gold"])
ax.set_title("Avoidable Loss", fontsize=14, fontweight="bold", pad=20)
ax.set_ylabel("Amount (M)", fontsize=12)
ax.set_xlabel("Month", fontsize=12)
ax.legend(["Monitoring Loss", "Avoidable Loss"], loc="upper left", frameon=False, fontsize=10)
axes[0, 2].legend(title=None, fontsize=10, loc="upper center", bbox_to_anchor=(0.5, 1.07), ncol=3, frameon=False)
axes[0, 2].set_xticklabels(axes[0, 2].get_xticklabels(), rotation=0)
ax.grid(axis="y", linestyle="--", alpha=0.5)

# 6. Pastel Summary Cards
def draw_futuristic_card(ax, x, y, main_text, label_text, color):
    # Create a rounded rectangle for the card
    box = FancyBboxPatch(
        (x, y), 0.5, 0.25, boxstyle="round,pad=0.02", linewidth=0,  # Removed border
        edgecolor=None, facecolor=color, zorder=2
    )
    ax.add_patch(box)

    # Add main text
    ax.text(
        x + 0.25, y + 0.17, main_text, fontsize=16, ha='center', weight='bold',
        color="black", zorder=3, path_effects=[patheffects.withStroke(linewidth=1, foreground="white")]
    )

    # Add label text
    ax.text(
        x + 0.25, y + 0.07, label_text, fontsize=12, ha='center', color="black",
        style="italic", zorder=3, path_effects=[patheffects.withStroke(linewidth=0.5, foreground="white")]
    )

# Create a subplot for the summary cards
axes[1, 2].axis("off")
card_ax = fig.add_axes([0.68, 0.05, 0.28, 0.4])  # [left, bottom, width, height] in 0-1 figure coords

# Aggressively remove all axis elements
card_ax.axis("off")  # Turn off the axis
card_ax.set_xticks([])  # Remove x-axis ticks
card_ax.set_yticks([])  # Remove y-axis ticks
card_ax.set_xticklabels([])  # Remove x-axis tick labels
card_ax.set_yticklabels([])  # Remove y-axis tick labels
for spine in card_ax.spines.values():  # Remove all spines (borders)
    spine.set_visible(False)

# Forcefully clear any residual grid lines
card_ax.grid(False)  # Ensure no grid lines are displayed
card_ax.patch.set_visible(False)  # Remove the background patch if it exists

# Define pastel colors and card positions
pastel_colors = ["#A8D5BA", "#B3DDF2", "#FFD3B6", "#D7BDE2"]
card_positions = [(0.05, 0.65), (0.55, 0.65), (0.05, 0.35), (0.55, 0.35), (0.05, 0.05), (0.55, 0.05)]

# Draw the summary cards
for i, row in df_summary.iterrows():
    if i * 2 + 1 < len(card_positions):
        x1, y1 = card_positions[i * 2]
        draw_futuristic_card(card_ax, x1, y1, f"{row['Total_Amount_Lost']:.1f}M", f"{row['Data_Month']} Avoidable Loss", pastel_colors[0])
        x2, y2 = card_positions[i * 2 + 1]
        draw_futuristic_card(card_ax, x2, y2, f"{row['Total_Amount_Saved']:.1f}M", f"{row['Data_Month']} Saved", pastel_colors[1])

# Remove grid lines from all other axes
for ax in axes.flatten():
    ax.grid(False)

# Save the figure as a PNG file in the "pngs" folder
output_dir = "pngs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "Canara_sheet2.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight')  # Save with high resolution
print(f"Dashboard saved as: {output_path}")
