import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import FancyBboxPatch

# MySQL Database Connection
def fetch_data(query):
    """Fetch data from MySQL database."""
    DB_SERVER = "localhost"
    DB_NAME = "TEST"
    DB_USER = "root"
    DB_PASSWORD = "#Harshu@123"

    try:
        conn = mysql.connector.connect(
            host=DB_SERVER, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except mysql.connector.Error as e:
        print(f"⚠️ Error: {e}")
        return None

# Query to Fetch Required Data
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

numeric_columns = ["Total_No_of_Transaction", "TotalAlert", 
                   "Total_no_of_Alert_Closed", "Total_No_of_Frauds_detected", 
                   "Total_Amount_Lost", "Total_Amount_Saved"]
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

# Convert alert closures & losses to millions
df["Total_no_of_Alert_Closed_M"] = df["Total_no_of_Alert_Closed"] / 1e6
df["Total_Amount_Lost_M"] = df["Total_Amount_Lost"] / 1e6

# Categorize Avoidable Loss vs Monitoring Loss
df["Loss_Type"] = df["Type_of_SCN"].apply(lambda x: "Monitoring Loss" if x == "Monitoring" else "Avoidable Loss")

# Aggregate Losses by Month
df_loss = df.groupby(["Data_Month", "Loss_Type"])["Total_Amount_Lost_M"].sum().unstack().fillna(0).reset_index()

# ✅ Group by Month and Sum Avoidable Loss & Saved Amount
df_summary = df.groupby("Data_Month")[["Total_Amount_Lost", "Total_Amount_Saved"]].sum().reset_index()
df_summary[["Total_Amount_Lost", "Total_Amount_Saved"]] /= 1e6  # Convert to Millions

# Set dashboard style
sns.set_style("white")
fig, axes = plt.subplots(2, 3, figsize=(20, 10))

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
axes[1, 0].legend(title=None, fontsize=10, loc="upper center", bbox_to_anchor=(0.5, 1.15), ncol=3, frameon=False)
axes[1, 0].set_xlabel("Month", labelpad=10)
axes[1, 0].set_ylabel("Closed Alert Count (M)")
axes[1, 0].set_xticklabels(axes[1, 0].get_xticklabels(), rotation=0)

# 4. Frauds by Channel
channel_fraud = df.pivot_table(index="Data_Month", columns="Alert_closure", values="Total_No_of_Frauds_detected", aggfunc="sum").fillna(0)
channel_fraud.plot(kind="bar", stacked=True, colormap="coolwarm", ax=axes[1, 1])
axes[1, 1].set_title("#Fraud by Channels", fontsize=14, fontweight="bold", pad=30, loc='center')
axes[1, 1].legend(title=None, fontsize=10, loc="upper center", bbox_to_anchor=(0.5, 1.15), ncol=3, frameon=False)
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
axes[0, 2].legend(title=None, fontsize=10, loc="upper center", bbox_to_anchor=(0.5, 1.1), ncol=3, frameon=False)
axes[0, 2].set_xticklabels(axes[0, 2].get_xticklabels(), rotation=0)
ax.grid(axis="y", linestyle="--", alpha=0.5)

# 6. Summary Cards for Jan & Feb
axes[1, 2].axis("off")  # clear the second slot too for full height card layout

def draw_card(ax, x, y, main_text, label_text, bgcolor):
    box = FancyBboxPatch((x, y), 0.4, 0.18,
                         boxstyle="round,pad=0.02",
                         linewidth=1.5,
                         edgecolor="black",
                         facecolor=bgcolor)
    ax.add_patch(box)
    ax.text(x + 0.2, y + 0.12, main_text, fontsize=14, ha='center', weight='bold')
    ax.text(x + 0.2, y + 0.06, label_text, fontsize=12, ha='center')

# Use axes[0,2] for top 2 rows, and axes[1,2] for next
card_positions = [(0.05, 0.7), (0.55, 0.7), (0.05, 0.4), (0.55, 0.4), (0.05, 0.1), (0.55, 0.1)]
colors = ["#FFF3E0", "#E8F5E9", "#FFEBEE", "#E3F2FD", "#FCE4EC", "#E0F7FA"]
card_ax = fig.add_subplot(2, 3, 6)  # occupy [1,2]
card_ax.axis("off")

for i, row in df_summary.iterrows():
    if i < len(card_positions):  # Limit to 3 months (6 cards)
        x, y = card_positions[i*2]
        draw_card(card_ax, x, y, f"{row['Total_Amount_Lost']:.1f}M", f"{row['Data_Month']} Avoidable Loss", colors[i*2])
        x, y = card_positions[i*2 + 1]
        draw_card(card_ax, x, y, f"{row['Total_Amount_Saved']:.1f}M", f"{row['Data_Month']} Saved", colors[i*2 + 1])

# Clean up all grid lines
for ax in axes.flatten():
    ax.grid(False)

plt.subplots_adjust(hspace=0.5, wspace=0.5)
plt.show()

