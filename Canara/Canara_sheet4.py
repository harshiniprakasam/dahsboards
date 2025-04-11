import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import FancyBboxPatch

# SQL Server Database Connection
def fetch_data(query):
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=192.168.5.236;"
        "DATABASE=cxpsadm;"
        "UID=cxpsadm;"
        "PWD=c_xps123"
    )
    try:
        conn = pyodbc.connect(conn_str)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except pyodbc.Error as e:
        print(f"⚠️ Error: {e}")
        return None

# Fetch Data
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

# Clean and preprocess
numeric_columns = ["Total_No_of_Transaction", "TotalAlert", 
                   "Total_no_of_Alert_Closed", "Total_No_of_Frauds_detected", 
                   "Total_Amount_Lost", "Total_Amount_Saved"]
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

df = df[df["Channel"].isin(["BC", "IB", "MB"])]  # 🔍 Filter for selected channels

df["Total_no_of_Alert_Closed_M"] = df["Total_no_of_Alert_Closed"] / 1e6
df["Total_Amount_Lost_M"] = df["Total_Amount_Lost"] / 1e6
df["Total_No_of_Transaction_M"] = df["Total_No_of_Transaction"] / 1e6
df["Loss_Type"] = df["Type_of_SCN"].apply(lambda x: "Monitoring Loss" if x == "Monitoring" else "Avoidable Loss")

# Summaries
df_summary = df.groupby(["Data_Month", "Channel"])[["Total_Amount_Lost", "Total_Amount_Saved"]].sum().reset_index()
df_summary[["Total_Amount_Lost", "Total_Amount_Saved"]] /= 1e6

# Chart setup
sns.set_style("whitegrid")
fig, axes = plt.subplots(2, 3, figsize=(22, 12))

# 1. Transaction Volume by Channel
sns.barplot(data=df, x="Data_Month", y="Total_No_of_Transaction_M", hue="Channel", ax=axes[0, 0], palette="Blues")
axes[0, 0].set_title("Transaction Volume by Channel (M)", fontsize=14)
axes[0, 0].set_ylabel("Transactions (M)")
axes[0, 0].legend(title="Channel")

# 2. Alerts per Million Transactions by Channel
df["Alerts_per_Million"] = df["TotalAlert"] / (df["Total_No_of_Transaction"] / 1e6)
sns.barplot(data=df, x="Data_Month", y="Alerts_per_Million", hue="Channel", ax=axes[0, 1], palette="Reds")
axes[0, 1].set_title("Alerts per Million Transactions", fontsize=14)
axes[0, 1].set_ylabel("Alerts per M")
axes[0, 1].legend(title="Channel")

# 3. Alerts Closed by Channel and Closure Type
pivot_alerts = df.pivot_table(index=["Data_Month", "Channel"], columns="Alert_closure",
                              values="Total_no_of_Alert_Closed_M", aggfunc="sum").fillna(0)
pivot_alerts.plot(kind="bar", stacked=True, ax=axes[1, 0], colormap="viridis", width=0.8)
axes[1, 0].set_title("Alerts Closed by Closure Type & Channel", fontsize=14)
axes[1, 0].set_ylabel("Alerts Closed (M)")
axes[1, 0].legend(title="Closure Type", bbox_to_anchor=(1.05, 1))

# 4. Frauds by Channel and Closure Type
pivot_fraud = df.pivot_table(index=["Data_Month", "Channel"], columns="Alert_closure",
                             values="Total_No_of_Frauds_detected", aggfunc="sum").fillna(0)
pivot_fraud.plot(kind="bar", stacked=True, ax=axes[1, 1], colormap="coolwarm", width=0.8)
axes[1, 1].set_title("Fraud Cases by Closure Type & Channel", fontsize=14)
axes[1, 1].set_ylabel("Fraud Count")
axes[1, 1].legend(title="Closure Type", bbox_to_anchor=(1.05, 1))

# 5. Avoidable vs Monitoring Loss by Channel
pivot_loss = df.groupby(["Data_Month", "Channel", "Loss_Type"])["Total_Amount_Lost_M"].sum().unstack().fillna(0)
pivot_loss.plot(kind="bar", stacked=False, ax=axes[0, 2], color=["orange", "dodgerblue"], width=0.8)
axes[0, 2].set_title("Loss Type by Channel (M)", fontsize=14)
axes[0, 2].set_ylabel("Amount (M)")
axes[0, 2].legend(title="Loss Type")

# 6. Summary Cards – optional
axes[1, 2].axis("off")

# Layout tweaks
for ax in axes.flatten():
    ax.set_xlabel("")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0)

plt.tight_layout()
plt.show()
