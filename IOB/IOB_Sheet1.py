import os
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# MySQL Connection Configuration
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="#Harshu@123",
    database="TEST"
)
cursor = conn.cursor(dictionary=True)

# SQL Query with 'Arpil-25'
query = """
SELECT
    data_month,
    CAST(REPLACE(total_no_of_customers, 'M', '') AS DECIMAL(10,1)) AS customers,
    CAST(REPLACE(total_no_of_accounts, 'M', '') AS DECIMAL(10,1)) AS accounts,
    CAST(REPLACE(total_no_of_transaction_per_month, 'M', '') AS DECIMAL(10,1)) AS transactions,
    CAST(REPLACE(REPLACE(total_alert_for, ',', ''), 'K', '') AS UNSIGNED) AS alerts_generated,
    CAST(REPLACE(total_no_of_alert_closed, 'K', '') AS DECIMAL(10,1)) AS alerts_closed,
    CAST(REPLACE(REPLACE(total_no_of_frauds_detected, ',', ''), 'K', '') AS UNSIGNED) AS frauds_detected
FROM IOB
WHERE data_month IN ('March-25', 'Arpil-25')
ORDER BY FIELD(data_month, 'March-25', 'Arpil-25');
"""

cursor.execute(query)
data = pd.DataFrame(cursor.fetchall())
cursor.close()
conn.close()

# Convert to numeric
numeric_cols = ["customers", "accounts", "transactions", "alerts_generated", "alerts_closed", "frauds_detected"]
data[numeric_cols] = data[numeric_cols].apply(pd.to_numeric, errors='coerce')

# Group by month
data_grouped = data.groupby("data_month")[numeric_cols].sum().reset_index()

# Metrics
metrics = {
    "#Customers": data_grouped["customers"],
    "#Accounts": data_grouped["accounts"],
    "#Transaction Volume": data_grouped["transactions"],
    "Alerts Generated": data_grouped["alerts_generated"],
    "#Alerts Closed": data_grouped["alerts_closed"],
    "#Fraud Detected": data_grouped["frauds_detected"]
}

months = ['March', 'Arpil']
fig, axs = plt.subplots(2, 3, figsize=(18, 10))
fig.suptitle("Enterprise Fraud Management\nIOB BANK CXO DASHBOARDS - April-2025", fontsize=16, fontweight='bold')

axs = axs.flatten()
bar_width = 0.3
x_base = [0, 1]  # Base x positions for each month
offset = bar_width / 1.5  # Slight offset to bring bars closer

# Format numbers
def format_val(val):
    return f"{val / 1_000_000:.1f}M" if val >= 1_000_000 else f"{int(val):,}"

# Format numbers for y-axis
def format_y_axis(val, _):
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    elif val >= 1_000:
        return f"{val / 1_000:.1f}K"
    else:
        return f"{int(val)}"

# Plot
for i, (title, values) in enumerate(metrics.items()):
    march_val = values[data_grouped["data_month"] == 'March-25'].values[0] if 'March-25' in data_grouped["data_month"].values else 0
    arpil_val = values[data_grouped["data_month"] == 'Arpil-25'].values[0] if 'Arpil-25' in data_grouped["data_month"].values else 0

    axs[i].bar(x_base[0] - offset, march_val, width=bar_width, color="#1f4acc", label="March-25")
    axs[i].bar(x_base[0] + offset, arpil_val, width=bar_width, color="#ff9900", label="April-25")

    axs[i].set_title(title, fontsize=13, fontweight='bold')
    axs[i].set_xticks([x_base[0]])
    axs[i].set_xticklabels(["March-25 vs April-25"])
    axs[i].set_ylabel(title, fontsize=11)

    # Annotate
    axs[i].text(x_base[0] - offset, march_val, format_val(march_val), ha='center', va='bottom', fontsize=9)
    axs[i].text(x_base[0] + offset, arpil_val, format_val(arpil_val), ha='center', va='bottom', fontsize=9)

    # Apply custom y-axis formatter
    axs[i].yaxis.set_major_formatter(mticker.FuncFormatter(format_y_axis))

# Remove unused subplots
for j in range(len(metrics), len(axs)):
    fig.delaxes(axs[j])

# Global legend
fig.legend(["March-25", "April-25"], loc='upper center', bbox_to_anchor=(0.5, 0.88), ncol=2, fontsize=11)

plt.tight_layout(rect=[0, 0, 1, 0.85])


output_dir = "pngs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "sheet1.png")
plt.savefig(output_path)
print(f"✅ Dashboard saved as: {output_path}")