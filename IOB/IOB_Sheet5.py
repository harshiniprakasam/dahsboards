import os
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

# Connect to your local MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="#Harshu@123",
    database="TEST"
)
cursor = conn.cursor(dictionary=True)

# Fetch ONLY March and April data
query = """
SELECT 
    month_number,
    SUM(CAST(REPLACE(REPLACE(total_alert_for, ',', ''), 'K', '') AS UNSIGNED)) AS alerts_generated,
    SUM(CAST(REPLACE(REPLACE(total_no_of_alert_closed, ',', ''), 'K', '') AS UNSIGNED)) AS alerts_closed,
    SUM(CAST(REPLACE(REPLACE(total_no_of_frauds_detected, ',', ''), 'K', '') AS UNSIGNED)) AS frauds_detected
FROM IOB
WHERE month_number IN ('3', '4')
GROUP BY month_number;
"""
cursor.execute(query)
data = pd.DataFrame(cursor.fetchall())
cursor.close()
conn.close()

# Sort months
month_map = {'3': 'MAR-25', '4': 'APR-25'}
data['month'] = data['month_number'].map(month_map)
month_order = ['MAR-25', 'APR-25']
data['month'] = pd.Categorical(data['month'], categories=month_order, ordered=True)
data = data.sort_values('month')

# Create a "T" layout using GridSpec
fig = plt.figure(figsize=(13, 7))
gs = gridspec.GridSpec(2, 2, height_ratios=[1, 1.1])  # Slightly more space for the bottom chart

ax1 = fig.add_subplot(gs[0, 0])  # Top left
ax2 = fig.add_subplot(gs[0, 1])  # Top right
ax3 = fig.add_subplot(gs[1, :])  # Bottom, spanning both columns

def format_val(val, kind='K'):
    val = float(val)
    if kind == 'M':
        return f"{val / 1e6:.2f}M"
    elif kind == 'K':
        return f"{val / 1e3:.0f}K"
    return str(val)

# Monthly Alerts Generated
ax1.plot(data['month'], data['alerts_generated'], marker='o', color='blue')
ax1.set_title("Monthly Alerts Generated Trend", fontweight='bold')
ax1.set_ylabel("Alerts Count", fontweight='bold')
ax1.set_xlabel("Month", fontweight='bold')
ax1.grid(True, linestyle=':', linewidth=0.8)
for i, val in enumerate(data['alerts_generated']):
    valf = float(val)
    ax1.text(i, valf + valf * 0.01 + 100, format_val(valf, 'M'), ha='center', fontsize=9, fontweight='bold', color='purple')

# Monthly Alerts Closed
ax2.plot(data['month'], data['alerts_closed'], marker='o', color='blue')
ax2.set_title("Monthly Alerts Closed Trend", fontweight='bold')
ax2.set_ylabel("Closed alerts", fontweight='bold')
ax2.set_xlabel("Month", fontweight='bold')
ax2.grid(True, linestyle=':', linewidth=0.8)
for i, val in enumerate(data['alerts_closed']):
    valf = float(val)
    ax2.text(i, valf + valf * 0.004 + 100, format_val(valf), ha='center', fontsize=9, fontweight='bold', color='purple')

# Monthly Fraud Detected (bottom, spanning both columns)
ax3.plot(data['month'], data['frauds_detected'], marker='o', color='blue')
ax3.set_title("Monthly Fraud Detected Trend", fontweight='bold')
ax3.set_ylabel("Fraud detected", fontweight='bold')
ax3.set_xlabel("Month", fontweight='bold')
ax3.grid(True, linestyle=':', linewidth=0.8)
ax3.set_xticks(range(len(data)))
ax3.set_xticklabels(data['month'].astype(str))
for i, val in enumerate(data['frauds_detected']):
    valf = float(val)
    ax3.text(i, valf + max(valf * 0.02, 10), f"{int(valf):,}", ha='center', fontsize=9, fontweight='bold', color='purple')

plt.tight_layout()

output_dir = "pngs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "sheet5.png")
plt.savefig(output_path)
print(f"Dashboard saved as: {output_path}")