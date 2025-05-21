import os
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="#Harshu@123",
    database="TEST"
)
cursor = conn.cursor(dictionary=True)

# SQL query to aggregate values by channel
query = """
SELECT 
    channel,
    CAST(REPLACE(REPLACE(total_alert_for, ',', ''), 'K', '') AS UNSIGNED) AS alerts_generated,
    CAST(REPLACE(REPLACE(total_no_of_alert_closed, ',', ''), 'K', '') AS UNSIGNED) AS alerts_closed,
    CAST(REPLACE(REPLACE(total_no_of_frauds_detected, ',', ''), 'K', '') AS UNSIGNED) AS frauds_detected
FROM IOB;
"""

cursor.execute(query)
data = pd.DataFrame(cursor.fetchall())
cursor.close()
conn.close()

# Group by channel and sum values
grouped = data.groupby("channel")[["alerts_generated", "alerts_closed", "frauds_detected"]].sum()

# Drop channels with all 0s
grouped = grouped[(grouped.T != 0).any()]

# Function to plot a donut chart
def plot_donut(ax, values, labels, title):
    colors = plt.cm.tab10.colors + plt.cm.Pastel1.colors
    total = sum(values)
    
    def make_autopct(values):
        def my_autopct(pct):
            val = int(round(pct * total / 100.0))
            return f"{val:,} ({pct:.1f}%)" if pct > 2 else ''
        return my_autopct

    wedges, texts, autotexts = ax.pie(
        values,
        startangle=90,
        counterclock=False,
        wedgeprops={'width': 0.4, 'edgecolor': 'white'},
        colors=colors,
        autopct=make_autopct(values)
    )

    ax.set_title(title, fontweight='bold')
    ax.legend(labels, loc='center left', bbox_to_anchor=(1.0, 0.5), fontsize=8)


# Plot setup
fig, axs = plt.subplots(2, 2, figsize=(12, 8))
fig.suptitle("IOB BANK CXO DASHBOARDS - April 2025", fontsize=16, fontweight='bold')

titles = ["Alerts Generated", "Alert Closure", "Fraud Detected"]
cols = ["alerts_generated", "alerts_closed", "frauds_detected"]

for i, (title, col) in enumerate(zip(titles, cols)):
    ax = axs[i // 2, i % 2]
    values = grouped[col]
    labels = grouped.index.tolist()
    plot_donut(ax, values.values, labels, title)

# Adjust the position of the "Alert Closure" chart
axs[0, 1].set_position([0.55, 0.38, 0.35, 0.35])  # bottom changed from 0.55 to 0.38

# Hide empty fourth subplot
axs[1, 1].axis('off')

plt.tight_layout(rect=[0, 0, 1, 0.95])


output_dir = "pngs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "sheet2.png")
plt.savefig(output_path)
print(f" Dashboard saved as: {output_path}")

