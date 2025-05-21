import os
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Fetch data from the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="#Harshu@123",
    database="TEST"
)
cursor = conn.cursor(dictionary=True)

query = """
SELECT 
    channel,
    month_number,
    CAST(REPLACE(REPLACE(total_alert_for, ',', ''), 'K', '') AS UNSIGNED) AS alerts_generated,
    CAST(REPLACE(REPLACE(total_no_of_alert_closed, ',', ''), 'K', '') AS UNSIGNED) AS alerts_closed,
    CAST(REPLACE(REPLACE(total_no_of_frauds_detected, ',', ''), 'K', '') AS UNSIGNED) AS frauds_detected
FROM IOB
WHERE month_number IN ('3', '4');
"""

cursor.execute(query)
data = pd.DataFrame(cursor.fetchall())
cursor.close()
conn.close()

# Step 2: Preprocess data
month_map = {'3': 'MAR-25', '4': 'APR-25'}
data['month'] = data['month_number'].map(month_map)
month_order = ['MAR-25', 'APR-25']
data['month'] = pd.Categorical(data['month'], categories=month_order, ordered=True)

# Step 3: Split into UPI and MB/IB data
upi_raw = data[data['channel'].str.contains('UPI', case=False, na=False)]
upi = upi_raw.groupby('month').agg({
    'alerts_generated': 'sum',
    'alerts_closed': 'sum',
    'frauds_detected': 'sum'
}).reset_index()

mb_ib = data[data['channel'].isin(['MB', 'IB'])]

# Step 4: Visualization setup
fig, axs = plt.subplots(2, 3, figsize=(16, 9))
fig.suptitle("IOB CHANNEL Alert Details - April 2025", fontsize=18, fontweight='bold')

# Adjust layout for section headings and spacing
plt.subplots_adjust(top=0.88, hspace=0.6)

# Section headings (custom y-positions)
fig.text(0.5, 0.92, "UPI/BHIM/QR CHANNEL Alert Details", ha='center', va='center', fontsize=14, fontweight='bold')
fig.text(0.5, 0.44, "IB, MB CHANNEL Alert Details", ha='center', va='center', fontsize=14, fontweight='bold')

# ---------- UPI/BHIM/QR Bar Charts ----------
def plot_bar(ax, df, col, title, show_xlabel=True):
    bars = ax.bar(df['month'], df[col], color='royalblue')
    ax.set_title(title, fontweight='bold')
    if show_xlabel:
        ax.set_xlabel("Month")
    else:
        ax.set_xlabel("")
    # Dynamically adjust the y-axis limit to ensure annotations fit
    max_height = df[col].max()
    ax.set_ylim(top=1.3 * max_height)  # Add 30% padding above the tallest bar

    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height + 0.05 * max_height,  # Add padding above the bar
                f'{int(height):,}',
                ha='center',
                va='bottom',
                fontsize=9
            )

plot_bar(axs[0, 0], upi, 'alerts_generated', "Alerts Generated Details")
plot_bar(axs[0, 1], upi, 'alerts_closed', "Closed Alert Details", show_xlabel=False)  # No x-axis label
plot_bar(axs[0, 2], upi, 'frauds_detected', "Fraud Details")

# ---------- MB & IB Grouped Bar Charts ----------
def plot_group_bar(ax, df, col, title=None):  # Make title optional
    pivot = df.pivot_table(index='month', columns='channel', values=col, aggfunc='sum').fillna(0)
    pivot.plot(kind='bar', ax=ax, color={'MB': 'orange', 'IB': 'royalblue'}, legend=True)
    if title:
        ax.set_title(title, fontweight='bold')
    ax.set_xlabel("Month")
    ax.set_xticklabels(pivot.index, rotation=0, ha='center')
    ax.legend(title="Channel")

    # Dynamically adjust the y-axis limit to ensure annotations fit
    max_height = pivot.values.max()
    ax.set_ylim(top=1.3 * max_height)  # Add 30% padding above the tallest bar

    for p in ax.patches:
        height = p.get_height()
        if height > 1:
            ax.annotate(
                f'{int(height):,}',
                (p.get_x() + p.get_width() / 2, height + 0.05 * max_height),
                ha='center',
                va='bottom',
                fontsize=8
            )

plot_group_bar(axs[1, 0], mb_ib, 'alerts_generated')  # No title
plot_group_bar(axs[1, 1], mb_ib, 'alerts_closed')     # No title
plot_group_bar(axs[1, 2], mb_ib, 'frauds_detected')   # No title

# Final layout tweak
plt.tight_layout(rect=[0, 0.03, 1, 0.9])


output_dir = "pngs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "sheet3.png")
plt.savefig(output_path)
print(f"Dashboard saved as: {output_path}")