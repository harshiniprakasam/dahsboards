import os
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# Fetch data
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

# Month mapping
month_map = {'3': 'MAR-25', '4': 'APR-25'}
data['month'] = data['month_number'].map(month_map)
month_order = ['MAR-25', 'APR-25']
data['month'] = pd.Categorical(data['month'], categories=month_order, ordered=True)

# Channel filters
dc_cc_data = data[data['channel'].str.contains('Debit Card|Credit Card', case=False, na=False)]
other_channels = data[~data['channel'].str.contains('UPI|IB|MB|Debit Card|Credit Card', case=False, na=False)]

# Pivot helper
def pivot_grouped(df, value_col):
    return df.pivot_table(index='month', columns='channel', values=value_col, aggfunc='sum').fillna(0)

# Pivot data
dc_cc_generated = pivot_grouped(dc_cc_data, 'alerts_generated')
dc_cc_closed = pivot_grouped(dc_cc_data, 'alerts_closed')
dc_cc_frauds = pivot_grouped(dc_cc_data, 'frauds_detected')

other_generated = pivot_grouped(other_channels, 'alerts_generated')
other_closed = pivot_grouped(other_channels, 'alerts_closed')
other_frauds = pivot_grouped(other_channels, 'frauds_detected')

# Plotting
fig, axs = plt.subplots(2, 3, figsize=(16, 8))
plt.subplots_adjust(top=0.93, hspace=1.5)

# Titles
fig.suptitle("CBS CHANNEL Alert Details", fontsize=11, fontweight='bold', y=0.98)
fig.text(0.5, 0.48, "PC, BBPS, OTHER REMITTANCE, QR, FI, BRANCH, SWIFT CHANNEL Alert Details", 
         ha='center', fontsize=10, fontweight='bold')

# Plotting function
# Plotting function
def plot_grouped(ax, df, title=None, show_xlabel=True):
    bars = df.plot(kind='bar', ax=ax)
    if title:
        ax.set_title(title, fontweight='bold', fontsize=11)
    if show_xlabel:
        ax.set_xlabel("Month")
    else:
        ax.set_xlabel("")  # Remove x-axis label
    ax.set_ylabel(df.columns.name or '')
    ax.tick_params(axis='x', rotation=0)
    for container in ax.containers:
        for bar in container:
            height = bar.get_height()
            ax.annotate(f'{int(height):,}', (bar.get_x() + bar.get_width() / 2, height + 100),
                        ha='center', va='bottom', fontsize=8)
    return bars



# Top row: DC/CC
plot_grouped(axs[0, 0], dc_cc_generated, "Alerts Generated Details")
plot_grouped(axs[0, 1], dc_cc_closed, "Closed Alert Details", show_xlabel=False)  # Remove x-axis label
plot_grouped(axs[0, 2], dc_cc_frauds, "Fraud Details")


# Bottom row: Other Channels
plot_grouped(axs[1, 0], other_generated)
plot_grouped(axs[1, 1], other_closed)
plot_grouped(axs[1, 2], other_frauds)

# Legends
top_handles, top_labels = axs[0, 0].get_legend_handles_labels()
bot_handles, bot_labels = axs[1, 0].get_legend_handles_labels()
for ax in axs.flatten():
    ax.legend().remove()

# Top legend
fig.legend(top_handles, top_labels, loc='upper center', bbox_to_anchor=(0.5, 0.96),
           ncol=len(top_labels), fontsize=9, frameon=False)

# Bottom legend (moved closer)
fig.legend(bot_handles, bot_labels, loc='upper center', bbox_to_anchor=(0.5, 0.10),
           ncol=len(bot_labels), fontsize=9, frameon=False)

# Adjust layout to reduce bottom space
plt.tight_layout(rect=[0, 0.08, 1, 0.93])


output_dir = "pngs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "sheet4.png")
plt.savefig(output_path)
print(f"Dashboard saved as: {output_path}")