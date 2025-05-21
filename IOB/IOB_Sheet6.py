import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
import matplotlib.ticker as mtick
import os

# Function to avoid overlapping text labels
def place_text_without_overlap(ax, x, y, text, placed_positions, xlim, ylim):
    offset_step = 0.015 * (ylim[1] - ylim[0])
    max_attempts = 10
    attempt = 0
    new_y = y
    while attempt < max_attempts:
        too_close = any(abs(new_y - existing_y) < offset_step for existing_y in placed_positions.get(x, []))
        if not too_close and ylim[0] <= new_y <= ylim[1]:
            break
        new_y += offset_step
        attempt += 1
    placed_positions.setdefault(x, []).append(new_y)

    ha = 'left' if x < (xlim[0] + xlim[1]) / 2 else 'right'
    offset_x = 0.01 * (xlim[1] - xlim[0]) * (1 if ha == 'left' else -1)

    ax.text(x + offset_x, new_y, text, fontsize=9, ha=ha, va='center', color='black', weight='bold')

# --- MySQL Connection ---
DB_SERVER = "localhost"
DB_NAME = "TEST"
DB_USER = "root"
DB_PASSWORD = "#Harshu@123"

try:
    conn = mysql.connector.connect(
        host=DB_SERVER,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    print("Connected to MySQL successfully.")
except mysql.connector.Error as e:
    print("Error connecting to MySQL:", e)
    exit()

# --- Query ---
query = """
SELECT 
    Bank,
    Alias,
    Accounts_Count,
    Transaction_Count,
    Scenario_count,
    Total_Alerts_per_month
FROM consolidate_data
"""

df = pd.read_sql(query, conn)
conn.close()

# --- Clean & Normalize ---
df['Bank'] = df['Bank'].astype(str).str.strip().str.upper()
df['Alias'] = df['Alias'].astype(str).str.strip()

# Ensure IOB alias is set properly
df.loc[df['Bank'] == 'IOB', 'Alias'] = 'IOB'

# --- Convert columns ---
cols = ['Total_Alerts_per_month', 'Accounts_Count', 'Transaction_Count', 'Scenario_count']
for col in cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df.dropna(subset=cols, inplace=True)
df = df[(df['Accounts_Count'] > 0) & (df['Transaction_Count'] > 0)]

# --- Metrics ---
df['Alerts_per_million_accounts'] = (df['Total_Alerts_per_month'] / df['Accounts_Count']) * 1_000_000
df['Alerts_per_million_transactions'] = (df['Total_Alerts_per_month'] / df['Transaction_Count']) * 1_000_000

# --- Color palette ---
unique_aliases = df["Alias"].unique()
palette = sns.color_palette("tab10", len(unique_aliases))
color_map = dict(zip(unique_aliases, palette))
df["Color"] = df["Alias"].map(color_map)

# --- Plot ---
fig, axes = plt.subplots(1, 2, figsize=(18, 8))
fig.suptitle("Density Distribution of Alerts Per Million Accounts\nacross Clari5 Clients",
             fontsize=16, fontweight='bold')

# Formatter
y_formatter = mtick.FuncFormatter(lambda x, _: f'{x/1e3:.0f}k')
x_formatter = mtick.FuncFormatter(lambda x, _: f'{x/1e6:.0f}M')

# --- Plot 1: Accounts vs Alerts per million (accounts) ---
ax1 = axes[0]
ax1.scatter(
    df['Accounts_Count'],
    df['Alerts_per_million_accounts'],
    s=df['Total_Alerts_per_month'] / 20 + 50,
    c=df['Color'],
    edgecolors="black",
    alpha=0.8
)
ax1.set_title("Accounts vs Alerts per Million Accounts", fontsize=13, fontweight='bold')
ax1.set_xlabel("Accounts Count", fontsize=11, fontweight='bold')
ax1.set_ylabel("Alerts per Million Accounts", fontsize=11, fontweight='bold')
ax1.xaxis.set_major_formatter(x_formatter)
ax1.yaxis.set_major_formatter(y_formatter)
ax1.set_facecolor('white')

xlim1 = ax1.get_xlim()
ylim1 = ax1.get_ylim()
placed_y_1 = {}

for _, row in df.iterrows():
    place_text_without_overlap(ax1, row["Accounts_Count"], row["Alerts_per_million_accounts"],
                               row["Alias"], placed_y_1, xlim1, ylim1)

# --- Plot 2: Transactions vs Alerts per million (accounts again) ---
ax2 = axes[1]
ax2.scatter(
    df['Transaction_Count'],
    df['Alerts_per_million_accounts'],  # using same Y-axis metric
    s=df['Total_Alerts_per_month'] / 20 + 50,
    c=df['Color'],
    edgecolors="black",
    alpha=0.8
)
ax2.set_title("Transactions vs Alerts per Million Accounts", fontsize=13, fontweight='bold')
ax2.set_xlabel("Transaction Count", fontsize=11, fontweight='bold')
ax2.set_ylabel("Alerts per Million Accounts", fontsize=11, fontweight='bold')
ax2.xaxis.set_major_formatter(x_formatter)
ax2.yaxis.set_major_formatter(y_formatter)
ax2.set_facecolor('white')

xlim2 = ax2.get_xlim()
ylim2 = ax2.get_ylim()
placed_y_2 = {}

for _, row in df.iterrows():
    place_text_without_overlap(ax2, row["Transaction_Count"], row["Alerts_per_million_accounts"],
                               row["Alias"], placed_y_2, xlim2, ylim2)

# --- Legend ---
handles = []
labels = []
for alias in unique_aliases:
    scenario_count = df[df["Alias"] == alias]["Scenario_count"].iloc[0]
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

# --- Save Output (Optional) ---
output_dir = "pngs"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "sheet6.png")
plt.savefig(output_path)
print(f" Dashboard saved as: {output_path}")
