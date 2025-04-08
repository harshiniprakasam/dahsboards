import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Style and colors
sns.set_style("whitegrid")
plt.rcParams.update({'font.size': 11, 'font.family': 'Arial'})
blue = '#00AEEF'

# DB fetch
def fetch_upi_data():
    query = """
        SELECT 
            Month_number,
            Total_No_of_Transaction,
            TotalAlert,
            Total_no_of_Alert_Closed,
            Total_Amount_Lost,
            Total_Amount_Saved
        FROM CANARA
        WHERE Channel = 'UPI' AND Month_number IN (1, 2)
    """
    config = {
        "host": "localhost",
        "database": "TEST",
        "user": "root",
        "password": "#Harshu@123"
    }

    with mysql.connector.connect(**config) as conn:
        df = pd.read_sql(query, conn)
        df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
        df['Month'] = df['Month_number'].map({1: 'Jan-25', 2: 'Feb-25'})
        return df

# Plot
def plot_upi_dashboard(df):
    months = ['Jan-25', 'Feb-25']
    grouped = df.groupby('Month').sum().reindex(months)

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle("UPI CHANNEL Alert Details", fontsize=22, fontweight='bold', y=0.98)

    # Chart 1: Transaction Count
    ax1 = axes[0, 0]
    trans_billion = grouped['Total_No_of_Transaction'] / 1e9
    sns.barplot(x=grouped.index, y=trans_billion, ax=ax1, color=blue)
    ax1.set_title("Transaction Details", fontsize=15, fontweight='bold')
    ax1.set_ylabel("Tran Count (in Billion)")
    ax1.set_xlabel("Month")
    ax1.set_ylim(0, trans_billion.max() * 1.15)
    ax1.bar_label(ax1.containers[0], fmt="%.2fb", padding=3)
    ax1.grid(False)

    # Chart 2: Alerts per Million
    alert_per_million = (grouped['TotalAlert'] / (grouped['Total_No_of_Transaction'] / 1_000_000)).fillna(0)
    ax2 = axes[0, 1]
    sns.barplot(x=grouped.index, y=alert_per_million, ax=ax2, color=blue)
    ax2.set_title("Alerts Per Million Transactions", fontsize=15, fontweight='bold')
    ax2.set_ylabel("Alert Count")
    ax2.set_xlabel("Month")
    ax2.set_ylim(0, alert_per_million.max() * 1.15)
    ax2.bar_label(ax2.containers[0], fmt="%.0f", padding=3)
    ax2.grid(False)

    # Chart 3: Closed Alert Count
    ax3 = axes[0, 2]
    closed = grouped['Total_no_of_Alert_Closed']
    sns.barplot(x=grouped.index, y=closed, ax=ax3, color=blue)
    ax3.set_title("Closed Alerts", fontsize=15, fontweight='bold')
    ax3.set_ylabel("Alert Count")
    ax3.set_xlabel("Month")
    ax3.set_ylim(0, closed.max() * 1.15)
    labels_closed = [f"{v/1000:.0f}K" for v in closed]
    ax3.bar_label(ax3.containers[0], labels=labels_closed, padding=3)
    ax3.grid(False)

    # Chart 4: Avoidable Loss
    ax6 = axes[1, 2]
    loss = grouped['Total_Amount_Lost']
    sns.barplot(x=grouped.index, y=loss, ax=ax6, color=blue)
    ax6.set_title("Avoidable Loss", fontsize=15, fontweight='bold')
    ax6.set_ylabel("Amount (INR)")
    ax6.set_xlabel("Month")
    ax6.set_ylim(0, loss.max() * 1.15)
    labels_loss = [f"{v/1000:.1f}K" for v in loss]
    ax6.bar_label(ax6.containers[0], labels=labels_loss, padding=3)
    ax6.grid(False)

    # Fancy Info Cards
    for i, month in enumerate(months):
        loss_amt = grouped.loc[month, 'Total_Amount_Lost'] / 1000
        saved_amt = grouped.loc[month, 'Total_Amount_Saved'] / 1000

        # X coordinate for positioning (left to right)
        x_base = 0.22 + i * 0.26

        # Month label
        fig.text(x_base, 0.33, f"{month}", fontsize=16, weight='bold', ha='center')

        # Loss card
        fig.text(
            x_base, 0.26,
            f"{loss_amt:,.1f}K INR\nAvoidable Loss",
            fontsize=14, weight='bold', ha='center', va='center',
            bbox=dict(boxstyle="round,pad=0.7", facecolor='#ffdddd', edgecolor='gray', linewidth=1.2)
        )

        # Saved card
        fig.text(
            x_base, 0.13,
            f"{saved_amt:,.1f}K INR\nSaved",
            fontsize=14, weight='bold', ha='center', va='center',
            bbox=dict(boxstyle="round,pad=0.7", facecolor='#ddffdd', edgecolor='gray', linewidth=1.2)
        )

    # Hide unused subplots
    axes[1, 0].axis('off')
    axes[1, 1].axis('off')

    # Adjust layout
    plt.tight_layout(rect=[0, 0.05, 1, 0.93])
    plt.show()

# Execute
upi_df = fetch_upi_data()
if not upi_df.empty:
    plot_upi_dashboard(upi_df)
