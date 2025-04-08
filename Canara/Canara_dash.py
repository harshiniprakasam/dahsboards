import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Database Connection Function
def fetch_data(query):
    """Fetch data from MySQL database."""
    DB_SERVER = "localhost"
    DB_NAME = "TEST"
    DB_USER = "root"
    DB_PASSWORD = "#Harshu@123"

    try:
        with mysql.connector.connect(
            host=DB_SERVER, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        ) as conn, conn.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data, columns=columns)

        if df.empty:
            print(f"⚠️ Warning: No data returned for query: {query}")
            return None

        # Convert second column to numeric and filter out zero values
        df[df.columns[1]] = pd.to_numeric(df[df.columns[1]], errors='coerce').fillna(0)
        df = df[df[df.columns[1]] > 0]  # Remove zero-value rows

        if df.empty:
            print("⚠️ Warning: All values are zero. Skipping chart.")
            return None

        print(f"✅ Data fetched:{df}")
        return df

    except mysql.connector.Error as e:
        print(f"⚠️ Error connecting to MySQL: {e}")
        return None

# Function to Create Donut Chart with Total Value in Millions
def create_donut_chart(ax, values, labels, title):
    """Create a donut chart while handling empty or zero values properly."""
    if values.isnull().all() or values.sum() == 0:
        ax.text(0, 0, "No Data", ha='center', va='center', fontsize=12, fontweight='bold')
        ax.set_xticks([])
        ax.set_yticks([])
        return

    try:
        # Ensure valid hex colors
        colors = sns.color_palette("husl", len(values)).as_hex()

        wedges, texts, autotexts = ax.pie(
            values, labels=None, autopct=lambda p: f'{p:.1f}%' if p > 2 else '',
            colors=colors,
            startangle=140, wedgeprops={'edgecolor': 'white'},
            pctdistance=0.85
        )

        # Create the inner white circle (donut effect)
        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        ax.add_patch(centre_circle)
        ax.set_title(title, fontsize=14, fontweight='bold')

        # Display total value inside the donut chart in millions or exact value if < 1M
        total_value = values.sum()
        formatted_total = f"{total_value / 1_000_000:.0f}M" if total_value >= 1_000_000 else f"{total_value:,.0f}"
        ax.text(0, 0, formatted_total, ha='center', va='center', fontsize=14, fontweight='bold')

        # Create a legend with circular markers
        legend_patches = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=c, markersize=10) for c in colors]
        ax.legend(legend_patches, labels, title="Channel", loc="center left", bbox_to_anchor=(1, 0.5), fontsize=10)

    except Exception as e:
        ax.text(0, 0, f"Error: {str(e)}", ha='center', va='center', fontsize=10, color='red')
        ax.set_xticks([])
        ax.set_yticks([])
        print(f"⚠️ Error while creating chart for {title}: {e}")

# Function to Generate the Dashboard
def generate_dashboard():
    """Fetch data for February and generate the dashboard."""
    queries = {
        "Transaction Volume": """
            SELECT Channel, COALESCE(SUM(Total_No_of_Transaction), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Alerts Per Million Transaction Volume": """
            SELECT Channel, COALESCE(SUM(TotalAlert), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Alert Closure": """
            SELECT Channel, COALESCE(SUM(Total_no_of_Alert_Closed), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Fraud Detected": """
            SELECT Channel, COALESCE(SUM(Total_no_of_Frauds_detected), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Saved Amount": """
            SELECT Channel, COALESCE(SUM(Total_Amount_Saved), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """,
        "Avoidable Lost Amount": """
            SELECT Channel, COALESCE(SUM(Total_Amount_Lost), 0) 
            FROM CANARA 
            WHERE Month_number = 2
            GROUP BY Channel
        """
    }

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('Enterprise Fraud Management - Canara Bank CXO Dashboard (February 2025)', fontsize=16, fontweight='bold')

    for ax, (title, query) in zip(axes.flatten(), queries.items()):
        df = fetch_data(query)
        if df is not None and not df.empty:
            create_donut_chart(ax, df.iloc[:, 1], df.iloc[:, 0], title)
        else:
            ax.text(0.5, 0.5, "No Data", ha='center', va='center', fontsize=12, fontweight='bold')

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()

# Generate the dashboard
generate_dashboard()
