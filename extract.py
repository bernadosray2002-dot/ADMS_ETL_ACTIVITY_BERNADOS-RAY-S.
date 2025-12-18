import pandas as pd
import sqlite3

def load_csv():
    """
    Loads all Japan and Myanmar CSV files into their respective SQLite staging databases.
    Assumes the CSVs are in the same folder as this script.
    """

    # --------------------------
    # 1. CSV files and table mapping
    # --------------------------
    japan_store = {
        "japan_branch.csv": "branch",
        "japan_Customers.csv": "customers",
        "japan_items.csv": "items",
        "japan_payment.csv": "payment",
        "sales_data.csv": "sales_data"
    }

    myanmar_store = {
        "myanmar_branch.csv": "branch",
        "myanmar_customers.csv": "customers",
        "myanmar_items.csv": "items",
        "myanmar_payments.csv": "payments",
        "sales_data.csv": "sales_data"
    }

    # --------------------------
    # 2. SQLite database connections
    # --------------------------
    conn_japan = sqlite3.connect("japan_staging_area.db")
    conn_myanmar = sqlite3.connect("myanmar_staging_area.db")

    try:
        # --------------------------
        # 3. Load Japan CSVs
        # --------------------------
        print("Loading Japan CSVs into staging database...")
        for csv_file, table_name in japan_store.items():
            df = pd.read_csv(csv_file)  # Read CSV
            df.to_sql(table_name, conn_japan, if_exists="replace", index=False)  # Write to SQLite
            print(f" - {csv_file} → table '{table_name}' ({len(df)} rows)")

        # --------------------------
        # 4. Load Myanmar CSVs
        # --------------------------
        print("\nLoading Myanmar CSVs into staging database...")
        for csv_file, table_name in myanmar_store.items():
            df = pd.read_csv(csv_file)
            df.to_sql(table_name, conn_myanmar, if_exists="replace", index=False)
            print(f" - {csv_file} → table '{table_name}' ({len(df)} rows)")

        # --------------------------
        # 5. Optional: print first 5 rows of each table for verification
        # --------------------------
        print("\nSample data from Japan staging DB:")
        for table_name in japan_store.values():
            sample = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5;", conn_japan)
            print(f"\nTable '{table_name}':\n{sample}")

        print("\nSample data from Myanmar staging DB:")
        for table_name in myanmar_store.values():
            sample = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5;", conn_myanmar)
            print(f"\nTable '{table_name}':\n{sample}")

        print("\n✅ All CSVs loaded into staging databases successfully!")

    except Exception as e:
        print("Error during extraction:", e)

    finally:
        conn_japan.close()
        conn_myanmar.close()



load_csv()
