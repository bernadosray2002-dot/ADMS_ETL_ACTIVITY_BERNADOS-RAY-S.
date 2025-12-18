import pandas as pd
import sqlite3
import os

def clean_sqlite_table():
    """
    Reads from the staging folder, performs data cleaning, 
    and standardizes currency and columns.
    """
    # 1. Paths
    staging_dir = r"C:\Users\phant\OneDrive\Documents\ADMS Final\Data\Staging"
    # We will save the cleaned result to a new 'Transformation' database
    trans_db_path = os.path.join(staging_dir, "transformed_data.db")
    
    # Database sources
    db_j_path = os.path.join(staging_dir, "japan staging area.db")
    db_m_path = os.path.join(staging_dir, "myanmar staging area.db")

    # Check if staging databases exist
    if not os.path.exists(db_j_path) or not os.path.exists(db_m_path):
        print("[ERROR] Staging databases not found. Please check the directory.")
        return

    conn_trans = sqlite3.connect(trans_db_path)

    try:
        # --- 2. CLEAN JAPAN DATA ---
        print("Cleaning Japan data...")
        conn_j = sqlite3.connect(db_j_path)
        # We read the items specifically to fix the price
        df_j_items = pd.read_sql_query("SELECT * FROM japan_items", conn_j)
        
        # 1. Strip whitespace from column names
        df_j_items.columns = df_j_items.columns.str.strip()
        
        # 2. Strip whitespace from text columns
        df_j_items['product_name'] = df_j_items['product_name'].str.strip()
        
        # 3. Currency Conversion: JPY to USD (Approx 1 JPY = 0.0065 USD)
        df_j_items['price_usd'] = df_j_items['price'] * 0.0065
        
        # 4. Drop duplicates
        df_j_items = df_j_items.drop_duplicates()
        
        # Save to transformation DB
        df_j_items.to_sql("clean_japan_items", conn_trans, if_exists='replace', index=False)
        conn_j.close()

        # --- 3. CLEAN MYANMAR DATA ---
        print("Cleaning Myanmar data...")
        conn_m = sqlite3.connect(db_m_path)
        df_m_items = pd.read_sql_query("SELECT * FROM myanmar_items", conn_m)
        
        # 1. Strip whitespace
        df_m_items.columns = df_m_items.columns.str.strip()
        df_m_items['name'] = df_m_items['name'].str.strip()
        
        # 2. Standardization: Rename columns to match Japan
        # (Myanmar has 'name' and 'type' vs Japan's 'product_name' and 'category')
        df_m_items = df_m_items.rename(columns={'name': 'product_name', 'type': 'category'})
        
        # 3. Myanmar is already in USD, but let's create the column for consistency
        df_m_items['price_usd'] = df_m_items['price']
        
        # 4. Drop duplicates
        df_m_items = df_m_items.drop_duplicates()
        
        # Save to transformation DB
        df_m_items.to_sql("clean_myanmar_items", conn_trans, if_exists='replace', index=False)
        conn_m.close()

        print(f"\n[SUCCESS] Cleaned data saved to: {trans_db_path}")

    except Exception as e:
        print(f"[ERROR] Cleaning failed: {e}")
    finally:
        conn_trans.close()

if __name__ == "__main__":
    clean_sqlite_table()