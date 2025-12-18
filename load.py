import pandas as pd
import sqlite3
import os

def load_presentation():
    # ... (paths same as before)
    presentation_dir = r"C:\Users\phant\OneDrive\Documents\ADMS Final\Data\Presentation"
    db_final = os.path.join(presentation_dir, "BIG TABLE.db")

    try:
        # --- 1. PROCESS JAPAN ---
        conn_j = sqlite3.connect("japan staging area.db")
        
        # We use quote marks around the column names because the CSV had 
        # literal single quotes in the header (e.g., 'product_id')
        query_j = """
            SELECT 
                s."'invoice_id'" as invoice_id, 
                s."'product_id'" as product_id, 
                s."'quantity'" as quantity, 
                s."'date'" as date,
                i.product_name, 
                i.category, 
                i.price 
            FROM japan_sales_data s
            JOIN japan_items i ON s."'product_id'" = i.id
        """
        df_j = pd.read_sql_query(query_j, conn_j)
        
        df_j['total_usd'] = (df_j['price'] * df_j['quantity']) * 0.0065
        df_j['country'] = 'Japan'
        conn_j.close()

        # --- 2. PROCESS MYANMAR ---
        conn_m = sqlite3.connect("myanmar staging area.db")
        # Myanmar also has single quotes in the headers!
        query_m = """
            SELECT 
                s."'invoice_id'" as invoice_id, 
                s."'product_id'" as product_id, 
                s."'quantity'" as quantity, 
                s."'date'" as date,
                i.name as product_name, 
                i.type as category, 
                i.price 
            FROM myanmar_sales_data s
            JOIN myanmar_items i ON s."'product_id'" = i.id
        """
        df_m = pd.read_sql_query(query_m, conn_m)
        
        df_m['total_usd'] = df_m['price'] * df_m['quantity']
        df_m['country'] = 'Myanmar'
        conn_m.close()

        # --- 3. CONSOLIDATE ---
        cols = ['invoice_id', 'product_name', 'category', 'quantity', 'total_usd', 'country', 'date']
        big_table = pd.concat([df_j[cols], df_m[cols]], ignore_index=True)

        # --- 4. LOAD ---
        conn_final = sqlite3.connect(db_final)
        big_table.to_sql("final_global_sales", conn_final, if_exists='replace', index=False)
        
        print(f"[SUCCESS] Big Table created with {len(big_table)} rows at {db_final}")
        conn_final.close()

    except Exception as e:
        print(f"[ERROR] Could not finish Presentation layer: {e}")

if __name__ == "__main__":
    load_presentation()