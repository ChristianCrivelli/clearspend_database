import sys
import os
import csv
import pyodbc
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import SERVER_NAME, DATABASE, DRIVER

data_source_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data-source'))

# Connecting to the Database
conn = pyodbc.connect(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
        ,autocommit=True
    )
cursor = conn.cursor()

def push_csv_to_db(file_name, table_name, columns):
    file_path = os.path.join(data_source_dir, file_name)
    
    if not os.path.exists(file_path):
        print(f"Error: {file_name} not found in {data_source_dir}")
        return
    
    # Enable fast_executemany for better performance with large datasets
    cursor.fast_executemany = True

    print(f"Starting ingestion for {table_name}...")

    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip the header row
        
        # Prepare the INSERT statement
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO ingestion.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Batch insert for efficiency
        batch_size = 1000
        batch = []
        
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(sql, batch)
                batch = []
        
        # Insert remaining rows
        if batch:
            cursor.executemany(sql, batch)

    conn.commit()
    print(f"Successfully pushed {file_name} to {table_name} ✅")

if __name__ == "__main__":
    # 1. Push Cards Data
    push_csv_to_db(
        "cards_data.csv", 
        "cards_data", 
        ["id", "client_id", "card_brand", "card_type", "card_number", "expires", "cvv", "has_chip", 
         "num_cards_issued", "credit_limit", "acct_open_date", "year_pin_last_changed", 
         "card_on_dark_web", "issuer_bank_name", "issuer_bank_state", "issuer_bank_type", "issuer_risk_rating"]
    )

    # 2. Push MCC Data
    push_csv_to_db(
        "mcc_data.csv", 
        "mcc_data", 
        ["code", "description", "notes", "updated_by"]
    )

    # 3. Push Users Data
    push_csv_to_db(
        "users_data.csv", 
        "users_data", 
        ["id", "current_age", "retirement_age", "birth_year", "birth_month", "gender", 
         "address", "latitude", "longitude", "per_capita_income", "yearly_income", 
         "total_debt", "credit_score", "num_credit_cards", "employment_status", "education_level"]
    )

    # 4. Push Transactions Data (Large File)
    push_csv_to_db(
        "transactions_data.csv", 
        "transactions_data", 
        ["id", "date", "client_id", "card_id", "amount", "use_chip", 
         "merchant_id", "merchant_city", "merchant_state", "zip", "mcc", "errors"]
    )

    cursor.close()
    conn.close()