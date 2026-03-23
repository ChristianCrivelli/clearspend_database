import sys
import os
import csv
import pyodbc
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import SERVER_NAME, DATABASE, DRIVER

# Connecting to the Database
conn = pyodbc.connect(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
        ,autocommit=True
    )
cursor = conn.cursor()

# Creating the Schema
cursor.execute("CREATE SCHEMA transformation")

# Creating the Tables
## cards_data.csv
cursor.execute("""
    CREATE TABLE transformation.cards_data (
        id INT,
        client_id INT,
        card_brand VARCHAR(15),
        card_type VARCHAR(6),
        card_number NVARCHAR(20),
        expires DATE,
        cvv INT,
        has_chip BIT,
        num_cards_issued INT,
        credit_limit INT, 
        acct_open_date DATE,
        year_pin_last_changed INT,
        card_on_dark_web BIT,
        issuer_bank_name VARCHAR(20),
        issuer_bank_state VARCHAR(5),
        issuer_bank_type VARCHAR(15),
        issuer_risk_rating VARCHAR(15)
        )
    """)

## mcc_data.csv


## transactions_data


## users_data


# Sanity Check
print("Schema Initiated Succefully!")

# Closing the Connection
cursor.close()
conn.close()
