import sys
import os
import csv
import pyodbc
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import SERVER_NAME, DATABASE, DRIVER

# Connecting to the Databse
conn = pyodbc.connect(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
        ,autocommit=True
    )
cursor = conn.cursor()

# Creating the Schema
cursor.execute("CREATE SCHEMA ingestion")

# Creating the Tables
## cards_data.csv
cursor.execute("""
    CREATE TABLE ingestion.cards_data (
        id NVARCHAR(10),
        client_id NVARCHAR(10),
        card_brand NVARCHAR(10),
        card_number NVARCHAR(50),
        expires NVARCHAR(10),
        cvv NVARCHAR(10),
        has_chip NVARCHAR(10),
        num_cards_issued NVARCHAR(10),
        credit_limit NVARCHAR(10),
        acct_open_date NVARCHAR(10),
        year_pin_last_changed NVARCHAR(10),
        card_on_dark_web NVARCHAR(10),
        issuer_bank_name NVARCHAR(50),
        issuer_bank_state NVARCHAR(50),
        issuer_bank_type NVARCHAR(20),
        issuer_risk_rating NVARCHAR(20)
        )
    """)

## mcc_data.csv
cursor.execute("""
    CREATE TABLE ingestion.mcc_data (
        code NVARCHAR(10),
        description NVARCHAR(50),
        notes NVARCHAR(50),
        updated_by NVARCHAR(50)
        )
    """)

## transactions_data
cursor.execute("""
    CREATE TABLE ingestion.transactions_data (
        id NVARCHAR(10),
        date NVARCHAR(20),
        client_id NVARCHAR(10),
        card_id NVARCHAR(10),
        amount NVARCHAR(50),
        use_chip NVARCHAR(50),
        merchant_id NVARCHAR(10),
        merchant_city NVARCHAR(50),
        merchant_state NVARCHAR(20),
        zip NVARCHAR(10),
        mcc NVARCHAR(10),
        errors  NVARCHAR(10)
        )
    """)

## users_data
cursor.execute("""
    CREATE TABLE ingestion.users_data (
        id NVARCHAR(10),
        current_age NVARCHAR(10),
        retirement_age NVARCHAR(10),
        birth_year NVARCHAR(10),
        birth_month NVARCHAR(10),
        gender NVARCHAR(10),
        address NVARCHAR(50),
        latitude NVARCHAR(10),
        longitude NVARCHAR(10),
        per_capita_income NVARCHAR(10),
        yearly_income NVARCHAR(10),
        total_debt NVARCHAR(10),
        credit_score NVARCHAR(10),
        num_credit_cards NVARCHAR(10),
        employment_status NVARCHAR(50),
        education_level NVARCHAR(50)
        )
    """)

# Sanity Check
print("Schema Initiated Succefully!")

# Closing the Connection
cursor.close()
conn.close()