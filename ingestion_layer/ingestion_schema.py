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
        id NVARCHAR(100),
        client_id NVARCHAR(100),
        card_brand NVARCHAR(100),
        card_type NVARCHAR(100),
        card_number NVARCHAR(100),
        expires NVARCHAR(100),
        cvv NVARCHAR(100),
        has_chip NVARCHAR(100),
        num_cards_issued NVARCHAR(100),
        credit_limit NVARCHAR(100),
        acct_open_date NVARCHAR(100),
        year_pin_last_changed NVARCHAR(100),
        card_on_dark_web NVARCHAR(100),
        issuer_bank_name NVARCHAR(500),
        issuer_bank_state NVARCHAR(500),
        issuer_bank_type NVARCHAR(500),
        issuer_risk_rating NVARCHAR(500)
        )
    """)

## mcc_data.csv
cursor.execute("""
    CREATE TABLE ingestion.mcc_data (
        code NVARCHAR(100),
        description NVARCHAR(500),
        notes NVARCHAR(500),
        updated_by NVARCHAR(500)
        )
    """)

## transactions_data
cursor.execute("""
    CREATE TABLE ingestion.transactions_data (
        id NVARCHAR(100),
        date NVARCHAR(100),
        client_id NVARCHAR(100),
        card_id NVARCHAR(100),
        amount NVARCHAR(100),
        use_chip NVARCHAR(100),
        merchant_id NVARCHAR(100),
        merchant_city NVARCHAR(500),
        merchant_state NVARCHAR(100),
        zip NVARCHAR(100),
        mcc NVARCHAR(100),
        errors NVARCHAR(100)
        )
    """)

## users_data
cursor.execute("""
    CREATE TABLE ingestion.users_data (
        id NVARCHAR(100),
        current_age NVARCHAR(100),
        retirement_age NVARCHAR(100),
        birth_year NVARCHAR(100),
        birth_month NVARCHAR(100),
        gender NVARCHAR(100),
        address NVARCHAR(500),
        latitude NVARCHAR(100),
        longitude NVARCHAR(100),
        per_capita_income NVARCHAR(100),
        yearly_income NVARCHAR(100),
        total_debt NVARCHAR(100),
        credit_score NVARCHAR(100),
        num_credit_cards NVARCHAR(100),
        employment_status NVARCHAR(500),
        education_level NVARCHAR(500)
        )
    """)

# Sanity Check
print("Schema Initiated Succefully!")

# Closing the Connection
cursor.close()
conn.close()