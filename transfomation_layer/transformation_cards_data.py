import sys
import os
import pyodbc
import pandas as pd
import datetime
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

# Cleaning the Data
df = pd.read_sql_query(sql="SELECT * FROM ingestion.cards_data", con=conn)

## id
df = df.drop_duplicates(subset=["id"], keep = "last")

print("Column id done!") #sanity check

## customer_id
print("Column customer_id done!") #sanity check

## card_brand
df["card_brand"] = df['card_brand'].str.strip()

df["card_brand"] = df["card_brand"].replace("VVisa", "Visa")
df["card_brand"] = df["card_brand"].replace("V", "Visa")
df["card_brand"] = df["card_brand"].replace("V!sa", "Visa")
df["card_brand"] = df["card_brand"].replace("visa-card", "Visa")
df["card_brand"] = df["card_brand"].replace("Vissa", "Visa")
df["card_brand"] = df["card_brand"].replace("VVisa", "Visa")
df["card_brand"] = df["card_brand"].replace("Vis", "Visa")

df["card_brand"] = df["card_brand"].replace("Ame x", "Amex")
df["card_brand"] = df["card_brand"].replace("Ame  x", "Amex")

df["card_brand"] = df["card_brand"].replace("Master Card", "Mastercard")
df["card_brand"] = df["card_brand"].replace("Master Card ", "Mastercard")
df["card_brand"] = df["card_brand"].replace("Master  Card", "Mastercard")

df["card_brand"] = df["card_brand"].replace("Dis cover", "Discover")
df["card_brand"] = df["card_brand"].replace("Dis  cover", "Discover")

df["card_brand"] = df["card_brand"].replace("unknown", None)
df["card_brand"] = df["card_brand"].replace("", None)
df["card_brand"] = df["card_brand"].fillna(None)

print("Column card_brand done!") #sanity check

## card_type
df["card_type"] = df['card_type'].str.strip()

df["card_type"] = df["card_type"].replace("Card - Credit", "Credit")
df["card_type"] = df["card_type"].replace("CR", "Credit")
df["card_type"] = df["card_type"].replace("CC", "Credit")
df["card_type"] = df["card_type"].replace("Cedit", "Credit")
df["card_type"] = df["card_type"].replace("Crdeit", "Credit")
df["card_type"] = df["card_type"].replace("Credt", "Credit")
df["card_type"] = df["card_type"].replace("CRED", "Credit")
df["card_type"] = df["card_type"].replace("Credit Card", "Credit")
df["card_type"] = df["card_type"].replace("CRED", "Credit")
df["card_type"] = df["card_type"].replace("Cre dit", "Credit")

df["card_type"] = df["card_type"].replace("Debit (Prepaid)", "Debit")
df["card_type"] = df["card_type"].replace("Debiit", "Debit")
df["card_type"] = df["card_type"].replace("DP", "Debit")
df["card_type"] = df["card_type"].replace("Prepaid", "Debit")
df["card_type"] = df["card_type"].replace("De bit", "Debit")
df["card_type"] = df["card_type"].replace("Prepaid Debit", "Debit")
df["card_type"] = df["card_type"].replace("Debti (Prepaid)", "Debit")
df["card_type"] = df["card_type"].replace("DB", "Debit")
df["card_type"] = df["card_type"].replace("D", "Debit")
df["card_type"] = df["card_type"].replace("DPP", "Debit")
df["card_type"] = df["card_type"].replace("DB-PP", "Debit")
df["card_type"] = df["card_type"].replace("Debit Card", "Debit")
df["card_type"] = df["card_type"].replace("DEB", "Debit")
df["card_type"] = df["card_type"].replace("Bank Debit", "Debit")
df["card_type"] = df["card_type"].replace("Debit Prepaid", "Debit")
df["card_type"] = df["card_type"].replace("PPD", "Debit")
df["card_type"] = df["card_type"].replace("Debit (Prepaid) Card", "Debit")
df["card_type"] = df["card_type"].replace("debit (prepaid)", "Debit")
df["card_type"] = df["card_type"].replace("Debit  (Prepaid)", "Debit")
df["card_type"] = df["card_type"].replace("Debit (Prepiad)", "Debit")
df["card_type"] = df["card_type"].replace("Debit(Prepaid)", "Debit")
df["card_type"] = df["card_type"].replace("Debit (Pre payed)", "Debit")
df["card_type"] = df["card_type"].replace("Debti", "Debit")

df["card_type"] = df["card_type"].replace("unknown", None)
df["card_type"] = df["card_type"].replace("", None)
df["card_type"] = df["card_type"].fillna(None)

print("Column card_type done!") #sanity check

## card_number
df["card_number"] = pd.to_numeric(df["card_number"], errors='coerce').round().astype('Int64')

print("Column card_number done!") #sanity check

## expires
df['expires'] = pd.to_datetime(df['expires'], format='%b-%y')

print("Column expires done!") #sanity check

## cvv

print("Column cvv done!") #sanity check

## has_chip
df["has_chip"] = df["has_chip"].replace("YES", 1)
df["has_chip"] = df["has_chip"].replace("NO", 0)

print("Column has_chip done!") #sanity check

## num_cards_issued
print("Column num_cards_issued done!") #sanity check

## credit_limit
df["credit_limit"] = df['credit_limit'].str.strip()

df['credit_limit'] = df['credit_limit'].apply(
    lambda x: str(x).replace('.', '') if 'k' not in str(x).lower() else x
) # remove a "." if there are no "k"s

df['credit_limit'] = pd.to_numeric(
    df['credit_limit']
    .str.lower()
    .str.replace('$', '', regex=False)
    .str.replace(',', '', regex=False)
    .str.replace('-', '', regex=False)
    .str.replace('k', 'e3', regex=False),
    errors='coerce'
).astype('Int64') # clean for "k"s, "$"s and ","

df["credit_limit"] = df["credit_limit"].replace("ten thousand", 10000)
df["credit_limit"] = df["credit_limit"].replace("error-value", None)
df["credit_limit"] = df["credit_limit"].replace("limit_unknown", None)

print("Column credit_limit done!") #sanity check

## acct_open_date
df["acct_open_date"] = pd.to_datetime(df["acct_open_date"], errors='coerce', format='mixed')

now = datetime.datetime.now()
df.loc[df['acct_open_date'] > now, 'acct_open_date'] = pd.NaT
df.loc[df['acct_open_date'] < pd.Timestamp('1900-01-01'), 'acct_open_date'] = pd.NaT

df["acct_open_date"] = df["acct_open_date"].replace("not available", None)

print("Column acct_open_date done!") #sanity check

## year_pin_last_changed

print("Column year_pin_last_changed done!") #sanity check

## card_on_dark_web
df["card_on_dark_web"] = df["card_on_dark_web"].replace("Yes", 1)
df["card_on_dark_web"] = df["card_on_dark_web"].replace("No", 0)

print("Column card_on_dark_web done!") #sanity check

## issuer_bank_name
df["issuer_bank_name"] = df['issuer_bank_name'].str.strip()

df["issuer_bank_name"] = df['issuer_bank_name'].replace("Bk of America", "Bank of America")
df["issuer_bank_name"] = df['issuer_bank_name'].replace("U.S. Bank", "Bank of America")
df["issuer_bank_name"] = df['issuer_bank_name'].replace("U.S. Bk", "Bank of America")
df["issuer_bank_name"] = df['issuer_bank_name'].replace("U.S. Bank", "Bank of America")
df["issuer_bank_name"] = df['issuer_bank_name'].replace("u.s. bank", "Bank of America")

df["issuer_bank_name"] = df['issuer_bank_name'].replace("Chase Bk", "JPMorgan Chase")
df["issuer_bank_name"] = df['issuer_bank_name'].replace("Chase Bank", "JPMorgan Chase")
df["issuer_bank_name"] = df['issuer_bank_name'].replace("JP Morgan Chase", "JPMorgan Chase")
df["issuer_bank_name"] = df['issuer_bank_name'].replace("CHASE BANK", "JPMorgan Chase")

df["issuer_bank_name"] = df['issuer_bank_name'].replace("Discover Bk", "Discover Bank")

df["issuer_bank_name"] = df['issuer_bank_name'].replace("PNC Bk", "PNC Bank")

df["issuer_bank_name"] = df['issuer_bank_name'].replace("Citi", "Citibank")
df["issuer_bank_name"] = df['issuer_bank_name'].replace("citi", "Citibank")

df["issuer_bank_name"] = df['issuer_bank_name'].replace("Ally Bk", "Ally Bank")


print("Column issuer_bank_name done!") #sanity check

## issuer_bank_state
df["issuer_bank_state"] = df['issuer_bank_state'].str.strip()

df["issuer_bank_state"] = df["issuer_bank_state"].replace("Illinois", "IL")
df["issuer_bank_state"] = df["issuer_bank_state"].replace("Pennsylvania", "PA")
df["issuer_bank_state"] = df["issuer_bank_state"].replace("Virginia", "VA")
df["issuer_bank_state"] = df["issuer_bank_state"].replace("New York", "NY")
df["issuer_bank_state"] = df["issuer_bank_state"].replace("California", "CA")
df["issuer_bank_state"] = df["issuer_bank_state"].replace("Minnesota", "MN")
df["issuer_bank_state"] = df["issuer_bank_state"].replace("Michigan", "MI")
df["issuer_bank_state"] = df["issuer_bank_state"].replace("North Carolina", "NC")

print("Column issuer_bank_state done!") #sanity check

## issuer_bank_type
df["issuer_bank_type"] = df['issuer_bank_type'].str.strip()

df["issuer_bank_type"] = df["issuer_bank_type"].replace("Online Only", "Online")
df["issuer_bank_type"] = df["issuer_bank_type"].replace("Online Bank", "Online")

df["issuer_bank_type"] = df["issuer_bank_type"].replace("Regional Bank", "Regional")

df["issuer_bank_type"] = df["issuer_bank_type"].replace("National Bank", "National")

print("Column issuer_bank_type done!") #sanity check

## issuer_risk_rating
df["issuer_risk_rating"] = df['issuer_risk_rating'].str.strip()

df["issuer_risk_rating"] = df["issuer_risk_rating"].replace("Med", "Medium")
df["issuer_risk_rating"] = df["issuer_risk_rating"].replace("MEDIUM", "Medium")

df["issuer_risk_rating"] = df["issuer_risk_rating"].replace("Low Risk", "Low")

print("Column issuer_risk_rating done!") #sanity check

# Logic to send the data back
columns = ", ".join(df.columns)
values_placeholders = ", ".join(["?"] * len(df.columns))
insert_sql = f"INSERT INTO transformation.cards_data ({columns}) VALUES ({values_placeholders})"

cursor.fast_executemany = True

# Convert Pandas NA/NAType objects to standard Python None for pyodbc compatibility
df_to_insert = df.astype(object).where(pd.notnull(df), None)

params = [tuple(x) for x in df_to_insert.values]
try:
    cursor.executemany(insert_sql, params)
    conn.commit()
    print(f"Successfully inserted {len(df)} rows.")
except Exception as e:
    print(f"An error occurred: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()