import pyodbc
from config import SERVER_NAME, DATABASE, DRIVER

def get_conn(database="master"):
    return pyodbc.connect(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )

# Step 1: Connect to master and create the database
conn = get_conn("master")
conn.autocommit = True
cursor = conn.cursor()

cursor.execute(f"IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{DATABASE}') DROP DATABASE {DATABASE}")
cursor.execute(f"CREATE DATABASE {DATABASE}")
print(f"Database '{DATABASE}' created ✅")

cursor.close()
conn.close()

# Step 2: Verify connection to the new database
conn = get_conn(DATABASE)
print(f"Connected to '{DATABASE}' successfully ✅")
conn.close()

# Method to be call
def get_conn(database=DATABASE):
    return pyodbc.connect(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
        , autocommit=True
    )