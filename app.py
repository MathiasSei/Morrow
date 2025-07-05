import pandas as pd
import sqlite3

# Path to transatcion CSV file
transaction_csv_path = "transactions.csv"

df = pd.read_csv(
    transaction_csv_path,
    usecols=["Transaksjonsdato", "Beskrivelse", "Beløp"],
    index_col=False
)

# Split Transaksjonsdato into date and time and replace Tra
df["Transaksjonsdato"] = pd.to_datetime(df["Transaksjonsdato"], dayfirst=True, errors="coerce")

print(df.head())

# -------------- SQL Database Setup --------------
# Sets up sqlite database connection
conn = sqlite3.connect("transactions.db")

""" Database schema
Table: transactions
Columns: TransactionID, Transactiondate, Description, Amount, Category

Table : categories
Columns: Category
Foreign key: Category in transactions references Category in categories

Table: categoryKeywords
Columns: Category, Keyword
Foreign key: Category in categoryKeywords references Category in categories
"""
# Create tables if they do not exist
conn.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    TransactionID INTEGER PRIMARY KEY AUTOINCREMENT,
    Transactiondate DATETIME NOT NULL,
    Description TEXT NOT NULL,
    Amount REAL NOT NULL,
    Category TEXT,
    FOREIGN KEY (Category) REFERENCES categories(Category)
)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS categories (
    Category TEXT PRIMARY KEY)
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS categoryKeywords (
    Category TEXT,
    Keyword TEXT,
    FOREIGN KEY (Category) REFERENCES categories(Category),
    PRIMARY KEY (Category, Keyword))
""")