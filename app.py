import pandas as pd
import sqlite3

# Path to transatcion CSV file
transaction_csv_path = "transactions.csv"

df = pd.read_csv(
    transaction_csv_path,
    usecols=["Transaksjonsdato", "Beskrivelse", "Beløp"],
    index_col=False
)

# Only sets Transaksjonstato to the first 10 characters
df["Transaksjonsdato"] = df["Transaksjonsdato"].astype(str).str[:10]
# Converts to date
df["Transaksjonsdato"] = pd.to_datetime(df["Transaksjonsdato"], dayfirst=True, errors="coerce")

print(df.head(15))

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
# Delete the transactions table if it exists
conn.execute("DROP TABLE IF EXISTS transactions")
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

# -------------- Handling transactions --------------
def transaction_handling():
    for index, row in df.iterrows():
        # Gets the transaction date, description, and amount
        transaction_date = row["Transaksjonsdato"]
        description = row["Beskrivelse"]
        amount = row["Beløp"]

        # Check if description matches keywords in categoryKeywords
        cursor = conn.execute("""
        SELECT Category FROM categoryKeywords
        WHERE Keyword IN (?)
        """, (description,))
        category = cursor.fetchone()
        if category:
            category = category[0]
        else:
            # If no match, assign a default category
            category = "Uncategorized"

def add_category(category_name):
    # Check if the category already exists
    cursor = conn.execute("SELECT Category FROM categories WHERE Category = ?", (category_name,))
    if cursor.fetchone() is not None:
        print(f"Category '{category_name}' already exists.")
        return
    """ Adds a new category to the categories table """
    conn.execute("INSERT INTO categories (Category) VALUES (?)", (category_name,))
    conn.commit()

def list_categories():
    """ Lists all categories in the categories table """
    cursor = conn.execute("SELECT Category FROM categories")
    categories = cursor.fetchall()
    for index, (category,) in enumerate(categories):
        print(f"{index + 1}. {category}")

# Go trough all transactions and insert them into the database
for index, row in df.iterrows():
    print(row)
    if index % 100 == 0:
        print(f"Processing transaction {index + 1} of {len(df)}")
    transaction_date = row["Transaksjonsdato"]
    print(transaction_date)
    # if transaction_date is only date, add time as 00:00:00
    if isinstance(transaction_date, pd.Timestamp):
        transaction_date = transaction_date.replace(hour=0, minute=0, second=0)
    description = row["Beskrivelse"]
    amount = row["Beløp"]

    # Convert pandas Timestamp to string (ISO format)
    if pd.notnull(transaction_date):
        transaction_date_str = transaction_date.strftime("%Y-%m-%d %H:%M:%S")
    else:
        transaction_date_str = None

    # Insert the transaction into the database
    conn.execute("""
    INSERT INTO transactions (Transactiondate, Description, Amount, Category)
    VALUES (?, ?, ?, ?)
    """, (transaction_date_str, description, amount, "Uncategorized"))
    conn.commit()
# Close the database connection
conn.close()