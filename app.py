import os
import pandas as pd
import sqlite3

# -------------- SQL Database Setup --------------

def setup_database():
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
        Transactiondate DATE NOT NULL,
        Description TEXT NOT NULL,
        Amount REAL NOT NULL,
        Type TEXT,
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

def clear_terminal():
    """ Clears the terminal screen """
    os.system('cls' if os.name == 'nt' else 'clear')

# -------------- Handling categories --------------
def get_transaction_category(description):
    """ Returns the category of a transaction based on its description """
    cursor = conn.execute("""
        SELECT Category FROM categoryKeywords
        WHERE LOWER(?) LIKE '%' || LOWER(Keyword) || '%'
        ORDER BY LENGTH(Keyword) DESC
        LIMIT 1
    """, (description,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return None

def set_transaction_category(description):
    """ Sets the category of a transaction based on its description """
    category = get_transaction_category(description)
    if category:
        return category
    else:
        # If no category is found, ask to add a new category
        category_list = list_categories()
        category_name = input(f"No category found for '{description}'. Please select category or add new one: ")
        # Check if input is number
        if category_name.isdigit() and int(category_name) <= len(category_list):
            category_name = category_list[int(category_name) - 1]
        elif category_name.isalpha():
            if category_name not in category_list:
                # If not already exists treat it as a new category
                print(f"Adding new category: {category_name}")
                add_category(category_name)
        else:
            # If empty return "Uncategorized"
            return "Uncategorized"
        # Add the keyword to the category
        add_keyword_to_category(category_name, description)
        return category_name

def add_category(category_name):
    # Check if the category already exists
    cursor = conn.execute("SELECT Category FROM categories WHERE Category = ?", (category_name,))
    if cursor.fetchone() is not None:
        print(f"Category '{category_name}' already exists.")
        return
    """ Adds a new category to the categories table """
    conn.execute("INSERT INTO categories (Category) VALUES (?)", (category_name,))
    conn.commit()
    return category_name

def add_keyword_to_category(category_name, keyword):
    """ Adds a keyword to a category in the categoryKeywords table """  
    # Check if the keyword already exists for the category
    cursor = conn.execute("SELECT Keyword FROM categoryKeywords WHERE Category = ? AND Keyword = ?", (category_name, keyword))
    if cursor.fetchone() is not None:
        return
    
    # Insert the new keyword into the categoryKeywords table
    conn.execute("INSERT INTO categoryKeywords (Category, Keyword) VALUES (?, ?)", (category_name, keyword))
    conn.commit()
    print(f"Keyword '{keyword}' added to category '{category_name}'.")

def list_categories():
    """ Lists all categories in the categories table and return list of categories """
    cursor = conn.execute("SELECT Category FROM categories")
    categories = cursor.fetchall()
    if not categories:
        print("No categories found.")
        return []
    
    print("Categories:")
    for index, category in enumerate(categories):
        print(f"{index + 1}. {category[0]}")
    
    return [category[0] for category in categories]

def print_google_search_link(query):
    import urllib.parse
    base_url = "https://www.google.com/search?q="
    search_url = base_url + urllib.parse.quote_plus(query)
    print(search_url)  # This will be clickable in most terminals

# --------- Handle transactions and insert them into the database ---------
def handle_transactions():
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

    setup_database()  # Setup the database

    
    
    # Go trough all transactions and insert them into the database
    for index, row in df.iterrows():
        clear_terminal()
        transaction_date = str(row["Transaksjonsdato"])
        description = row["Beskrivelse"]
        amount = row["Beløp"]
        print(f"Processing transaction {index + 1} of {len(df)}")
        print(f"Transaction date: {transaction_date}, Description: {description}, Amount: {amount}")
        print_google_search_link(description)

        # Set transaction_type
        if amount < 0:
            transaction_type = "Expense"
            amount = abs(amount)  # Store amount as positive for expenses
            # Set the category for the transaction
            category = set_transaction_category(description)
        elif amount > 0:
            category = "InnPåKonto"
            if "Innbetaling" in description:
                transaction_type = "Innbetaling"
            elif "Bonus" in description:
                transaction_type = "Bonus"
            else:
                # # Ask if the transaction is an Innbetaling or return
                # while True:
                #     transaction_type = input(f"Is the transaction '{description}' an Innbetaling or return? (I/R): ").strip().upper()
                #     if transaction_type in ["I", "R"]:
                #         transaction_type = "Income" if transaction_type == "I" else "Return"
                #         break
                #     else:
                #         print("Invalid input. Please enter 'I' for Innbetaling or 'R' for return.")
                transaction_type = "Return"
        else:
            transaction_type = "Unknown"
    
        # Insert the transaction into the database
        conn.execute("""
        INSERT INTO transactions (Transactiondate, Description, Amount, Type, Category)
        VALUES (?, ?, ?, ?, ?)
        """, (transaction_date, description, amount, transaction_type, category))
        conn.commit()
    # Close the database connection
    conn.close()

if __name__ == "__main__":
    conn = sqlite3.connect("transactions.db")
    handle_transactions()
    conn.close()