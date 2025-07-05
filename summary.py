import sqlite3
conn = sqlite3.connect('transactions.db')

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

categories = list_categories()

# Get amount per category
def get_amount_per_category():
    cursor = conn.cursor()
    
    # Create a dictionary to hold the total amount per category
    category_totals = {}
    
    for category in categories:
        cursor.execute("SELECT SUM(Amount) FROM transactions WHERE Category = ?", (category,))
        total = cursor.fetchone()[0] or 0  # Use 0 if no transactions found
        category_totals[category] = total
    
    conn.close()
    return category_totals

print("Amount per category:")
for category, total in get_amount_per_category().items():
    print(f"{category}: {total:.2f} NOK")