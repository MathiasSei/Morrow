import sqlite3
import app

categories = app.list_categories()

# Get amount per category
def get_amount_per_category():
    conn = sqlite3.connect('transactions.db')
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