import pandas as pd
import sqlite3

class GenSalesReport:

    @staticmethod
    def read_customer_data(conn):
        return pd.read_sql_query("SELECT * FROM customers", conn)

    @staticmethod
    def read_item_data(conn):
        return pd.read_sql_query("SELECT * FROM items", conn)

    @staticmethod
    def read_orders_data(conn):
        return pd.read_sql_query("SELECT * FROM orders", conn)

    @staticmethod
    def read_sales_data(conn):
        return pd.read_sql_query("SELECT * FROM sales", conn)

def create_report(conn):
    c = conn.cursor()

    # SQL query
    query = '''
            SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) as total_quantity
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            JOIN items i ON o.item_id = i.item_id
            WHERE c.age BETWEEN 18 AND 35
            GROUP BY c.customer_id, c.age, i.item_name
            HAVING total_quantity > 0
            '''

    out = pd.read_sql_query(query, conn)

    # createing a output file 
    out.to_csv('output.csv', sep=';', index=False)
    
def main():
    # conn obj
    conn = sqlite3.connect('sales_data.db')

    
    obj = GenSalesReport()

    # Read data from SQLite database
    customers = obj.read_customer_data(conn)
    items = obj.read_item_data(conn)
    orders = obj.read_orders_data(conn)
    sales = obj.read_sales_data(conn)

    # Generate and print the report
    create_report(conn)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()
