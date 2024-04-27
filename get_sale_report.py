#!/usr/bin/python

import pandas as pd

class GenSalesReport:

    @staticmethod
    def read_customer_data(filename):
        return pd.read_csv(filename, index_col='customer_id')
   
    @staticmethod
    def read_item_data(filename):
        return pd.read_csv(filename, index_col='item_id')
   
    @staticmethod
    def read_orders_data(filename):
        return pd.read_csv(filename) 

    @staticmethod
    def read_sales_data(filename):
        return pd.read_csv(filename, index_col='sales_id')

obj = GenSalesReport
customers = obj.read_customer_data('input_data/customer.csv')
items = obj.read_item_data('input_data/items.csv')
orders = obj.read_orders_data('input_data/orders.csv')
sales = obj.read_sales_data('input_data/sales.csv')

def main():

    # Creating dataframes
    sales_orders = sales.merge(orders, left_index=True, right_on='sales_id')
    sales_orders_items = sales_orders.merge(items, left_on='item_id', right_index=True)
    sales_orders_items_customers = sales_orders_items.merge(customers, left_on='customer_id', right_index=True)

    # customers age between 18-35
    filtered_customers = sales_orders_items_customers[(sales_orders_items_customers['age'] >= 18) & (sales_orders_items_customers['age'] <= 35)]

    grouped_data = filtered_customers.groupby(['customer_id', 'age', 'item_name']).agg({'quantity': 'sum'}).reset_index()

    grouped_data = grouped_data[grouped_data['quantity'] > 0]

    # createing a output file 
    grouped_data.to_csv('output/sample_output.csv', sep=';', index=False)

if __name__ == "__main__":
    main()
