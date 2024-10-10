## To demonstrate immediate execution (Non Lazy)

import pandas as pd
import time

# will display all columns
pd.set_option('display.max_columns', None)

def print_timing(start_time, operation):
    print("-"*100)
    print(f"\n{operation} took {time.time() - start_time:.4f} seconds")
    print("-"*100)

print("\nPandas Eager Evaluation:")
start_time = time.time()

# Read CSV file
#df_pandas = pd.read_csv("data/sales_100.csv")

df_pandas = pd.read_parquet('https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/sales_onemillion.parquet')
df_pandas.describe()

print_timing(start_time, "Reading in Pandas")

# Perform operations
print("Executing Pandas operations...")
result_pandas = (
    df_pandas[df_pandas["Region"] == "Sub-Saharan Africa"]
    .groupby("Country")
    .agg({
        "Total Revenue": "sum",
        "Total Profit": "sum",
        "Order ID": "count"
    })
    .rename(columns={"TotalRevenue": "Total Revenue", "TotalProfit": "Total Profit", "Order ID": "Number of Orders"})
    .sort_values("Total Revenue", ascending=False)
)
print(result_pandas)

print_timing(start_time, "Executing Pandas operations")
pd.reset_option('display.max_columns')
