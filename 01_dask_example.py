import dask.dataframe as dd
import time

def print_timing(start_time, operation):
    print("-" * 100)
    print(f"{operation} took {time.time() - start_time:.4f} seconds")
    print("-" * 100)

print("Dask Lazy Evaluation:")
start_time = time.time()

# Create a lazy DataFrame
#df_lazy = dd.read_csv("data/sales_100.csv")

df_lazy = dd.read_parquet('https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/sales_onemillion.parquet')

print_timing(start_time, "Creating lazy DataFrame")

# Define operations lazily
query = (
    df_lazy[df_lazy["Region"] == "Sub-Saharan Africa"]
    .groupby("Country")
    .agg({
        "Total Revenue": "sum",
        "Total Profit": "sum",
        "Order ID": "count"
    })
    .rename(columns={
        "TotalRevenue": "Total Revenue",
        "TotalProfit": "Total Profit",
        "Order ID": "Number of Orders"
    })
    .sort_values("Total Revenue", ascending=False)
)

print_timing(start_time, "Defining lazy operations")

# Execute the query and collect the results
print("Executing Dask query...")
result_dask = query.compute()
print_timing(start_time, "Executing Dask query")

print("Dask result:")
print(result_dask)
