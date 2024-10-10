## To demonstrate Lazy Evaluation

import polars as pl
import time

def print_timing(start_time, operation):
    print("-"*100)
    print(f"{operation} took {time.time() - start_time:.4f} seconds")
    print("-"*100)
    
print("Polars Lazy Evaluation:")
start_time = time.time()

# Create a lazy DataFrame
# df_lazy = pl.scan_csv("data/sales_100.csv")

df_lazy = pl.scan_parquet('https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/sales_onemillion.parquet')

print_timing(start_time, "Creating lazy DataFrame")

# Define operations lazily
query = (
    df_lazy
    .filter(pl.col("Region") == "Sub-Saharan Africa")
    .group_by("Country")
    .agg([
        pl.sum("Total Revenue").alias("Total Revenue"),
        pl.sum("Total Profit").alias("Total Profit"),
        pl.count("Order ID").alias("Number of Orders")
    ])
    .sort("Total Revenue", descending=True)
)
#print(query)

print_timing(start_time, "Defining lazy operations")

# Execute the query and collect the results
print("Executing Polars query...")
result_polars = query.collect()
print_timing(start_time, "Executing Polars query")

#print("Polars result:")
print(result_polars)
