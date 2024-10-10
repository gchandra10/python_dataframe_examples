import dask.dataframe as dd
import time

start_time = time.time()
df = dd.read_parquet('https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/sales_onemillion.parquet')

# Compute and display the first few rows
print(df.head(20))
print(f"\n Took {time.time() - start_time:.4f} seconds")
