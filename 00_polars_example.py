import polars as pl
import time

start_time = time.time()
df = pl.scan_parquet('https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/sales_onemillion.parquet')

# Head doesnt display the data.
# print(df.head())
df.limit(20)

print(df.collect())
print(f"\n Took {time.time() - start_time:.4f} seconds")
