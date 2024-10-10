import pandas as pd
import time

start_time = time.time()

# will display all columns
pd.set_option('display.max_columns', None)

df = pd.read_parquet('https://raw.githubusercontent.com/gchandra10/filestorage/refs/heads/main/sales_onemillion.parquet')

# Display the first few rows
print(df.head(20))
print(f"\n Took {time.time() - start_time:.4f} seconds")
pd.reset_option('display.max_columns')