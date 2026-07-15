import sys 
import pandas as pd 

print("arguments",  sys.argv)

month = int(sys.argv[1])

print(f"pipeline initialized, Month: {month}")

df = pd.DataFrame({"day": [1, 2], "number of passengers": [3, 4]})
df['month'] = month
df.to_parquet(f"output_{month}.parquet", index=False)

print(df.head())