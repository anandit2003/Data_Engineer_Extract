import pandas as pd
import pyarrow
import pyarrow.parquet as pq
csvPath = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\weather20160301.csv"
parquetFilename = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\par_extract_weather20160301.parquet"
df = pd.read_csv(csvPath)
df.to_parquet(parquetFilename)
