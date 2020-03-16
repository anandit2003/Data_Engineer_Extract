import pandas as pd
import pyarrow
import pyarrow.parquet as pq
csvPath = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\weather20160201.csv"
parquetFilename = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\par_extract_weather20160201.parquet"
df = pd.read_csv(csvPath)
df.to_parquet(parquetFilename)