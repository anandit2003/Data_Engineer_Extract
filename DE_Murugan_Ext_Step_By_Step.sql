/*


The instructions in this file will help to find the hottest day in a region based on the given files.

1)	Check the given csv files [weather.20160201.csv, weather.20160301.csv] and make sure the required columns is filled in with a value.
2)	If you find any empty values for the required column can do data munging if necessary.
3)	Convert the given csv files into parquet files.
4)	Create the parquet files into day partition for future checks & quires.


---To find the following--

Convert the weather data into parquet format. Set the raw group to appropriate value you see fit for this data.
The converted data should be query able to answer the following question.

- Which date was the hottest day?
- What was the temperature on that day?
- In which region was the hottest day?

Please provide the source code, tests, documentations and any assumptions you have made.
Note: We are looking for the candidate’s “Data Engineering” ability not just the Python programming skills.
The weather data is provided separately

*/


--Basic Software Prerequisites--

1)	Install Python for Windows
2)	pip install virtualenv
3)	pip install jupyter notebook


---Start Jupyter Notebook---

After successfully installation of the above software’s

Start the Jupyter notebook

Web will open based on your setup like [http://localhost:8888/tree/ext_test]


Run the bellow comments in the Jupyter note.


--***********************************************************************************************************--



/*
--##########################--
---Convert CSV into Parquet--- 
--##########################--
*/






--#################################--
--## For file weather20160201.csv##--
--#################################--



import pandas as pd
import pyarrow
import pyarrow.parquet as pq
csvPath = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\weather20160201.csv"
parquetFilename = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\par_extract_weather20160201.parquet"
df = pd.read_csv(csvPath)
df.to_parquet(parquetFilename)


--##Testing##--

1) 
import pyarrow.parquet as pq
parquetFilename = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\par_extract_weather20160201.parquet"
data = pq.read_pandas(parquetFilename, columns=['ObservationDate', 'ScreenTemperature','Region']).to_pandas()

2) data.head(5) --Make sure you get the first few rows for feb 2016
3) data.tail(5) --Make sure you get the last few rows for feb 2016

--##Find the hottest day in the month##--
4) data['ObservationDate'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  = 2016-02-21
	
	
--##Find the hottest temperature##--
data['ScreenTemperature'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  =  15.6
	

--##Find the hottest region##--
data['Region'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  =  South West England
	
	
--***********************************************************************************************************--



--#################################--
--## For file weather20160301.csv##--
--#################################--



import pandas as pd
import pyarrow
import pyarrow.parquet as pq
csvPath = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\weather20160301.csv"
parquetFilename = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\par_extract_weather20160301.parquet"
df = pd.read_csv(csvPath)
df.to_parquet(parquetFilename)


--##Testing##--

1) 
import pyarrow.parquet as pq
parquetFilename = "D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract\par_extract_weather20160301.parquet"
data = pq.read_pandas(parquetFilename, columns=['ObservationDate', 'ScreenTemperature','Region']).to_pandas()

2) data.head(5) --Make sure you get the first few rows for mar 2016
3) data.tail(5) --Make sure you get the last few rows for mar 2016

--##Find the hottest day in the month##--
4) data['ObservationDate'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  = 2016-03-17T00:00:00
	
	
--##Find the hottest temperature##--
data['ScreenTemperature'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  =  15.8
	

--##Find the hottest region##--
data['Region'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  =  Highland & Eilean Siar




		
	
--***********************************************************************************************************--	


	
--#################################--
--## Combine all parquet files   ##--
--#################################--


--##Testing##--

import os
import glob
import pandas as pq

path =r'D:\Keyrus_Scripts\Local_Oracle_Scripts\Data_Engineer_Extract'
filenames = glob.glob(path + "/*.parquet")
dfs = []
for filename in filenames:
    dfs.append(pq.read_parquet(filename))
data = pq.concat(dfs, ignore_index=True)




2) data.head(5) --Make sure you get the first few rows for feb 2016
3) data.tail(5) --Make sure you get the last few rows for mar 2016

--##Find the hottest day in the month##--
4) data['ObservationDate'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  = 2016-03-17T00:00:00
	
	
--##Find the hottest temperature##--
data['ScreenTemperature'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  =  15.8
	

--##Find the hottest region##--
data['Region'] [data.ScreenTemperature >= data['ScreenTemperature'].max()]
	--## OutPut  =  Highland & Eilean Siar
	

--***********************************************************************************************************--	

	
--#################################--
--## Finding				     ##--
--#################################--



Based on the give data the findings are 

-- Which date was the hottest day?
Hottest Day = 2016-03-17T00:00:00

-- What was the temperature on that day?
Hottest Temperature  = 15.8

-- In which region was the hottest day?
Hottest Region = Highland & Eilean Siar
