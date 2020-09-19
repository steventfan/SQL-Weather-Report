# Weather Locations and Recordings Project

Find and sort the US states weather stations from least to greatest difference between the precipitation of the highest highest average month and the lowest average month.

**Note:** Actual weather year data files have been truncated due to their large file sizes

## To Run Program:
```./run.sh <locations directory> <recordings directory> <output directory>```

## Assumptions:
All averages are calculated per month, regardless of the year.

Locations with CTRY as "US" but with STATE as "" are excluded.

Assume matching of locations and recordings only relies on the USAF and STN--- value. WBAN is not utilized.

All PRCP values with valid data are appended with a letter between A-I. PRCP values without the necessary letter are excluded.

PRCP values are calculated based on the numeric value provided multiplied by a factor. The factor is based on the letter in the attribute: A:4 B:2 C:4/3 D:1 E:2 F:1 G:1 H:0 I:0.

## Overall Description:
I divided the problem into extracting and parsing the locations and recordings datasets, combining the two datasets into a table, querying the table, sorting the results from the query, and outputting the results.

For the extracting and parsing of the datasets, I extracted the data line by line and saved only the necessary attributes needed for the specifications of the project. This is to limit the memory size of the datasets. The datasets are then converted to PySpark dataframes to allow database functionality and to eliminate unnecessary or redundant operations.

For the combining of the two datasets into a table, the operation is rather trivial. I used PySpark join to combine the two datasets based on matching USAF and STN values from the locations and the recordings dataset respectively into one PySpark dataframe. Thus, queries can be run on a single PySpark dataframe.

For querying the table, for each month in a year, I queried using PySpark SQL. The parameters for the SQL query is: WHERE table.MONTH == currentMonthIteration, GROUPBY table.STATE, and AVG table.PRCP. The averages for the current month of the states are then compared to the current maximum and minimum average months for each state respectively. The months and difference are then updated accordingly. The process is repeated for each subsequent month.

For sorting the results, the states are simply sorted based on their difference value.

For outputting the results, the results are simply output into a file.

## Total Runtime:
163.58202266693115 seconds
