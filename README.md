# Accidents Data Analysis using Spark
## Introduction
Accidents are one of the major cause of deaths in the United States. Every year over a million people die due to accidents. The constant increase in the population increased the necessity of having multiple vehicles per family, thereby increasing the chances of accidents. The surge in population and the proportional increase in the self-owned vehicle count is a measure to reckon. In the following project, we take different publicly available data sources in different formats and apply 

## Datasets
1. **[Accidents Dataset](https://www.kaggle.com/sobhanmoosavi/us-accidents)**: This kaggle dataset is the source of Accidents data of US. This data provides the key attributes of accidents with timestamps ranging from 2015 to 2019. The accidents are listed at the grain of street thus have a detailed location information. This dataset is downloaded and uploaded to S3 bucket from where it is read in the form of CSV. This data has a total of _2.25 million_ records.
2. **[Population Dataset](http://www2.census.gov/programs-surveys/popest/datasets/2010-2019/national/totals/nst-est2019-alldata.csv)**: This is a CSV dataset is provided by US Census. The data provides the information like population estimate, immigrant data, birth rate, death rate, etc.,. done by the bureau for the years 2010 to 2019. We extract the necessary data of population data for the required years.
3. **[Vehicles Dataset](https://en.wikipedia.org/wiki/List_of_U.S._states_by_vehicles_per_capita)**: This data is taken from the Wikipedia which gives information of Vehicles per 1000 people per state for the year 2017. We save this data in the form in text for the sake variety of data formats. This data is uploaded to S3 bucket and then read as txt file. This data is saved to Spark dataframe and extrapolated for the years 2016, 2018, 2019 based on [https://www.statista.com/statistics/859950/vehicles-in-operation-by-quarter-united-states/](https://www.statista.com/statistics/859950/vehicles-in-operation-by-quarter-united-states/). We take a ratio of the sales and calculate the vehicles per 1000 per state. 
4. **State Codes**: As everywhere the state data is given in the form of codes, the corresponding mapping between state codes and state names is saved in an S3 bucket and used. 

## Architecture 
**Snowflake Schema**
![Flow diagram](/images/ER.jpeg)

## Implementation 
The data is available in different formats like CSV and text files. We use python Logging to capture important log information.
### Stages
#### 1. Landing - 
The population dataset is available online from a URL as CSV.
The other datasets are manually downloaded and uploaded to S3 bucket. 

#### 2. Staging - 
The population dataset is loaded into a pandas dataframe and then a Spark Dataframe is created. 
The other datasets are loaded from S3 bucket and staging Spark Dataframes are created. 

All the dataframes are loaded without any manipulation.


#### 3. Dimensions - 
**Accidents** - We extract only required columns like 'id','severity','start_time','end_time','description','side','state','weather_condition', 'year'. We apply data quality checks on different columns to eliminate rows with NULL values on key columns. We also select the data where data has Start_Time has year from 2016 to 2019. 
![Sample Data](/images/accidentsd.png)

**Population** - We extract the population estimate for the years 2016-19 and the corresponding states name.
![Sample Data](/images/populationd.png)

**Vehicles** - The data is available for the year 2017 as text and we format using UDF. We take another statistic and take the ratio of vehicle sales for the years 2016-19 and extrapolate the data for the other years. 
![Sample Data](/images/vehicless.png)

**The statistic we consider for extrapolation**
![Sample_Data](/images/stats.png)

**After extrapolation**
![Sample Data](/images/vehiclesd.png)

**State Codes** - State codes are read as text and formatted using UDF. 

*State Codes are used as some of the datasets have State Codes and others have State names. This serves as a mapping.*

**Vehicles_Population** - We join population and vehicles data on state and create this dimension, we get an year wise mapping of population and vehicles.
![Sample Data](/images/veh_pop.png)

#### 4. Facts - 
**Accidents** - As the data in accidents dimension is indiviually reported, we aggregate it at state level and take the count of accidents year wise. ***We have columnar data for population and vehicles as we intend to avoid redundant data.***
![Sample Data](/images/fact.png)

## Analysis 
#### Why this data model?
The implemented data model is appropriate for the analytics proposed in the next section. Implementing a snowflake schema gives more scope for analytics on dimensions that do not involve all the data. The intermediate dimension created could be used for analytics and also helps in easier join of data with the Accidents. This way we ensure the data integrity while dealing with multiple data sources. 

#### Why Spark?
The Accidents dataset has a total 2.25 million records which are then filtered by country set to US and start_time year between 2016 to 2019. 
The Population dataset and vehicles dataset have state wide data for 50 states. 
We use spark for analysis as the joins and any other queries are executed faster because of the distributed computing nature of spark. And with data of this scale, the parallel computing power of Spark is leveraged. As many of the queries also involve grouping, the Map Reduce is effectively utilised.
*Even the data is increased by 100x, bringing in data for future years, Spark would be able handle the computing efficiently. *

#### Why no use of Airflow?
1. The data is historical.
2. There is no data source where we would get the above data periodically in a known format to run pipelines.

Hence we load the static data and use it for analysis. 

#### Parallel Access
Once the data is loaded into the corresponding dataframes, we save them as parquet files and write them to S3 bucket for analysis. This data could then be loaded into Spark Dataframes and be used for analysis. This way, we don’t have an issue with number of users accessing the data.

#### Data Quality Checks
We have multiple data quality checks to ensure we have the data 
1. Is in the correct Date range.
2. Is not having duplicates.
3. Meets required parameter values. (Ex: Country is US)

## Use Cases
The following data model fits well for the following analysis - 
- The rate at which the accidensts are happening with an increase in population. 
- How the number of self-owned vehicles in effecting Accidents rate.
- How the above measures have changed over a period of past three years.
- Accident hotspots and the corresponding population and vehicle rates in the nearest state. 
and more. 

The individual dimensional tables which have been processed and cleaned can also be used for following analytics - 
- Major factors of accidents 
    - Time of day
    - Severity and hotspots
    - Weather condition
    etc.
- Vehicle sales with change in population
and more.


