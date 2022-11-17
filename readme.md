# Udacity capstone project for data engineering 

I would like to find some relationships between the weather and taxi rides in New York. Therefore, I found two datasets:
* [The trip record data for the yellow cabs in NY](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
* [Hourly weather data as measured at Laguardia Airport in NY](https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/2021-01-01)

The goal is to be able to run an analytical query that can tell us if the amount of trips increases when it is raining or not. From there, we would like to delve deeper, e.g., assess the effect of rush hour on the amount of trips when it is raining. Thus, the goal is to build a database that facilitates analysis.

## How to use

Do as follows:

1. Creat a Redshift cluster by running `sh create_cluster.sh` in the terminal.
2. Fill in all details in the `config_example.cfg` file and rename to `config.cfg`
3. Run `python taxi_and_weather_etl.py` from the terminal and enjoy!

## Data assesment

The data was assesed in the file `data_exploration.ipynb`.

## ETL pipeline

For the pipeline we'll undertake the following process.

1. We stage the data in S3 in CSV format.
2. We import the data from S3 into Redshift using staging tables.
3. We do some data checks to ensure that the import went well.
4. We do transformations to obtain our data model for our analyses.

## Data model

For the data model, we used the ERD as given below.

![ERD](img/schema.png)

Thus, we first stage the data untouched in Redshift. We create two tables:

* `staging_weather`, which contains the raw weather data
* `staging_taxi`, which contains the raw taxi data

From there, we do some transformations to obtain the following tables:

* `taxi_facts`, which contains the taxi data that we need to perform our analysis from the `staging_taxi` table with an additional identification column. In this table, we dropped the rows that had a negative fare_amount or total_amount.
* `taxi_locations`, which contains information about the location IDs used in the columns: pickup_location_id and drop_off_location_id.
* `weather_facts`, which contains the weather data that we need to perform our analysis from the `staging_weather` table.
* `time_table`, which contains all of the timestamps from the `taxi_facts` and `weather_facts` table in specific units.

Finally, we also create a view in which we combine the `taxi_facts` and `weather facts` table:

* `taxi_weather_facts`, which joins the `taxi_facts` table with the `weather_facts` table based on the proximity of pickup_timestamp and time_column (the closer the better). We do one join per row in the `taxi_facts` table.

## Analysis

From here, we can finally perform our analysis to find out if rain has an effect on the amount of rides per hour. We do so by executing the following query:

~~~~sql
SELECT rain,
    sum(counts)/count(hour) as rides_per_hour
FROM (
    SELECT 
        count(a.pickup_timestamp) as counts,
        a.precipitation > 0 as rain,
        b.hour,
        b.day,
        b.month,
        b.year
    FROM taxi_weather_facts a, time_table b
    WHERE a.pickup_timestamp = b.time_column
    GROUP BY rain, b.hour, b.day, b.month, b.year
)
GROUP BY rain
~~~~

We find that there are less rides when there is rain then where there is rain, which corresponds to the conclusion of [this paper](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5584943/). However, the paper also states that rush-hour makes a difference, which we leave up to the reader to find out.

## Further development

For further development, Airflow could be incorporated to run this pipeline on a monthly basis. If we wish to run this pipeline every day, we could obtain the taxi dataset each day from [NYC OpenData](https://data.cityofnewyork.us/browse?Dataset-Information_Agency=Taxi+and+Limousine+Commission+%28TLC%29&). The weather data can be obtained from the API as is on a daily basis. The ETL script can easily be implemented in Airflow, as it is structured as a DAG would be.

The data could easily scale as we are using Redshift to 100x. In that case, we might have to add some nodes to our clusters. The table data is distributed by making use of `diststyle`, `distkey` and `sortkey` where necessary to ensure optimal query performance. Therefore, Redshift could also easily accomodate access by 100+ people per day.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)