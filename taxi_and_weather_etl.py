#%%
import requests
from datetime import date, timedelta
import pandas as pd
from typing import Iterator
import psycopg2
import configparser
from sql_queries import (
    CopyQueries,
    CreateQueries,
    PopulateQueries,
    QualityQueries,
    DropQueries,
)
import boto3
from tempfile import NamedTemporaryFile
from logging import getLogger, basicConfig, INFO
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData

basicConfig(level=INFO)
logger = getLogger(__name__)

config = configparser.ConfigParser()
config.read("config.cfg")

S3_BUCKET = config["s3"]["bucket"]
KEY = config["aws"]["key"]
SECRET = config["aws"]["secret"]


def get_weather_data(start_date: date, end_date: date) -> Iterator[pd.DataFrame]:
    """
    Gets the weather data from the weather API as a generator to save memory
    """

    def to_datestring(date_object: date) -> str:
        return date_object.strftime("%Y%m%d")

    session = requests.Session()
    next_date_to_get = start_date

    while True:

        date_to_get = next_date_to_get

        if date_to_get > end_date:
            break

        next_date_to_get = date_to_get + timedelta(days=1)

        r = session.get(
            "https://api.weather.com/v1/location/KLGA:9:US/observations/historical.json",
            params={
                "apiKey": "e1f10a1e78da46f5b10a1e78da96f525",
                "units": "m",
                "startDate": to_datestring(date_to_get),
                "endDate": to_datestring(date_to_get),
            },
        )

        weather_data = pd.DataFrame.from_dict(r.json()["observations"])[
            "valid_time_gmt, temp, dewPt, rh, pressure, wspd, precip_hrly, feels_like, wx_phrase".split(
                ", "
            )
        ]

        yield weather_data


def create_ERD() -> None:
    """
    Creates an ERD of our database when all data is imported
    """
    graph = create_schema_graph(
        metadata=MetaData(
            "postgresql://{user}:{password}@{host}:{port}/{db}".format(
                **config["redshift"]
            )
        )
    )
    graph.write_png("./img/schema.png")


class SQL_ETL:
    """
    Class that has all the ETL processes for Redshift (drop, create, copy, and so forth)
    """

    def __init__(self):
        self.connection = psycopg2.connect(
            "dbname={db} user={user} password={password} port={port} host={host}".format(
                **config["redshift"]
            )
        )

    def _loop_and_execute(self, connection, queries) -> None:
        with connection.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
                connection.commit()

    def create_tables(self) -> None:
        """
        Creates each table
        """
        logger.info("Creating tables in Redshift")
        self._loop_and_execute(self.connection, CreateQueries().queries())

    def drop_tables(self) -> None:
        """
        Drops each table
        """
        logger.info("Dropping tables in Redshift")
        self._loop_and_execute(self.connection, DropQueries().queries())

    def copy_tables(self) -> None:
        """
        Copies the data from S3 into each table
        """
        logger.info("Copying data from S3 to Redshift")
        self._loop_and_execute(self.connection, CopyQueries().queries())

    def data_quality_checks(self) -> None:
        """
        Checks for the data quality queries that the result
            equals 0
        """
        logger.info("Checking the data quality of the staged tables.")
        with self.connection.cursor() as cursor:
            count = 1
            for query in QualityQueries().queries():
                cursor.execute(query)
                results = cursor.fetchall()
                result = results[0][0]
                assert (
                    result == 0
                ), f"Data quality not passed for test {count}, found {result} instead of 0"
                count += 1

    def populate_tables(self) -> None:
        """
        Populates the analytics tables using staged tables
        """
        logger.info("Populating tables in Redshift")
        self._loop_and_execute(self.connection, PopulateQueries().queries())

    def close_connection(self) -> None:
        self.connection.close()


class DataToS3:
    """
    Class that stages the data from different sources into S3 as CSVs
    """

    def __init__(self):
        self.s3 = boto3.resource(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET,
        )

    def weather_data_to_s3(self, start_date: date, end_date: date) -> None:
        logger.info("Downloading weather data to S3")
        s3 = self.s3
        counter = 1
        for df in get_weather_data(start_date, end_date):
            s3.Object(
                S3_BUCKET,
                "raw/weather/New_York/"
                + str(start_date)
                + "-"
                + str(end_date)
                + "/day_"
                + str(counter)
                + ".csv",
            ).put(Body=df.to_csv(index=False), ContentType="text/csv")
            counter += 1

    def taxi_data_to_s3(self, year: int, month: int) -> None:
        logger.info("Downloading taxi data to S3")
        s3 = self.s3
        url = (
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
            + str(year)
            + "-"
            + str(month).zfill(2)
            + ".parquet"
        )
        s3_key = "raw/taxi/New_York/" + url.split("/")[-1][:-7] + "csv"

        # if stream_to_s3:
        #     with requests.get(url, stream=True) as r:
        #         s3.Bucket(S3_BUCKET).upload_fileobj(r.raw, s3_key)
        # else:
        with NamedTemporaryFile("wb") as f:
            data = requests.get(url)
            f.write(data.content)
            s3.Object(S3_BUCKET, s3_key).put(
                Body=pd.read_parquet(f.name).to_csv(index=False), ContentType="text/csv"
            )

    def taxi_zone_ids_to_s3(self) -> None:
        logger.info("Downloading taxi zone lookup data to S3")
        s3 = self.s3
        url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
        s3_key = "raw/taxi/New_York/taxi_zone_lookup.csv"
        with requests.get(url, stream=True) as r:
            s3.Object(S3_BUCKET, s3_key).put(Body=r.text, ContentType="text/csv")


def main() -> None:
    """
    1. Gets the weather data for January 2021 into S3
    2. Gets the taxi data for January 2021 into S3
    3. Gets the taxi location ids into S3
    4. Drops all tables if they exist
    5. Creates all tables
    6. Copies the data from S3 into the staging tables
    7. Some data quality checks if the data is as expected
    8. We transform the data from the staging tables
    9. We close the connection
    """
    stager = DataToS3()
    stager.weather_data_to_s3(date(2021, 1, 1), date(2021, 1, 31))  # Step 1
    stager.taxi_data_to_s3(2021, 1)  # Step 2
    stager.taxi_zone_ids_to_s3()  # Step 3
    ETL = SQL_ETL()
    ETL.drop_tables()  # Step 4
    ETL.create_tables()  # Step 5
    ETL.copy_tables()  # Step 6
    ETL.data_quality_checks()  # Step 7
    ETL.populate_tables()  # Step 8
    ETL.close_connection()  # Step 9


if __name__ == "__main__":
    # create_ERD()
    main()
