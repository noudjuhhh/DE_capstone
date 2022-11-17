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

basicConfig(level=INFO)
logger = getLogger(__name__)

config = configparser.ConfigParser()
config.read("config.cfg")

S3_BUCKET = config["s3"]["bucket"]
KEY = config["aws"]["key"]
SECRET = config["aws"]["secret"]


def get_weather_data(start_date: date, end_date: date) -> Iterator[pd.DataFrame]:
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


class SQL_ETL:
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
        Creates each table using the queries in `create_table_queries` list.
        """
        logger.info("Creating tables in Redshift")
        self._loop_and_execute(self.connection, CreateQueries().queries())

    def drop_tables(self) -> None:
        """
        Drops each table using the queries in `drop_table_queries` list.
        """
        logger.info("Dropping tables in Redshift")
        self._loop_and_execute(self.connection, DropQueries().queries())

    def copy_tables(self) -> None:
        """
        Copy table using the queries in `copy_data_queries` list.
        """
        logger.info("Copying data from S3 to Redshift")
        self._loop_and_execute(self.connection, CopyQueries().queries())

    def data_quality_checks(self) -> None:
        """
        Checks for the data quality queries that the result does
        equal 0
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
        Populate tables using the queries in `populate_table_queries` list.
        """
        logger.info("Populating tables in Redshift")
        self._loop_and_execute(self.connection, PopulateQueries().queries())

    def close_connection(self) -> None:
        self.connection.close()


class DataToS3:
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
    # stager = DataToS3()
    # stager.weather_data_to_s3(date(2021, 1, 1), date(2021, 1, 31))
    # stager.taxi_data_to_s3(2021, 1)
    # stager.taxi_zone_ids_to_s3()
    ETL = SQL_ETL()
    ETL.drop_tables()
    ETL.create_tables()
    ETL.copy_tables()
    ETL.data_quality_checks()
    ETL.populate_tables()
    ETL.close_connection()


if __name__ == "__main__":
    main()
