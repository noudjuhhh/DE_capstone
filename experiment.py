#%%
import requests
from datetime import date, timedelta
import pandas as pd
from typing import Iterator
import psycopg2
import configparser
from sql_queries import create_table_queries
import boto3
from utils import profile
from tempfile import NamedTemporaryFile

config = configparser.ConfigParser()
config.read("config.cfg")

S3_BUCKET = config["s3"]["bucket"]
KEY = config["aws"]["key"]
SECRET = config["aws"]["secret"]


def get_weather_data(start_date: date, end_date: date) -> Iterator[pd.DataFrame]:
    def to_datestring(date_object):
        return date_object.strftime("%Y%m%d")

    session = requests.Session()
    next_date_to_get = start_date

    while True:

        if next_date_to_get >= end_date:
            break

        date_to_get = next_date_to_get
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


def create_tables(connection):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    with connection.cursor() as cursor:
        for query in create_table_queries:
            cursor.execute(query)
            connection.commit()


def weather_data_to_s3(start_date: date, end_date: date) -> None:
    s3 = boto3.resource(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )

    counter = 0
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
        ).put(Body=df.to_csv())
        counter += 1


@profile
def taxi_data_to_s3(stream_to_s3=True) -> None:
    s3 = boto3.resource(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    s3_key = "raw/taxi/New_York/" + url.split("/")[-1]

    if stream_to_s3:
        with requests.get(url, stream=True) as r:
            s3.Bucket(S3_BUCKET).upload_fileobj(r.raw, s3_key)
    else:
        with NamedTemporaryFile("wb") as f:
            data = requests.get(url)
            f.write(data.content)
            s3.Bucket(S3_BUCKET).upload_file(f.name, s3_key)


def main():
    # conn = psycopg2.connect(
    #     "dbname={db} user={user} password={password} port={port} host={host}".format(
    #         **config["redshift"]
    #     )
    # )
    # # create_tables(conn)
    # conn.close()
    weather_data_to_s3(date(2021, 1, 1), date(2021, 2, 1))
    taxi_data_to_s3()


if __name__ == "__main__":
    main()
