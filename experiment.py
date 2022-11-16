#%%
import requests
from datetime import date, timedelta
import pandas as pd
from typing import Iterator
import psycopg2
import configparser
from sql_queries import create_table_queries

config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_DB = config["dwh"]["db"]
DWH_DB_USER = config["dwh"]["user"]
DWH_DB_PASSWORD = config["dwh"]["password"]
DWH_PORT = config["dwh"]["port"]
DWH_HOST = config["dwh"]["host"]

S3_BUCKET = config["s3"]["bucket"]


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
    counter = 0
    for df in get_weather_data(start_date, end_date):
        s3.Object(
            "mybucket",
            "raw/weather/New_York/"
            + str(start_date)
            + "-"
            + str(end_date)
            + "/day_"
            + str(counter)
            + ".csv",
        ).put(Body=df.to_csv())
        counter += 1


def main():
    # conn = psycopg2.connect(
    #     f"host={DWH_HOST} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}"
    # )
    # create_tables(conn)
    stage_weather_data(conn, date(2021, 1, 1), date(2021, 2, 1))
    conn.close()


if __name__ == "__main__":
    # main()
    pass
