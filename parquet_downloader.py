#%% We first get the
import pandas as pd

trip_data_orig = pd.read_parquet(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
)

#%%
trip_data = trip_data_orig.copy()


def localize_dt(var):
    return var.dt.tz_localize("America/New_York")


for dt_column in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
    trip_data[dt_column] = localize_dt(trip_data[dt_column])

columns_to_keep = "tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, PULocationID, DOLocationID, RatecodeID, payment_type, fare_amount, total_amount".split(
    ", "
)

trip_data[columns_to_keep].set_axis(
    "pickup_timestamp, dropoff_timestamp, passenger_count, trip_distance, pickup_location_id, drop_off_location_id, rate_code, payment_type, fare_amount, paid_amount".split(
        ", "
    ),
    axis=1,
)


#%%
location_ids = pd.read_csv(
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
)

# %%
import json
import requests

r = requests.get(
    "https://api.weather.com/v1/location/KLGA:9:US/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=m&startDate=20210101&endDate=20210131"
)
json = r.json()

#%%
weather_data = pd.DataFrame.from_dict(json["observations"])
weather_data = weather_data.dropna(axis=1)


def convert_to_dt(var: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(var, unit="s")
        .dt.tz_localize("GMT")
        .dt.tz_convert("America/New_York")
    )


for dt_column in ["expire_time_gmt", "valid_time_gmt"]:
    weather_data[dt_column] = convert_to_dt(weather_data[dt_column])

weather_data = weather_data[
    "valid_time_gmt, temp, dewPt, rh, pressure, wspd, precip_hrly, feels_like, wx_phrase".split(
        ", "
    )
].set_axis(
    "timestamp, temperature, dewpoint_temperature, relative_humidity, pressure, windspeed, precipitation, feels_like, classification".split(
        ", "
    ),
    axis=1,
)
