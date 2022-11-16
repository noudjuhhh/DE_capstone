import configparser

config = configparser.ConfigParser()
config.read("config.cfg")

S3_BUCKET = config["s3"]["bucket"]
KEY = config["aws"]["key"]
SECRET = config["aws"]["secret"]

staging_weather_create = """
CREATE TABLE IF NOT EXISTS staging_weather (
    valid_time_gmt bigint,
    temp int,
    dewPt int,
    rh int,
    pressure int,
    wspd int,
    precip_hrly float,
    feels_like int,
    wx_phrase varchar(255)
)
"""

staging_taxi_create = """
CREATE TABLE IF NOT EXISTS staging_taxi (
    VendorID smallint,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count int,
    trip_distance float,
    RatecodeID int,
    store_and_fwd_flag CHAR(1),
    PULocationID int,
    DOLocationID int,
    payment_type int,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    improvement_surcharge float,
    total_amount float,
    congestion_surcharge float,
    airport_fee float
)
"""

copy_data_weather = f"""
COPY staging_weather 
FROM 's3://{S3_BUCKET}/raw/weather/New_York/'
ACCESS_KEY_ID '{KEY}'
SECRET_ACCESS_KEY '{SECRET}'
region 'us-east-1'
FORMAT AS CSV;
"""

copy_data_taxi = f"""
COPY staging_taxi
FROM 's3://{S3_BUCKET}/raw/taxi/New_York/'
ACCESS_KEY_ID '{KEY}'
SECRET_ACCESS_KEY '{SECRET}'
region 'us-east-1'
FORMAT AS PARQUET;
"""

create_table_queries = [staging_weather_create, staging_taxi_create]
copy_data_queries = [copy_data_weather, copy_data_taxi]
