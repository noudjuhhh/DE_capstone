import configparser
from typing import List

config = configparser.ConfigParser()
config.read("config.cfg")

S3_BUCKET = config["s3"]["bucket"]
KEY = config["aws"]["key"]
SECRET = config["aws"]["secret"]


class CreateQueries:
    """
    Contains all the queries for the creation of tables
    """

    staging_weather_create = """
    CREATE TABLE IF NOT EXISTS staging_weather (
        valid_time_gmt bigint,
        temp smallint,
        dewPt smallint,
        rh smallint,
        pressure float,
        wspd smallint,
        precip_hrly float,
        feels_like smallint,
        wx_phrase varchar(255)
    )
    """

    staging_taxi_create = """
    CREATE TABLE IF NOT EXISTS staging_taxi (
        VendorID smallint,
        tpep_pickup_datetime timestamp,
        tpep_dropoff_datetime timestamp,
        passenger_count float,
        trip_distance float,
        RatecodeID float, 
        store_and_fwd_flag CHAR(1),
        PULocationID smallint,
        DOLocationID smallint,
        payment_type smallint,
        fare_amount decimal(12,2),
        extra decimal(12,2),
        mta_tax decimal(12,2),
        tip_amount decimal(12,2),
        tolls_amount decimal(12,2),
        improvement_surcharge decimal(12,2),
        total_amount decimal(12,2),
        congestion_surcharge decimal(12,2),
        airport_fee decimal(12,2)
    )
    """

    taxi_lookup_create = """
    CREATE TABLE IF NOT EXISTS taxi_locations (
        location_id smallint PRIMARY KEY,
        borough varchar(255),
        zone varchar(255),
        service_zone varchar(255)
    )
    diststyle all
    """

    time_table_create = """
    CREATE TABLE IF NOT EXISTS time_table (
        time_column timestamptz PRIMARY KEY distkey sortkey,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
    """

    taxi_table_create = """
    CREATE TABLE IF NOT EXISTS taxi_facts (
        ride_id int IDENTITY(0,1) PRIMARY KEY,
        pickup_timestamp timestamptz NOT NULL REFERENCES time_table(time_column) distkey sortkey,
        dropoff_timestamp timestamptz NOT NULL REFERENCES time_table(time_column),
        passenger_count smallint,
        trip_distance float NOT NULL,
        pickup_location_id smallint NOT NULL REFERENCES taxi_locations(location_id),
        drop_off_location_id smallint NOT NULL REFERENCES taxi_locations(location_id),
        fare_amount decimal(12,2),
        paid_amount decimal(12,2)
    )
    """

    weather_table_create = """
    CREATE TABLE IF NOT EXISTS weather_facts (
        time_column timestamptz PRIMARY KEY REFERENCES time_table(time_column) distkey sortkey,
        temperature smallint NOT NULL,
        dewpoint_temperature smallint NOT NULL,
        relative_humidity smallint NOT NULL,
        pressure float NOT NULL,
        windspeed smallint NOT NULL,
        precipitation float NOT NULL,
        feels_like smallint NOT NULL,
        classification varchar(255)  NOT NULL
    )
    """

    def queries(self) -> List[str]:
        return [
            self.staging_weather_create,
            self.staging_taxi_create,
            self.time_table_create,
            self.taxi_lookup_create,
            self.taxi_table_create,
            self.weather_table_create,
        ]


class CopyQueries:
    """
    Contains all the queries for the copying of the data from S3 into
        the tables
    """

    table_and_sources = {
        "staging_weather": "raw/weather/New_York",
        "staging_taxi": "raw/taxi/New_York/yellow_trip",
        "taxi_locations (location_id, borough, zone, service_zone)": "raw/taxi/New_York/taxi_zone_lookup",
    }

    def _copy_query(self, table_name: str, s3_key: str) -> str:
        return f"""
        COPY {table_name}
        FROM 's3://{S3_BUCKET}/{s3_key}'
        ACCESS_KEY_ID '{KEY}'
        SECRET_ACCESS_KEY '{SECRET}'
        region 'us-east-1'
        FORMAT AS CSV
        DELIMITER ','
        IGNOREHEADER 1
        """

    def queries(self) -> List[str]:
        return [
            self._copy_query(dict_key, self.table_and_sources[dict_key])
            for dict_key in self.table_and_sources.keys()
        ]


class PopulateQueries:
    """
    Contains all the queries to populate the analytics tables
        from the staging tables
    """

    taxi_table = """
    INSERT INTO taxi_facts (
        pickup_timestamp,
        dropoff_timestamp,
        passenger_count,
        trip_distance,
        pickup_location_id,
        drop_off_location_id,
        fare_amount,
        paid_amount
    )
    SELECT
        tpep_pickup_datetime AT TIME ZONE 'America/New_York' pickup_timestamp,
        tpep_dropoff_datetime AT TIME ZONE 'America/New_York' dropoff_timestamp,
        passenger_count,
        trip_distance,
        PULocationID AS pickup_location_id,
        DOLocationID AS drop_off_location_id,
        fare_amount,
        total_amount AS paid_amount
    FROM staging_taxi
    WHERE
        fare_amount >=0 AND paid_amount => 0
    """

    weather_table = """
    INSERT INTO weather_facts
    SELECT
        convert_timezone('GMT',
                'America/New_York',
                timestamp 'epoch' + valid_time_gmt * interval '1 second')
            AS time_column,
        temp AS temperature,
        dewPt AS dewpoint_temperature,
        rh AS relative_humidity,
        pressure,
        wspd AS windspeed,
        precip_hrly AS precipitation,
        feels_like,
        wx_phrase AS classification
    FROM staging_weather
    """

    taxi_weather_table = """
    CREATE VIEW taxi_weather_facts AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                row_number() over (
                    partition by taxi_facts.ride_id
                    order by abs(extract(
                        epoch from weather_facts.time_column - taxi_facts.pickup_timestamp
                    )) asc
                ) AS ranking
            FROM
                taxi_facts 
            JOIN
                weather_facts ON weather_facts.time_column between taxi_facts.pickup_timestamp  
                and taxi_facts.pickup_timestamp + '1 hour'::interval 
                OR weather_facts.time_column between taxi_facts.pickup_timestamp - '1 hour'::interval  
                and taxi_facts.pickup_timestamp
        )
        WHERE ranking = 1
    )
    """

    time_table = """
    INSERT INTO time_table
    SELECT time_column,
        extract(hour FROM time_column) as hour,
        extract(day FROM time_column) as day,
        extract(week FROM time_column) as week,
        extract(month FROM time_column) as month,
        extract(year FROM time_column) as year,
        extract(dow FROM time_column) as weekday
    FROM
    (
        SELECT DISTINCT tpep_pickup_datetime AT TIME ZONE 'America/New_York' as time_column
        FROM staging_taxi
        UNION
        SELECT DISTINCT tpep_dropoff_datetime AT TIME ZONE 'America/New_York' as time_column
        FROM staging_taxi
        UNION
        SELECT DISTINCT convert_timezone('GMT', 'America/New_York', timestamp 'epoch' + valid_time_gmt * interval '1 second') as time_column
        FROM staging_weather
    )
    """

    def queries(self) -> List[str]:
        return [
            self.time_table,
            self.taxi_table,
            self.weather_table,
            self.taxi_weather_table,
        ]


class QualityQueries:
    """
    Contains all the queries for the data quality checks
    """

    def queries(self) -> List[str]:
        return [
            """select count(*) from staging_weather where temp > 100 or temp < -100""",
            """select count(*) from staging_taxi where trip_distance < 0""",
            """select (count(*) - 24*31 < 0)::int from staging_weather""",
        ]


class DropQueries:
    """
    Contains all the queries for dropping all the views and tables
    """

    tables_to_drop = "staging_weather, staging_taxi, taxi_facts, weather_facts, taxi_locations, time_table".split(
        ", "
    )
    views_to_drop = ["taxi_weather_facts"]

    def queries(self) -> List[str]:
        return ["DROP VIEW IF EXISTS " + table for table in self.views_to_drop] + [
            "DROP TABLE IF EXISTS " + table for table in self.tables_to_drop
        ]
