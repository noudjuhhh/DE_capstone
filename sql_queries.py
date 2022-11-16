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

create_table_queries = [staging_weather_create]
