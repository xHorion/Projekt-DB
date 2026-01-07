-- CREATE DATABASE IF NOT EXISTS UKF_PROJECT; -- Using instead my own student db.
-- CREATE SCHEMA IF NOT EXISTS UKF_PROJECT.UKF_PROJECT;

-- USE WAREHOUSE QUAIL_WH;

-- USE DATABASE QUAIL_DB;
-- USE SCHEMA UKF_PROJECT;

-- Load

CREATE OR REPLACE TABLE STAGING_HOURLY_WEATHER AS
SELECT 
    "PC_SECT" AS pc_sect,
    "Validity_date_and_time" AS validity_date_and_time,
    "Issued_at" AS issued_at,
    "Screen_temperature" AS screen_temperature,
    "POINT" AS point
FROM POSTCODE_SECTOR_WEATHER_FORECASTS__SAMPLE.PCSECT_FORECAST."free_view_hourly";

CREATE OR REPLACE TABLE STAGING_DAILY_WEATHER AS
SELECT 
    "PC_SECT" AS pc_sect,
    "Validity_date" AS validity_date,
    "Issued_at" AS issued_at,
    "Max_temperature_day" AS max_temperature_day,
    "Min_temperature_night" AS min_temperature_night,
    "Max_feels_like_temperature_day" AS max_feels_like_temperature_day,
    "Min_feels_like_temperature_night" AS min_feels_like_temperature_night,
    "Visibility_Approx_Local_Midday" AS visibility_approx_local_midday,
    "Visibility_Approx_Local_Midnight" AS visibility_approx_local_midnight,
    "Significant_weather_day" AS significant_weather_day,
    "Significant_weather_type_day" AS significant_weather_type_day,
    "Significant_weather_night" AS significant_weather_night,
    "Significant_weather_type_night" AS significant_weather_type_night,
    "Wind_speed_Approx_Local_Midday" AS wind_speed_approx_local_midday,
    "Wind_direction_Approx_Local_Midday" AS wind_direction_approx_local_midday,
    "Wind_speed_Approx_Local_Midnight" AS wind_speed_approx_local_midnight,
    "Wind_direction_Approx_Local_Midnight" AS wind_direction_approx_local_midnight,
    "POINT" AS point
FROM POSTCODE_SECTOR_WEATHER_FORECASTS__SAMPLE.PCSECT_FORECAST."free_view_daily";

CREATE OR REPLACE TABLE DIM_LOCATION (
    location_key INT PRIMARY KEY AUTOINCREMENT,
    postcode VARCHAR(20),
    latitude FLOAT,
    longitude FLOAT
);

CREATE OR REPLACE TABLE DIM_WEATHER_TYPE (
    weather_type_key INT PRIMARY KEY AUTOINCREMENT,
    weather_type_code INT,
    weather_description VARCHAR(50)
);

CREATE OR REPLACE TABLE DIM_DATE (
    date_key INT PRIMARY KEY, -- Format YYYYMMDD
    date DATE,
    day INT,
    week_day INT,
    month INT,
    year INT,
    quarter INT,
    season VARCHAR(15)
);

CREATE OR REPLACE TABLE DIM_WIND_TYPE (
    wind_type_key INT PRIMARY KEY AUTOINCREMENT,
    wind_direction VARCHAR(5), -- N, NE, E, SE, S, SW, W, NW, N
    wind_speed_category VARCHAR(15)
);

CREATE OR REPLACE TABLE FACT_WEATHER_FORECAST (
    id INT PRIMARY KEY AUTOINCREMENT,
    
    max_temperature_day NUMBER(6,2),
    min_temperature_night NUMBER(6,2),
    
    avg_temperature_day NUMBER(6,2),
    past_day_temperature_delta NUMBER(6,2),
    
    max_feels_like_temperature_day NUMBER(6,2),
    min_feels_like_temperature_night NUMBER(6,2),
    
    visibility_midday NUMBER(6),
    visibility_midnight NUMBER(6),
    
    wind_direction_midday NUMBER(3),
    wind_direction_midnight NUMBER(3),
    wind_speed_midday NUMBER(6,2),
    wind_speed_midnight NUMBER(6,2),
    
    dim_date_key INT,
    dim_location_key INT,
    dim_weather_type_key_midday INT,
    dim_weather_type_key_midnight INT,
    dim_wind_type_key_midday INT,
    dim_wind_type_key_midnight INT,
    
    forecast_issued_at TIMESTAMP_NTZ 
);

-- UDF
-- Using user defined function to not repeat yourself

CREATE OR REPLACE FUNCTION get_wind_direction(deg FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE 
        WHEN deg >= 337.5 OR deg < 22.5 THEN 'N'
        WHEN deg >= 22.5 AND deg < 67.5 THEN 'NE'
        WHEN deg >= 67.5 AND deg < 112.5 THEN 'E'
        WHEN deg >= 112.5 AND deg < 157.5 THEN 'SE'
        WHEN deg >= 157.5 AND deg < 202.5 THEN 'S'
        WHEN deg >= 202.5 AND deg < 247.5 THEN 'SW'
        WHEN deg >= 247.5 AND deg < 292.5 THEN 'W'
        WHEN deg >= 292.5 AND deg < 337.5 THEN 'NW'
        ELSE 'Unknown'
    END
$$;

CREATE OR REPLACE FUNCTION get_wind_category(speed FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    CASE 
        WHEN speed < 11 THEN 'Light'
        WHEN speed BETWEEN 11 AND 28 THEN 'Moderate'
        WHEN speed BETWEEN 29 AND 49 THEN 'Strong'
        ELSE 'Gale'
    END
$$;

-- Transform

INSERT INTO DIM_LOCATION (postcode, longitude, latitude)
SELECT DISTINCT 
    pc_sect AS postcode,
    ST_X(point) AS longitude,
    ST_Y(point) AS latitude
FROM STAGING_DAILY_WEATHER;

INSERT INTO DIM_DATE (date_key, date, day, month, year, quarter, season)
SELECT DISTINCT 
    TO_NUMBER(TO_CHAR(validity_date, 'YYYYMMDD')) AS date_key,
    validity_date AS date,
    DAY(validity_date) AS day,
    MONTH(validity_date) AS month,
    YEAR(validity_date) AS year,
    QUARTER(validity_date) AS quarter,
    CASE 
        WHEN MONTH(validity_date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(validity_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(validity_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Autumn'
    END AS season
FROM STAGING_DAILY_WEATHER;

INSERT INTO DIM_WEATHER_TYPE (weather_type_code, weather_description)
SELECT DISTINCT significant_weather_day, significant_weather_type_day FROM STAGING_DAILY_WEATHER
UNION
SELECT DISTINCT significant_weather_night, significant_weather_type_night FROM STAGING_DAILY_WEATHER;

INSERT INTO DIM_WIND_TYPE (wind_direction, wind_speed_category)
WITH All_Wind_Data AS (
    SELECT wind_direction_approx_local_midday as deg, wind_speed_approx_local_midday as speed FROM STAGING_DAILY_WEATHER
    UNION ALL
    SELECT wind_direction_approx_local_midnight as deg, wind_speed_approx_local_midnight as speed FROM STAGING_DAILY_WEATHER
)
SELECT DISTINCT
    get_wind_direction(deg) AS direction,
    get_wind_category(speed) AS category 
FROM All_Wind_Data;

-- Final Transform

INSERT INTO FACT_WEATHER_FORECAST (
    max_temperature_day, min_temperature_night, 
    avg_temperature_day,
    past_day_temperature_delta, 
    max_feels_like_temperature_day, min_feels_like_temperature_night, 
    wind_speed_midday, wind_speed_midnight, wind_direction_midday, wind_direction_midnight,
    visibility_midday, visibility_midnight,
    dim_date_key, dim_location_key, 
    dim_weather_type_key_midday, dim_weather_type_key_midnight,
    dim_wind_type_key_midday, dim_wind_type_key_midnight,
    forecast_issued_at
)
-- Deduplicating hour forecasts
WITH Hourly_Deduped AS (
    SELECT * 
    FROM STAGING_HOURLY_WEATHER
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pc_sect, validity_date_and_time ORDER BY issued_at DESC) = 1
),
-- Agregating hour forecasts
Hourly_Agregation AS (
    SELECT 
        DATE(validity_date_and_time) AS v_date,
        pc_sect,
        ROUND(AVG(screen_temperature), 2) AS daily_avg
    FROM Hourly_Deduped
    GROUP BY v_date, pc_sect
),
-- Deduplicating daily forecasts
Daily_Deduped AS (
    SELECT * 
    FROM STAGING_DAILY_WEATHER
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pc_sect, validity_date ORDER BY issued_at DESC) = 1 
),
-- Calculating temperature LAG
Daily_With_Lag AS (
    SELECT 
        dd.*,
        h.daily_avg,
        LAG(h.daily_avg) OVER (PARTITION BY dd.pc_sect ORDER BY dd.validity_date) as prev_avg
    FROM Daily_Deduped dd
    -- Using left join to protect raw data from not inserting if some data was not filled in a right way
    LEFT JOIN Hourly_Agregation h ON dd.validity_date = h.v_date AND dd.pc_sect = h.pc_sect 
)
-- Final Select & Lookup
SELECT 
    dwl.max_temperature_day AS max_temperature_day,
    dwl.min_temperature_night AS min_temperature_night,
    dwl.daily_avg AS avg_temperature_day,
    ABS(dwl.daily_avg - COALESCE(dwl.prev_avg, dwl.daily_avg)) AS past_day_temperature_delta, 
    dwl.max_feels_like_temperature_day AS max_feels_like_temperature_day,
    dwl.min_feels_like_temperature_night AS min_feels_like_temperature_night,
    dwl.wind_speed_approx_local_midday AS wind_speed_approx_local_midday,
    dwl.wind_speed_approx_local_midnight AS wind_speed_approx_local_midnight,
    dwl.wind_direction_approx_local_midday AS wind_direction_approx_local_midday,
    dwl.wind_direction_approx_local_midnight AS wind_direction_approx_local_midnight,
    dwl.visibility_approx_local_midday AS visibility_approx_local_midday,
    dwl.visibility_approx_local_midnight AS visibility_approx_local_midnight,
    TO_NUMBER(TO_CHAR(dwl.validity_date, 'YYYYMMDD')) AS dim_date_key,
    l.location_key AS dim_location_key,
    wtd.weather_type_key AS dim_weather_type_key_midday,
    wtn.weather_type_key AS dim_weather_type_key_midnight,
    wdtm.wind_type_key AS dim_wind_type_key_midday,
    wdtn.wind_type_key AS dim_wind_type_key_midnight,
    dwl.issued_at AS forecast_issued_at
FROM Daily_With_Lag dwl
JOIN DIM_LOCATION l ON dwl.pc_sect = l.postcode
-- Using left join to protect raw data from not inserting if some data was not filled in a right way
LEFT JOIN DIM_WEATHER_TYPE wtd ON dwl.significant_weather_day = wtd.weather_type_code
LEFT JOIN DIM_WEATHER_TYPE wtn ON dwl.significant_weather_night = wtn.weather_type_code
LEFT JOIN DIM_WIND_TYPE wdtm ON 
    wdtm.wind_direction = get_wind_direction(dwl.wind_direction_approx_local_midday) AND 
    wdtm.wind_speed_category = get_wind_category(dwl.wind_speed_approx_local_midday)
LEFT JOIN DIM_WIND_TYPE wdtn ON 
    wdtn.wind_direction = get_wind_direction(dwl.wind_direction_approx_local_midnight) AND 
    wdtn.wind_speed_category = get_wind_category(dwl.wind_speed_approx_local_midnight);

-- Optimizing storage

DROP TABLE IF EXISTS STAGING_HOURLY_WEATHER;
DROP TABLE IF EXISTS STAGING_DAILY_WEATHER;

-- Verification

-- SELECT 
--     COUNT(*) AS total_rows,
--     COUNT(CASE WHEN past_day_temperature_delta > 0 THEN 1 END) AS rows_with_change
-- FROM FACT_WEATHER_FORECAST;

-- SELECT dd.date, dl.postcode, wf.past_day_temperature_delta 
-- FROM FACT_WEATHER_FORECAST wf 
-- JOIN DIM_DATE dd ON wf.dim_date_key=dd.date_key 
-- JOIN DIM_LOCATION dl ON wf.dim_location_key=dl.location_key ORDER BY dl.postcode, dd.date ASC;

SELECT * FROM FACT_WEATHER_FORECAST LIMIT 10;
