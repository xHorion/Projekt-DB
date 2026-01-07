-- Graf 1 Forecast Lead Time (Data Quality Check)

SELECT 
    l.postcode,
    f.forecast_issued_at,
    d.date,
    DATEDIFF('hour', f.forecast_issued_at, d.date) as forecast_lead_time_hours
FROM FACT_WEATHER_FORECAST f
JOIN DIM_LOCATION l ON f.dim_location_key = l.location_key
JOIN DIM_DATE d ON f.dim_date_key = d.date_key
ORDER BY forecast_lead_time_hours DESC;

-- Graf 2 Dates For Outdoor Work

SELECT 
    l.postcode, 
    d.date,
    f.avg_temperature_day,
    wd.wind_speed_category,
    wt.weather_description
FROM FACT_WEATHER_FORECAST f
JOIN DIM_LOCATION l ON f.dim_location_key = l.location_key
JOIN DIM_DATE d ON f.dim_date_key = d.date_key
JOIN DIM_WEATHER_TYPE wt ON f.dim_weather_type_key_midday = wt.weather_type_key
JOIN DIM_WIND_TYPE wd ON f.dim_wind_type_key_midday = wd.wind_type_key
WHERE wt.weather_description LIKE '%Clear%' OR wt.weather_description LIKE '%Partly Cloudy%'
  AND wd.wind_speed_category = 'Light'
  AND f.visibility_midday > 10000
ORDER BY d.date;;

-- Graf 3 Posible Weather Volatility in Regions

SELECT 
    l.postcode,
    ROUND(AVG(f.past_day_temperature_delta), 2) AS avg_instability_index,
    COUNT(DISTINCT f.dim_weather_type_key_midday) AS unique_weather_types_seen,
    MAX(f.past_day_temperature_delta) AS peak_temperature_swing
FROM FACT_WEATHER_FORECAST f
JOIN DIM_LOCATION l ON f.dim_location_key = l.location_key
GROUP BY l.postcode
HAVING COUNT(*) >= 4 
ORDER BY avg_instability_index DESC;

-- Graf 4 Morning Fogs Possibility

SELECT 
    l.postcode, 
    d.date,
    f.visibility_midnight,
    f.wind_speed_midnight,
    CASE 
        WHEN f.visibility_midnight < 500 AND wd.wind_speed_category = 'Light' 
            THEN 2 -- High Risk
        
        WHEN f.visibility_midnight < 1000 
            THEN 1 -- Moderate Risk
            
        ELSE 0 
    END AS fog_probability_pct
FROM QUAIL_DB.UKF_PROJECT.FACT_WEATHER_FORECAST f
JOIN DIM_LOCATION l ON f.dim_location_key = l.location_key
JOIN DIM_DATE d ON f.dim_date_key = d.date_key
JOIN QUAIL_DB.UKF_PROJECT.DIM_WIND_TYPE wd ON wd.wind_type_key = f.dim_wind_type_key_midnight
WHERE f.visibility_midnight < 1000;

-- Graf 5 Night And Day Temperature Voliatility

SELECT 
    l.postcode, 
    d.date,
    f.max_temperature_day,
    f.min_temperature_night,
    (f.max_temperature_day - f.min_temperature_night) AS temp_amplitude
FROM FACT_WEATHER_FORECAST f
JOIN DIM_LOCATION l ON f.dim_location_key = l.location_key
JOIN DIM_DATE d ON f.dim_date_key = d.date_key;

-- Graf 6 Visibility by region

SELECT 
    d.date, 
    l.postcode, 
    ROUND(AVG(f.visibility_midday), 0) as avg_visibility_meters
FROM FACT_WEATHER_FORECAST f
JOIN DIM_LOCATION l ON f.dim_location_key = l.location_key
JOIN DIM_DATE d ON f.dim_date_key = d.date_key
GROUP BY 1, 2
ORDER BY 1 DESC, 3 ASC;

-- Graf 7 Comfortable temperature during the day

SELECT 
    d.date, 
    ROUND(AVG(f.max_temperature_day - f.max_feels_like_temperature_day), 1) as daily_chill_factor
FROM FACT_WEATHER_FORECAST f
JOIN DIM_DATE d ON f.dim_date_key = d.date_key
GROUP BY d.date
ORDER BY d.date ASC;

-- Graf 8 Analysis of sudden temperature changes by region and date

SELECT 
    TO_TIMESTAMP(d.date) as date_x, 
    l.postcode, 
    f.avg_temperature_day, 
    f.past_day_temperature_delta
FROM FACT_WEATHER_FORECAST f
JOIN DIM_LOCATION l ON f.dim_location_key = l.location_key
JOIN DIM_DATE d ON f.dim_date_key = d.date_key
WHERE f.past_day_temperature_delta > 3.5
ORDER BY d.date ASC;

-- Graf 9 Analysis of weather changes throughout the day

SELECT 
    d.date, 
    AVG(CASE 
        WHEN dwd.weather_description = dwn.weather_description THEN 1 
        ELSE 2 
    END) AS daily_instability_index,
    MODE(dwd.weather_description) AS main_day_weather
FROM FACT_WEATHER_FORECAST f
JOIN DIM_DATE d ON f.dim_date_key = d.date_key
JOIN DIM_WEATHER_TYPE dwd ON f.dim_weather_type_key_midday = dwd.weather_type_key
JOIN DIM_WEATHER_TYPE dwn ON f.dim_weather_type_key_midnight = dwn.weather_type_key
GROUP BY d.date
ORDER BY d.date ASC;

-- Graf 10 Energy consumption by region (HDD)

WITH stats AS (
    SELECT AVG(18 - max_temperature_day) as avg_hdd
    FROM FACT_WEATHER_FORECAST
)
SELECT 
    l.postcode,
    d.date,
    CASE 
        WHEN f.max_temperature_day < 18 THEN (18 - f.max_temperature_day)
        ELSE 0 
    END AS heating_degree_days,
    f.past_day_temperature_delta
FROM FACT_WEATHER_FORECAST f
JOIN DIM_DATE d ON f.dim_date_key = d.date_key
JOIN DIM_LOCATION l ON f.dim_location_key = l.location_key
ORDER BY heating_degree_days DESC;


