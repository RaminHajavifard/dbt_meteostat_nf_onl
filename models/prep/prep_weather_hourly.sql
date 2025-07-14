WITH hourly_data AS (
    SELECT * 
    FROM {{ ref('staging_weather_hourly') }}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date                                 -- Extract date part only (YYYY-MM-DD)
        , timestamp::TIME AS time                                 -- Extract time part only (HH:MI:SS)
        , TO_CHAR(timestamp, 'HH24:MI') AS hour                   -- Hour and minute as string (e.g., '15:30')
        , TO_CHAR(timestamp, 'FMMonth') AS month_name            -- Month name (e.g., 'July'), FM removes padding
        , TO_CHAR(timestamp, 'FMDay') AS weekday                 -- Weekday name (e.g., 'Monday'), FM removes padding
        , DATE_PART('day', timestamp) AS date_day                -- Numeric day of the month (1–31)
        , DATE_PART('month', timestamp) AS date_month            -- Numeric month (1–12)
        , DATE_PART('year', timestamp) AS date_year              -- Year (e.g., 2025)
        , DATE_PART('week', timestamp) AS cw                     -- Calendar week (1–53)
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        , CASE 
            WHEN time BETWEEN TIME '00:00:00' AND TIME '05:59:59' THEN 'night'
            WHEN time BETWEEN TIME '06:00:00' AND TIME '17:59:59' THEN 'day'
            WHEN time BETWEEN TIME '18:00:00' AND TIME '23:59:59' THEN 'evening'
          END AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features
