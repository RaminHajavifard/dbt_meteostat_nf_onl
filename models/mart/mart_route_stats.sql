WITH flights_cleaned AS (
    SELECT * 
    FROM {{ ref('prep_flights') }}
),

route_stats AS (
    SELECT 
        origin,
        dest,
        COUNT(*) AS total_flights,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines,
        ROUND(AVG(actual_elapsed_time), 2) AS avg_elapsed_time,
        ROUND(AVG(arr_delay), 2) AS avg_arrival_delay,
        MAX(arr_delay) AS max_delay,
        MIN(arr_delay) AS min_delay,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted
    FROM flights_cleaned
    GROUP BY origin, dest
),

airport_info AS (
    SELECT faa, name, city, country
    FROM {{ ref('prep_airports') }}
)

SELECT 
    rs.origin,
    origin_airport.name AS origin_name,
    origin_airport.city AS origin_city,
    origin_airport.country AS origin_country,

    rs.dest,
    dest_airport.name AS dest_name,
    dest_airport.city AS dest_city,
    dest_airport.country AS dest_country,

    rs.total_flights,
    rs.unique_airplanes,
    rs.unique_airlines,
    rs.avg_elapsed_time,
    rs.avg_arrival_delay,
    rs.max_delay,
    rs.min_delay,
    rs.total_cancelled,
    rs.total_diverted

FROM route_stats rs
LEFT JOIN airport_info origin_airport ON rs.origin = origin_airport.faa
LEFT JOIN airport_info dest_airport ON rs.dest = dest_airport.faa
