WITH flights_cleaned AS (
    SELECT * 
    FROM {{ ref('prep_flights') }}
),

airport_stats AS (
    SELECT
        faa,
        COUNT(DISTINCT CASE WHEN origin IS NOT NULL THEN dest END) AS unique_departure_connections,
        COUNT(DISTINCT CASE WHEN dest IS NOT NULL THEN origin END) AS unique_arrival_connections,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS total_actual_flights,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines
    FROM flights_cleaned
    GROUP BY faa
),

airport_info AS (
    SELECT faa, name, city, country
    FROM {{ ref('prep_airports') }}
)

SELECT 
    a.faa,
    info.name,
    info.city,
    info.country,
    a.unique_departure_connections,
    a.unique_arrival_connections,
    a.total_flights,
    a.total_cancelled,
    a.total_diverted,
    a.total_actual_flights,
    a.unique_airplanes,
    a.unique_airlines
FROM airport_stats a
LEFT JOIN airport_info info ON a.faa = info.faa
