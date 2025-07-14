WITH airports_reorder AS (
    SELECT faa
    	   ,country
    	   ,region
           ,name
           ,city
           ,lat
           ,lon
           ,alt
           ,tz
           ,dst
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder