WITH trips AS (

SELECT
    ride_id,
    DATE(TRY_TO_TIMESTAMP(started_at)) AS trip_date,
    start_statio_id AS start_station_id,
    end_station_id,
    member_csual AS member_casual,
    TIMESTAMPDIFF(
        SECOND,
        TRY_TO_TIMESTAMP(started_at),
        TRY_TO_TIMESTAMP(ended_at)
    ) AS trip_duration_seconds

FROM {{ ref('stg_bike') }}

WHERE TRY_TO_TIMESTAMP(started_at) IS NOT NULL
  AND TRY_TO_TIMESTAMP(ended_at) IS NOT NULL

)

SELECT *
FROM trips