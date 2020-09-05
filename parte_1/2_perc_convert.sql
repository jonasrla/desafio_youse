SELECT
  city_name,
  sign_up_day,
  SUM(CASE WHEN DATE_PART('DAY',
                          interval_sign_up_trip)*24 +
                DATE_PART('hour',
                          interval_sign_up_trip) < 168
           THEN 1
      ELSE 0) / COUNT(*) conversion_rate
FROM (
  SELECT
    client_id,
    city_name,
    EXTRACT(DAY FROM TIMESTAMP sign_up_ts) sign_up_day,
    AGE(sign_up_ts, COALESCE(first_request_at, NOW())) interval_sign_ up_trip
  FROM
    (SELECT
       city_id,
       city_name
     FROM
       cities
     WHERE
       city_name IN ('Sao Paulo', 'Campinas')
    ) valid_cities

  LEFT JOIN
    (SELECT
       rider_id,
       city_id,
       _ts sign_up_ts
     FROM
       events
     WHERE
       event_name = 'sign_up_success'
       _ts BETWEEN '2018-01-01 00:00:00'::timestamp AND '2018-01-07 23:59:59'::timestamp) sign_up_events
  ON valid_cities.city_id = sign_up_events.city_id

  LEFT JOIN
    (SELECT
       client_id,
       MIN(request_at) first_request_at
     FROM
       TRIPS
     GROUP BY
       client_id
   ) first_trip
    ON
      sign_up_events.rider_id = first_trip.client_id
)
GROUP BY city_name, sign_up_day
