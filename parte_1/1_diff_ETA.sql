SELECT
  AVG(predicted_eta - actual_eta) diferenca_media,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY predicted_eta - actual_eta) diferenca_mediana,
  AVG(ABS(predicted_eta - actual_eta)) erro_medio
FROM
  Trips
LEFT JOIN
  cities
ON Trips.city_id = cities.city_id
WHERE
  city_name in ('Sao Paulo', 'Campinas')
  DATE_PART('day', request_at - current_timestamp) < 30
