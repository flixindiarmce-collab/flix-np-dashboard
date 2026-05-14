-- np_history.sql
-- Backward-looking departure counts from bus_inventory.
-- For each historical departure_date, pick the LATEST scrape (resilient to 2026-04-01 regime change).
-- Excludes partial-crawl scrape_dates (< 150K rows OR forward span < 29 days).
-- API injects:
--   {origin_filter}, {corridor_filter}, {line_filter}, {product_filter}, {operator_filter}
--   {history_days}              45 for trend, 14 for wow, etc.

WITH clean_days AS (
  SELECT DATE(scrape_timestamp, 'Asia/Kolkata') AS scrape_date
  FROM (
    SELECT
      scrape_timestamp,
      COUNT(*) OVER (PARTITION BY DATE(scrape_timestamp, 'Asia/Kolkata')) AS day_n,
      DATE_DIFF(
        MAX(PARSE_DATE('%d-%b-%Y', departure_date)) OVER (PARTITION BY DATE(scrape_timestamp, 'Asia/Kolkata')),
        MIN(PARSE_DATE('%d-%b-%Y', departure_date)) OVER (PARTITION BY DATE(scrape_timestamp, 'Asia/Kolkata')),
        DAY
      ) AS day_span
    FROM `redbus-agent-490708.redbus.bus_inventory`
    WHERE scrape_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ({history_days} + 15) DAY)
  )
  WHERE day_n >= 150000 AND day_span >= 29
  GROUP BY scrape_date
),

base AS (
  SELECT
    scrape_timestamp,
    DATE(scrape_timestamp, 'Asia/Kolkata') AS scrape_date,
    relation_name,
    PARSE_DATE('%d-%b-%Y', departure_date) AS departure_date,
    departure_time,
    service_id,
    line_number,
    travels_name,
    bus_product_type,
    SAFE_CAST(available_seats AS INT64) AS available_seats,
    total_seats
  FROM `redbus-agent-490708.redbus.bus_inventory`
  WHERE DATE(scrape_timestamp, 'Asia/Kolkata') IN (SELECT scrape_date FROM clean_days)
    {origin_filter}
    {corridor_filter}
    {line_filter}
    {product_filter}
),

-- For each historical departure_date, pick the latest scrape of each bus.
dedup AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY relation_name, departure_date, service_id, departure_time
        ORDER BY scrape_timestamp DESC
      ) AS rn
    FROM base
  )
  WHERE rn = 1
),

flagged AS (
  SELECT
    *,
    CASE WHEN LOWER(travels_name) LIKE '%flix%'                                            THEN 'flix'
         WHEN LOWER(travels_name) LIKE '%intrcity%'                                        THEN 'intrcity'
         WHEN LOWER(travels_name) LIKE '%zingbus%' AND LOWER(travels_name) NOT LIKE '%maxx%' THEN 'zingbus'
         WHEN LOWER(travels_name) LIKE '%nuego%'                                           THEN 'nuego'
         WHEN LOWER(travels_name) LIKE '%freshbus%'                                        THEN 'freshbus'
         WHEN LOWER(travels_name) LIKE '%laxmi holidays%' AND LOWER(travels_name) NOT LIKE '%pvt%' THEN 'laxmi'
         ELSE NULL END                                                                       AS operator
  FROM dedup
)

-- trend_45: daily departure counts per operator over past N days
SELECT
  departure_date,
  operator,
  COUNT(*) AS departure_count
FROM flagged
WHERE operator IS NOT NULL
  AND departure_date BETWEEN DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL {history_days} DAY)
                         AND DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 1 DAY)
GROUP BY departure_date, operator
ORDER BY departure_date, operator;
