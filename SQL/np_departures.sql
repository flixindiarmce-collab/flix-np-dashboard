-- np_departures.sql
-- Forward-looking departure counts from bus_inventory (D0-D30).
-- Single source: bus_inventory covers D0-D30 in one table; 24h staleness on D0-D4 acceptable for NP planning.
-- Dedupe: latest scrape per (relation_name, departure_date, service_id, departure_time) — each bus counted once.
-- Note: bus_inventory has NO line_number and NO bus_product_type. Use service_id as line analog.
-- Product type derived from is_seater/is_sleeper booleans + LOWER(bus_type) LIKE '%volvo%'.
-- API injects:
--   {pbd_lower}, {pbd_upper}   integer PBD bounds (0,4 | 5,30 | 0,30)
--   {origin_filter}            "" or "AND STARTS_WITH(LOWER(relation_name), LOWER('Delhi'))"
--   {corridor_filter}          "" or "AND relation_name = 'Delhi → Lucknow'"
--   {line_filter}              "" or "AND service_id = 'XYZ'"
--   {product_filter}           "" or "AND ((is_seater=TRUE AND is_sleeper=FALSE) OR ...)"
--   {operator_filter}          "AND (LOWER(travels_name) LIKE '%flix%' OR ...)"

WITH base AS (
  SELECT
    scrape_timestamp,
    relation_name,
    PARSE_DATE('%d-%b-%Y', departure_date)                                AS departure_date,
    departure_time,
    service_id,
    travels_name,
    bus_type,
    is_seater,
    is_sleeper,
    is_ac,
    SAFE_CAST(available_seats AS INT64)                                   AS available_seats,
    total_seats,
    DATE_DIFF(PARSE_DATE('%d-%b-%Y', departure_date), CURRENT_DATE('Asia/Kolkata'), DAY) AS pbd,
    ROW_NUMBER() OVER (
      PARTITION BY relation_name, departure_date, service_id, departure_time
      ORDER BY scrape_timestamp DESC
    ) AS rn
  FROM `redbus-agent-490708.redbus.bus_inventory`
  WHERE PARSE_DATE('%d-%b-%Y', departure_date)
          BETWEEN DATE_ADD(CURRENT_DATE('Asia/Kolkata'), INTERVAL {pbd_lower} DAY)
              AND DATE_ADD(CURRENT_DATE('Asia/Kolkata'), INTERVAL {pbd_upper} DAY)
    {origin_filter}
    {corridor_filter}
    {line_filter}
    {product_filter}
),

dedup AS (
  SELECT * FROM base WHERE rn = 1
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
         ELSE NULL END                                                                       AS operator,
    CASE WHEN departure_time IS NULL THEN 'unknown'
         WHEN SAFE_CAST(SUBSTR(departure_time, 1, 2) AS INT64) BETWEEN 0  AND 5  THEN '00:00-05:59'
         WHEN SAFE_CAST(SUBSTR(departure_time, 1, 2) AS INT64) BETWEEN 6  AND 11 THEN '06:00-11:59'
         WHEN SAFE_CAST(SUBSTR(departure_time, 1, 2) AS INT64) BETWEEN 12 AND 17 THEN '12:00-17:59'
         WHEN SAFE_CAST(SUBSTR(departure_time, 1, 2) AS INT64) BETWEEN 18 AND 23 THEN '18:00-23:59'
         ELSE 'unknown' END                                                                  AS hour_band
  FROM dedup
),

scoped AS (
  SELECT * FROM flagged WHERE operator IS NOT NULL
)

-- Three result sets the API will return as separate queries / one query with multiple selects.
-- by_operator_hourband
SELECT
  operator,
  hour_band,
  COUNT(*)                                                AS departure_count,
  SUM(total_seats)                                        AS seats_offered,
  ROUND(AVG(SAFE_DIVIDE(total_seats - available_seats, total_seats)) * 100, 1) AS avg_load_pct
FROM scoped
GROUP BY operator, hour_band
ORDER BY operator, hour_band;
