"""
/api/np-departures
  GET — forward-looking departure counts from bus_inventory (D0-D30).

Query params:
  pbd_range     d0-d4 | d5-d30 | d0-d30        default: d0-d30
  origin_hub    City name (origin of relation_name)      optional
  corridor      Full relation_name                       optional
  line_code     Line code (line_number)                  optional
  product_type  Comma-separated: Seater,Sleeper,Hybrid,Volvo  optional (default: all)
  operators     Comma-separated: flix,intrcity,zingbus,nuego,freshbus,laxmi  optional (default: all)

Returns:
  {
    "ok":             true,
    "filters_applied": {...},
    "pbd_range":      [lower, upper],
    "by_operator_hourband": [{operator, hour_band, departure_count, seats_offered, avg_load_pct}],
    "by_day":               [{date, operator, departure_count}],
    "by_line":              [{line_code, corridor, operator, departure_count, departure_times}]
  }
"""
import json
import os
from collections import defaultdict
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs

from google.cloud import bigquery
from google.oauth2 import service_account


PROJECT = "redbus-agent-490708"
SCOPES  = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
]

PBD_RANGES = {
    "d0-d4":  (0, 4),
    "d5-d30": (5, 30),
    "d0-d30": (0, 30),
}

OPERATOR_PATTERNS = {
    "flix":     "LOWER(travels_name) LIKE '%flix%'",
    "intrcity": "LOWER(travels_name) LIKE '%intrcity%'",
    "zingbus":  "(LOWER(travels_name) LIKE '%zingbus%' AND LOWER(travels_name) NOT LIKE '%maxx%')",
    "nuego":    "LOWER(travels_name) LIKE '%nuego%'",
    "freshbus": "LOWER(travels_name) LIKE '%freshbus%'",
    "laxmi":    "(LOWER(travels_name) LIKE '%laxmi holidays%' AND LOWER(travels_name) NOT LIKE '%pvt%')",
}

PRODUCT_TYPES_DEFAULT = ["Seater", "Sleeper", "Hybrid", "Volvo"]


def _bq_client():
    sa_info = json.loads(os.environ["SA_CREDENTIALS_JSON"])
    creds   = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return bigquery.Client(project=PROJECT, credentials=creds)


def _sql_string_escape(s: str) -> str:
    return s.replace("'", "''")


def _build_query(params: dict) -> tuple[str, dict]:
    pbd_range = params.get("pbd_range", "d0-d30")
    if pbd_range not in PBD_RANGES:
        pbd_range = "d0-d30"
    pbd_lower, pbd_upper = PBD_RANGES[pbd_range]

    # Operator filter — default to all known operators
    op_keys = params.get("operators", "").split(",") if params.get("operators") else list(OPERATOR_PATTERNS.keys())
    op_keys = [k.strip() for k in op_keys if k.strip() in OPERATOR_PATTERNS]
    if not op_keys:
        op_keys = list(OPERATOR_PATTERNS.keys())
    operator_filter = " OR ".join(OPERATOR_PATTERNS[k] for k in op_keys)

    # Product type filter
    products = params.get("product_type", "").split(",") if params.get("product_type") else PRODUCT_TYPES_DEFAULT
    products = [_sql_string_escape(p.strip()) for p in products if p.strip()]
    product_filter = ""
    if products:
        product_list = ",".join(f"'{p}'" for p in products)
        product_filter = f"AND bus_product_type IN ({product_list})"

    # Optional dimensional filters
    extra_filters = []
    if params.get("corridor"):
        extra_filters.append(f"AND relation_name = '{_sql_string_escape(params['corridor'])}'")
    elif params.get("origin_hub"):
        hub = _sql_string_escape(params["origin_hub"])
        extra_filters.append(f"AND STARTS_WITH(LOWER(relation_name), LOWER('{hub}'))")
    if params.get("line_code"):
        extra_filters.append(f"AND line_number = '{_sql_string_escape(params['line_code'])}'")
    extra_filter = " ".join(extra_filters)

    sql = f"""
        WITH base AS (
          SELECT
            scrape_timestamp,
            relation_name,
            PARSE_DATE('%d-%b-%Y', departure_date)                                AS departure_date,
            departure_time,
            service_id,
            line_number,
            travels_name,
            bus_product_type,
            SAFE_CAST(available_seats AS INT64)                                   AS available_seats,
            SAFE_CAST(total_seats AS INT64)                                       AS total_seats,
            ROW_NUMBER() OVER (
              PARTITION BY relation_name, departure_date, service_id, departure_time
              ORDER BY scrape_timestamp DESC
            ) AS rn
          FROM `redbus-agent-490708.redbus.bus_inventory`
          WHERE PARSE_DATE('%d-%b-%Y', departure_date)
                  BETWEEN DATE_ADD(CURRENT_DATE('Asia/Kolkata'), INTERVAL {pbd_lower} DAY)
                      AND DATE_ADD(CURRENT_DATE('Asia/Kolkata'), INTERVAL {pbd_upper} DAY)
            AND ({operator_filter})
            {product_filter}
            {extra_filter}
        )
        SELECT
          relation_name,
          departure_date,
          departure_time,
          line_number,
          travels_name,
          bus_product_type,
          available_seats,
          total_seats,
          CASE WHEN LOWER(travels_name) LIKE '%flix%'                                            THEN 'flix'
               WHEN LOWER(travels_name) LIKE '%intrcity%'                                        THEN 'intrcity'
               WHEN LOWER(travels_name) LIKE '%zingbus%' AND LOWER(travels_name) NOT LIKE '%maxx%' THEN 'zingbus'
               WHEN LOWER(travels_name) LIKE '%nuego%'                                           THEN 'nuego'
               WHEN LOWER(travels_name) LIKE '%freshbus%'                                        THEN 'freshbus'
               WHEN LOWER(travels_name) LIKE '%laxmi holidays%' AND LOWER(travels_name) NOT LIKE '%pvt%' THEN 'laxmi'
               ELSE 'other' END                                                                    AS operator,
          CASE WHEN departure_time IS NULL THEN 'unknown'
               WHEN SAFE_CAST(SUBSTR(departure_time, 1, 2) AS INT64) BETWEEN 0  AND 5  THEN '00:00-05:59'
               WHEN SAFE_CAST(SUBSTR(departure_time, 1, 2) AS INT64) BETWEEN 6  AND 11 THEN '06:00-11:59'
               WHEN SAFE_CAST(SUBSTR(departure_time, 1, 2) AS INT64) BETWEEN 12 AND 17 THEN '12:00-17:59'
               WHEN SAFE_CAST(SUBSTR(departure_time, 1, 2) AS INT64) BETWEEN 18 AND 23 THEN '18:00-23:59'
               ELSE 'unknown' END                                                                  AS hour_band
        FROM base
        WHERE rn = 1
    """

    applied = {
        "pbd_range":    pbd_range,
        "operators":    op_keys,
        "product_type": products,
        "corridor":     params.get("corridor"),
        "origin_hub":   params.get("origin_hub"),
        "line_code":    params.get("line_code"),
    }
    return sql, applied


def _aggregate(rows):
    """Build three aggregations from a single result set, no pandas needed."""
    by_oh = defaultdict(lambda: {"count": 0, "seats": 0, "load_sum": 0.0, "load_n": 0})
    by_day = defaultdict(int)
    by_line = defaultdict(lambda: {"count": 0, "times": []})

    for r in rows:
        op = r["operator"]
        if op == "other":
            continue

        # by_operator_hourband
        k1 = (op, r["hour_band"])
        by_oh[k1]["count"] += 1
        if r["total_seats"]:
            by_oh[k1]["seats"] += int(r["total_seats"])
            if r["available_seats"] is not None and r["total_seats"]:
                load = 1.0 - (r["available_seats"] / r["total_seats"])
                by_oh[k1]["load_sum"] += load
                by_oh[k1]["load_n"]   += 1

        # by_day
        by_day[(r["departure_date"].isoformat(), op)] += 1

        # by_line
        line  = r["line_number"] or "(no-line)"
        k3    = (line, r["relation_name"], op)
        by_line[k3]["count"] += 1
        if r["departure_time"]:
            by_line[k3]["times"].append(r["departure_time"])

    by_operator_hourband = [
        {
            "operator":        op,
            "hour_band":       hb,
            "departure_count": v["count"],
            "seats_offered":   v["seats"],
            "avg_load_pct":    round((v["load_sum"] / v["load_n"]) * 100, 1) if v["load_n"] else None,
        }
        for (op, hb), v in sorted(by_oh.items())
    ]

    by_day_out = [
        {"date": d, "operator": op, "departure_count": n}
        for (d, op), n in sorted(by_day.items())
    ]

    by_line_out = [
        {
            "line_code":        line,
            "corridor":         corridor,
            "operator":         op,
            "departure_count":  v["count"],
            "departure_times":  sorted(set(v["times"])),
        }
        for (line, corridor, op), v in sorted(by_line.items())
    ]

    return by_operator_hourband, by_day_out, by_line_out


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        qs     = parse_qs(self.path.split("?", 1)[-1]) if "?" in self.path else {}
        params = {k: v[0] for k, v in qs.items()}

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=3600")
        self.end_headers()

        try:
            sql, applied = _build_query(params)
            client = _bq_client()
            rows   = [dict(r) for r in client.query(sql).result()]

            by_oh, by_day, by_line = _aggregate(rows)

            self.wfile.write(json.dumps({
                "ok":                    True,
                "filters_applied":       applied,
                "row_count_raw":         len(rows),
                "by_operator_hourband":  by_oh,
                "by_day":                by_day,
                "by_line":               by_line,
            }, default=str).encode())

        except Exception as exc:
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode())

    def log_message(self, *_):
        pass
