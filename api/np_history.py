"""
/api/np-history
  GET — backward-looking departure counts from bus_inventory.

Query params:
  view          wow | trend_45 | mom         default: wow
  origin_hub    City name                    optional
  corridor      Full relation_name           optional
  line_code     Line code                    optional
  product_type  Comma-separated              optional
  operators     Comma-separated              optional

Logic:
  For each historical departure_date, pick the LATEST scrape_timestamp.
  This makes the metric resilient to the 2026-04-01 crawl-volume regime change
  (we always count distinct buses, never raw rows).

  Excludes partial-crawl scrape_dates (n < 150K rows OR forward span < 29 days).

Views:
  - wow:       this week (last 7 days) vs prior week, by operator
  - trend_45:  daily departure counts per operator over past 45 days
  - mom:       returns placeholder until 2026-06-01
"""
import json
import os
from collections import defaultdict
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs

from google.cloud import bigquery
from google.oauth2 import service_account


PROJECT = "redbus-agent-490708"
SCOPES  = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
]

OPERATOR_PATTERNS = {
    "flix":     "LOWER(travels_name) LIKE '%flix%'",
    "intrcity": "LOWER(travels_name) LIKE '%intrcity%'",
    "zingbus":  "(LOWER(travels_name) LIKE '%zingbus%' AND LOWER(travels_name) NOT LIKE '%maxx%')",
    "nuego":    "LOWER(travels_name) LIKE '%nuego%'",
    "freshbus": "LOWER(travels_name) LIKE '%freshbus%'",
    "laxmi":    "(LOWER(travels_name) LIKE '%laxmi holidays%' AND LOWER(travels_name) NOT LIKE '%pvt%')",
}

ROUTE_CORRIDOR = {
    'delhi to chandigarh':'IN01','chandigarh to delhi':'IN01',
    'delhi to shimla':'IN01','shimla to delhi':'IN01',
    'delhi to dehradun':'IN01','dehradun to delhi':'IN01',
    'delhi to manali':'IN01','manali to delhi':'IN01',
    'delhi to lucknow':'IN02','lucknow to delhi':'IN02',
    'delhi to indore':'IN03','indore to delhi':'IN03',
    'delhi to ujjain':'IN03','ujjain to delhi':'IN03',
    'delhi to jaipur':'IN04','jaipur to delhi':'IN04',
    'mumbai to ujjain':'IN05','ujjain to mumbai':'IN05',
    'pune to ujjain':'IN05','ujjain to pune':'IN05',
    'mumbai to indore':'IN05','indore to mumbai':'IN05',
    'indore to ujjain':'IN05','ujjain to indore':'IN05',
    'indore to pune':'IN05','pune to indore':'IN05',
    'hyderabad to pune':'IN06','pune to hyderabad':'IN06',
    'pune to goa':'IN07','goa to pune':'IN07',
    'mumbai to goa':'IN07','goa to mumbai':'IN07',
    'bangalore to hyderabad':'IN09','hyderabad to bangalore':'IN09',
    'bangalore to vijayawada':'IN09','vijayawada to bangalore':'IN09',
    'hyderabad to vijayawada':'IN09','vijayawada to hyderabad':'IN09',
    'hyderabad to visakhapatnam':'IN09','visakhapatnam to hyderabad':'IN09',
    'hyderabad to nellore':'IN09','nellore to hyderabad':'IN09',
    'hyderabad to tirupati':'IN09','tirupati to hyderabad':'IN09',
    'chennai to hyderabad':'IN10','hyderabad to chennai':'IN10',
    'chennai to vijayawada':'IN10','vijayawada to chennai':'IN10',
    'bangalore to chennai':'IN11','chennai to bangalore':'IN11',
    'bangalore to coimbatore':'IN11','coimbatore to bangalore':'IN11',
    'bangalore to kochi':'IN11','kochi to bangalore':'IN11',
    'chennai to coimbatore':'IN11','coimbatore to chennai':'IN11',
    'chennai to madurai':'IN11','madurai to chennai':'IN11',
    'chennai to nagercoil':'IN11','nagercoil to chennai':'IN11',
    'bangalore to belgaum':'IN12','belgaum to bangalore':'IN12',
    'bangalore to goa':'IN12','goa to bangalore':'IN12',
    'bangalore to pune':'IN12','pune to bangalore':'IN12',
    'bangalore to kundapur':'IN12','kundapur to bangalore':'IN12',
    'bangalore to mangaluru':'IN12','mangaluru to bangalore':'IN12',
    'bangalore to udupi':'IN12','udupi to bangalore':'IN12',
}


def _region_to_relations(region: str) -> list[str]:
    region = (region or "").strip().upper()
    return [k for k, v in ROUTE_CORRIDOR.items() if v == region]


PRODUCT_TYPES_ALL = {"Seater", "Sleeper", "Hybrid", "Volvo"}

PRODUCT_TYPE_CLAUSES = {
    "Seater":  "(is_seater = TRUE  AND is_sleeper = FALSE)",
    "Sleeper": "(is_sleeper = TRUE AND is_seater = FALSE)",
    "Hybrid":  "(is_seater = TRUE  AND is_sleeper = TRUE)",
    "Volvo":   "LOWER(bus_type) LIKE '%volvo%'",
}

MIN_ROWS_PER_DAY = 150_000
MIN_FORWARD_DAYS = 29


def _bq_client():
    sa_info = json.loads(os.environ["SA_CREDENTIALS_JSON"])
    creds   = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return bigquery.Client(project=PROJECT, credentials=creds)


def _sql_string_escape(s: str) -> str:
    return s.replace("'", "''")


def _build_filter_clauses(params: dict) -> tuple[str, dict]:
    """Build the operator/product/dimension filter SQL fragments."""
    op_keys = params.get("operators", "").split(",") if params.get("operators") else list(OPERATOR_PATTERNS.keys())
    op_keys = [k.strip() for k in op_keys if k.strip() in OPERATOR_PATTERNS]
    if not op_keys:
        op_keys = list(OPERATOR_PATTERNS.keys())
    operator_filter = " OR ".join(OPERATOR_PATTERNS[k] for k in op_keys)

    products_raw = params.get("product_type", "").split(",") if params.get("product_type") else list(PRODUCT_TYPES_ALL)
    products = [p.strip() for p in products_raw if p.strip() in PRODUCT_TYPES_ALL]
    if not products or set(products) == PRODUCT_TYPES_ALL:
        product_filter = ""
    else:
        product_filter = "AND (" + " OR ".join(PRODUCT_TYPE_CLAUSES[p] for p in products) + ")"

    extra_filters = []
    if params.get("relation"):
        extra_filters.append(f"AND relation_name = '{_sql_string_escape(params['relation'])}'")
    elif params.get("corridor"):
        extra_filters.append(f"AND relation_name = '{_sql_string_escape(params['corridor'])}'")
    elif params.get("region"):
        relations = _region_to_relations(params["region"])
        if relations:
            relation_list = ",".join(f"'{_sql_string_escape(r)}'" for r in relations)
            extra_filters.append(f"AND LOWER(relation_name) IN ({relation_list.lower()})")
    if params.get("line_code"):
        extra_filters.append(f"AND service_id = '{_sql_string_escape(params['line_code'])}'")

    applied = {
        "operators":    op_keys,
        "product_type": products,
        "corridor":     params.get("corridor"),
        "origin_hub":   params.get("origin_hub"),
        "line_code":    params.get("line_code"),
    }
    return operator_filter, product_filter, " ".join(extra_filters), applied


def _build_trend_sql(history_days: int, op_filter: str, prod_filter: str, extra_filter: str) -> str:
    """Daily departure counts per operator across past N days, using latest-scrape-per-bus dedupe."""
    return f"""
        WITH clean_days AS (
          SELECT scrape_date
          FROM (
            SELECT
              DATE(scrape_timestamp, 'Asia/Kolkata') AS scrape_date,
              COUNT(*)                               AS n,
              DATE_DIFF(
                MAX(PARSE_DATE('%d-%b-%Y', departure_date)),
                MIN(PARSE_DATE('%d-%b-%Y', departure_date)),
                DAY
              )                                      AS forward_span
            FROM `redbus-agent-490708.redbus.bus_inventory`
            WHERE scrape_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ({history_days} + 15) DAY)
            GROUP BY scrape_date
          )
          WHERE n >= {MIN_ROWS_PER_DAY} AND forward_span >= {MIN_FORWARD_DAYS}
        ),
        base AS (
          SELECT
            scrape_timestamp,
            relation_name,
            PARSE_DATE('%d-%b-%Y', departure_date)  AS departure_date,
            departure_time,
            service_id,
            travels_name,
            bus_type,
            is_seater,
            is_sleeper
          FROM `redbus-agent-490708.redbus.bus_inventory`
          WHERE DATE(scrape_timestamp, 'Asia/Kolkata') IN (SELECT scrape_date FROM clean_days)
            AND ({op_filter})
            {prod_filter}
            {extra_filter}
            AND PARSE_DATE('%d-%b-%Y', departure_date)
                  BETWEEN DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL {history_days} DAY)
                      AND DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 1 DAY)
        ),
        dedup AS (
          SELECT * FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY relation_name, departure_date, service_id, departure_time
                ORDER BY scrape_timestamp DESC
              ) AS rn
            FROM base
          )
          WHERE rn = 1
        )
        SELECT
          departure_date,
          CASE WHEN LOWER(travels_name) LIKE '%flix%'                                            THEN 'flix'
               WHEN LOWER(travels_name) LIKE '%intrcity%'                                        THEN 'intrcity'
               WHEN LOWER(travels_name) LIKE '%zingbus%' AND LOWER(travels_name) NOT LIKE '%maxx%' THEN 'zingbus'
               WHEN LOWER(travels_name) LIKE '%nuego%'                                           THEN 'nuego'
               WHEN LOWER(travels_name) LIKE '%freshbus%'                                        THEN 'freshbus'
               WHEN LOWER(travels_name) LIKE '%laxmi holidays%' AND LOWER(travels_name) NOT LIKE '%pvt%' THEN 'laxmi'
               ELSE 'other' END                                                                    AS operator,
          COUNT(*) AS departure_count
        FROM dedup
        GROUP BY departure_date, operator
        ORDER BY departure_date, operator
    """


def _do_trend_45(params: dict, applied: dict, op_filter: str, prod_filter: str, extra_filter: str):
    sql = _build_trend_sql(45, op_filter, prod_filter, extra_filter)
    rows = [dict(r) for r in _bq_client().query(sql).result()]
    points = [
        {"departure_date": r["departure_date"].isoformat(), "operator": r["operator"], "departure_count": r["departure_count"]}
        for r in rows if r["operator"] != "other"
    ]
    return {
        "ok":              True,
        "view":            "trend_45",
        "filters_applied": applied,
        "points":          points,
    }


def _do_wow(params: dict, applied: dict, op_filter: str, prod_filter: str, extra_filter: str):
    sql = _build_trend_sql(14, op_filter, prod_filter, extra_filter)
    rows = [dict(r) for r in _bq_client().query(sql).result()]

    today        = date.today()
    cur_start    = today - timedelta(days=7)
    cur_end      = today - timedelta(days=1)
    prior_start  = today - timedelta(days=14)
    prior_end    = today - timedelta(days=8)

    cur = defaultdict(int)
    prior = defaultdict(int)
    for r in rows:
        if r["operator"] == "other":
            continue
        d = r["departure_date"]
        n = r["departure_count"]
        if cur_start <= d <= cur_end:
            cur[r["operator"]] += n
        elif prior_start <= d <= prior_end:
            prior[r["operator"]] += n

    operators = sorted(set(cur) | set(prior))
    delta = {}
    for op in operators:
        c, p = cur.get(op, 0), prior.get(op, 0)
        delta[op] = {
            "current":   c,
            "prior":     p,
            "abs_delta": c - p,
            "pct_delta": round(((c - p) / p) * 100, 1) if p else None,
        }

    return {
        "ok":              True,
        "view":            "wow",
        "filters_applied": applied,
        "current_week":    {"start": cur_start.isoformat(),   "end": cur_end.isoformat(),   "by_operator": dict(cur)},
        "prior_week":      {"start": prior_start.isoformat(), "end": prior_end.isoformat(), "by_operator": dict(prior)},
        "delta":           delta,
    }


def _do_mom_placeholder():
    return {
        "ok":      True,
        "view":    "mom",
        "stub":    True,
        "message": "MoM requires ≥2 months of clean data. Available from 2026-06 onwards.",
    }


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        qs     = parse_qs(self.path.split("?", 1)[-1]) if "?" in self.path else {}
        params = {k: v[0] for k, v in qs.items()}
        view   = params.get("view", "wow")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "private, max-age=60")
        self.end_headers()

        try:
            op_filter, prod_filter, extra_filter, applied = _build_filter_clauses(params)

            if view == "trend_45":
                result = _do_trend_45(params, applied, op_filter, prod_filter, extra_filter)
            elif view == "mom":
                result = _do_mom_placeholder()
            else:
                result = _do_wow(params, applied, op_filter, prod_filter, extra_filter)

            self.wfile.write(json.dumps(result, default=str).encode())

        except Exception as exc:
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode())

    def log_message(self, *_):
        pass
