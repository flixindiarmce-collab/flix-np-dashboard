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
import re
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
    """Return list of relation_names belonging to a region code (IN01-IN12)."""
    region = (region or "").strip().upper()
    return [k for k, v in ROUTE_CORRIDOR.items() if v == region]


PRODUCT_TYPES_ALL = {"Seater", "Sleeper", "Hybrid", "Volvo"}

PRODUCT_TYPE_CLAUSES = {
    "Seater":  "(is_seater = TRUE  AND is_sleeper = FALSE)",
    "Sleeper": "(is_sleeper = TRUE AND is_seater = FALSE)",
    "Hybrid":  "(is_seater = TRUE  AND is_sleeper = TRUE)",
    "Volvo":   "LOWER(bus_type) LIKE '%volvo%'",
}


def _bq_client():
    sa_info = json.loads(os.environ["SA_CREDENTIALS_JSON"])
    creds   = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return bigquery.Client(project=PROJECT, credentials=creds)


def _sql_string_escape(s: str) -> str:
    return s.replace("'", "''")


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _build_query(params: dict) -> tuple[str, dict]:
    pbd_range = params.get("pbd_range", "d0-d30")
    if pbd_range not in PBD_RANGES:
        pbd_range = "d0-d30"
    pbd_lower, pbd_upper = PBD_RANGES[pbd_range]

    # Explicit dep_from / dep_to override pbd_range
    dep_from = (params.get("dep_from") or "").strip()
    dep_to   = (params.get("dep_to")   or "").strip()
    if _DATE_RE.match(dep_from) and _DATE_RE.match(dep_to):
        date_lower_sql = f"DATE '{dep_from}'"
        date_upper_sql = f"DATE '{dep_to}'"
        applied_date = {"dep_from": dep_from, "dep_to": dep_to}
    else:
        date_lower_sql = f"DATE_ADD(CURRENT_DATE('Asia/Kolkata'), INTERVAL {pbd_lower} DAY)"
        date_upper_sql = f"DATE_ADD(CURRENT_DATE('Asia/Kolkata'), INTERVAL {pbd_upper} DAY)"
        applied_date = {"pbd_range": pbd_range}

    # Operator filter — default to all known operators
    op_keys = params.get("operators", "").split(",") if params.get("operators") else list(OPERATOR_PATTERNS.keys())
    op_keys = [k.strip() for k in op_keys if k.strip() in OPERATOR_PATTERNS]
    if not op_keys:
        op_keys = list(OPERATOR_PATTERNS.keys())
    operator_filter = " OR ".join(OPERATOR_PATTERNS[k] for k in op_keys)

    # Product type filter — derived from is_seater/is_sleeper/bus_type
    products_raw = params.get("product_type", "").split(",") if params.get("product_type") else list(PRODUCT_TYPES_ALL)
    products = [p.strip() for p in products_raw if p.strip() in PRODUCT_TYPES_ALL]
    if not products or set(products) == PRODUCT_TYPES_ALL:
        product_filter = ""
    else:
        product_filter = "AND (" + " OR ".join(PRODUCT_TYPE_CLAUSES[p] for p in products) + ")"

    # Optional dimensional filters
    # `relation` (specific origin->destination) takes priority over `region` (IN01-IN12)
    extra_filters = []
    if params.get("relation"):
        extra_filters.append(f"AND relation_name = '{_sql_string_escape(params['relation'])}'")
    elif params.get("corridor"):  # legacy alias
        extra_filters.append(f"AND relation_name = '{_sql_string_escape(params['corridor'])}'")
    elif params.get("region"):
        relations = _region_to_relations(params["region"])
        if relations:
            relation_list = ",".join(f"'{_sql_string_escape(r)}'" for r in relations)
            extra_filters.append(f"AND LOWER(relation_name) IN ({relation_list.lower()})")
    if params.get("line_code"):
        extra_filters.append(f"AND service_id = '{_sql_string_escape(params['line_code'])}'")
    extra_filter = " ".join(extra_filters)

    sql = f"""
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
            SAFE_CAST(total_seats AS INT64)                                       AS total_seats,
            ROW_NUMBER() OVER (
              PARTITION BY relation_name, departure_date, service_id, departure_time
              ORDER BY scrape_timestamp DESC
            ) AS rn
          FROM `redbus-agent-490708.redbus.bus_inventory`
          WHERE scrape_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
            AND PARSE_DATE('%d-%b-%Y', departure_date) BETWEEN {date_lower_sql} AND {date_upper_sql}
            AND ({operator_filter})
            {product_filter}
            {extra_filter}
        )
        SELECT
          relation_name,
          departure_date,
          departure_time,
          service_id,
          travels_name,
          bus_type,
          is_seater,
          is_sleeper,
          is_ac,
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
        **applied_date,
        "operators":    op_keys,
        "product_type": products,
        "region":       params.get("region"),
        "relation":     params.get("relation"),
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

        # by_service (uses service_id as the stable "line" identifier)
        svc   = r["service_id"] or "(no-service)"
        k3    = (svc, r["relation_name"], op)
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
            "service_id":       svc,
            "corridor":         corridor,
            "operator":         op,
            "departure_count":  v["count"],
            "departure_times":  sorted(set(v["times"])),
        }
        for (svc, corridor, op), v in sorted(by_line.items())
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

            # Per-bus rows for the schedule table view (the NP-team primary view).
            # Cap at 5000 to keep payload bounded; if user hits cap, they should filter further.
            ROW_CAP = 5000
            per_bus = []
            for r in rows[:ROW_CAP]:
                if r["operator"] == "other":
                    continue
                seats_total = int(r["total_seats"]) if r["total_seats"] else None
                seats_avail = int(r["available_seats"]) if r["available_seats"] is not None else None
                load_pct = None
                if seats_total and seats_avail is not None:
                    load_pct = round((1 - seats_avail / seats_total) * 100, 1)
                if r["is_seater"] and r["is_sleeper"]:
                    product = "Hybrid"
                elif r["is_sleeper"]:
                    product = "Sleeper"
                elif r["is_seater"]:
                    product = "Seater"
                else:
                    product = "Unknown"
                per_bus.append({
                    "departure_date": r["departure_date"].isoformat(),
                    "departure_time": r["departure_time"],
                    "operator":       r["operator"],
                    "relation_name":  r["relation_name"],
                    "service_id":     r["service_id"],
                    "bus_type":       r["bus_type"],
                    "product":        product,
                    "is_ac":          bool(r["is_ac"]) if r["is_ac"] is not None else None,
                    "is_volvo":       bool(r["bus_type"] and "volvo" in r["bus_type"].lower()),
                    "total_seats":    seats_total,
                    "avail_seats":    seats_avail,
                    "load_pct":       load_pct,
                })

            self.wfile.write(json.dumps({
                "ok":                    True,
                "filters_applied":       applied,
                "row_count_raw":         len(rows),
                "row_cap":               ROW_CAP,
                "rows":                  per_bus,
                "by_operator_hourband":  by_oh,
                "by_day":                by_day,
                "by_line":               by_line,
            }, default=str).encode())

        except Exception as exc:
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode())

    def log_message(self, *_):
        pass
