"""
/api/np-history
  GET — backward-looking departure counts from bus_inventory.

Query params:
  view          wow | trend_45 | share | dow | top_relations
                | wow_relation | util | mom       default: wow
  dep_from      YYYY-MM-DD                       optional
  dep_to        YYYY-MM-DD                       optional
  origin_hub    City name                        optional
  corridor      Full relation_name               optional
  line_code     Line code                        optional
  product_type  Comma-separated                  optional
  operators     Comma-separated                  optional

Logic:
  For each historical departure_date, count DISTINCT service_id across
  ALL scrapes that ever sighted it (UNION semantics). bus_inventory is
  an SRP archive — services drop off scrapes once they sell out, so any
  single scrape under-counts what actually ran. UNION across all scrapes
  in the window gives the real run count.

  No partial-crawl filtering: every scrape, even a small one, strictly
  ADDS signal to the UNION (it can only confirm "this service existed
  on this date", never disprove it).

Views:
  - wow:           this week (last 7 days) vs prior week, by operator
                   (always rolling vs today; ignores dep_from/dep_to)
  - trend_45:      daily departure counts per operator over the window
                   (45d default, or dep_from/dep_to if supplied)
  - share:         daily operator-share % over the window
  - dow:           avg daily departures per (operator × day-of-week)
                   over the window
  - top_relations: top 10 relations by total departures over the window,
                   with per-operator breakdown
  - wow_relation:  per-relation WoW gainers and losers (rolling 14d
                   regardless of dep_from/dep_to)
  - util:          daily seat-weighted utilization % for Flix vs
                   Competitors over the window
  - mom:           returns placeholder until 2026-06-01
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

# Product-type classification runs a 6-branch CASE ladder against
# (total_seats, bus_type). Ordered evaluation — first matching branch wins.
# bus_inventory_enriched.bus_product_type is intentionally NOT consulted:
# this ladder is the single source of truth so np-dashboard and comp-parity
# reach identical verdicts for the same bus.
#
#   Branch 1 (Sleeper): total_seats = 36 — the canonical 2+1 flat-berth
#                       capacity overrides any bus_type text.
#   Branch 2 (Hybrid ): bus_type contains BOTH 'seater' AND 'sleeper'
#                       (e.g. "A/C Seater/Sleeper" — dual cabin).
#   Branch 3 (Hybrid ): bus_type contains 'sleeper.*sleeper' AND 'semi'
#                       ("Semi Sleeper + Sleeper" combo).
#   Branch 4 (Seater ): bus_type contains 'semi' + 'sleeper' without the
#                       double-sleeper pattern (semi-sleeper is a reclining
#                       seat, not a berth).
#   Branch 5 (Sleeper): bus_type has 'sleeper' and NOT 'seater' — pure
#                       sleeper.
#   Branch 6 (Seater ): bus_type has 'seater' and NOT 'sleeper' — pure
#                       seater.
#
# A bus with bus_type matching no branch (and total_seats != 36) falls to
# Unknown and is EXCLUDED by any product-type filter.

_BT = "LOWER(bus_type)"
_DOUBLE_SLEEPER = f"REGEXP_CONTAINS({_BT}, r'sleeper.*sleeper')"

_B1_SLEEPER = "total_seats = 36"
_B2_HYBRID  = f"NOT ({_B1_SLEEPER}) AND {_BT} LIKE '%sleeper%' AND {_BT} LIKE '%seater%'"
_B3_HYBRID  = (
    f"NOT ({_B1_SLEEPER}) "
    f"AND NOT ({_BT} LIKE '%sleeper%' AND {_BT} LIKE '%seater%') "
    f"AND {_DOUBLE_SLEEPER} AND {_BT} LIKE '%semi%'"
)
_B4_SEATER = (
    f"NOT ({_B1_SLEEPER}) "
    f"AND NOT ({_BT} LIKE '%sleeper%' AND {_BT} LIKE '%seater%') "
    f"AND NOT ({_DOUBLE_SLEEPER} AND {_BT} LIKE '%semi%') "
    f"AND {_BT} LIKE '%semi%' AND {_BT} LIKE '%sleeper%' AND NOT {_DOUBLE_SLEEPER}"
)
_B5_SLEEPER = (
    f"NOT ({_B1_SLEEPER}) "
    f"AND NOT ({_BT} LIKE '%sleeper%' AND {_BT} LIKE '%seater%') "
    f"AND NOT ({_DOUBLE_SLEEPER} AND {_BT} LIKE '%semi%') "
    f"AND NOT ({_BT} LIKE '%semi%' AND {_BT} LIKE '%sleeper%') "
    f"AND {_BT} LIKE '%sleeper%' AND {_BT} NOT LIKE '%seater%'"
)
_B6_SEATER = (
    f"NOT ({_B1_SLEEPER}) "
    f"AND NOT ({_BT} LIKE '%sleeper%' AND {_BT} LIKE '%seater%') "
    f"AND NOT ({_DOUBLE_SLEEPER} AND {_BT} LIKE '%semi%') "
    f"AND NOT ({_BT} LIKE '%semi%' AND {_BT} LIKE '%sleeper%') "
    f"AND NOT ({_BT} LIKE '%sleeper%' AND {_BT} NOT LIKE '%seater%') "
    f"AND {_BT} LIKE '%seater%' AND {_BT} NOT LIKE '%sleeper%'"
)

PRODUCT_TYPE_CLAUSES = {
    "Sleeper": f"(({_B1_SLEEPER}) OR ({_B5_SLEEPER}))",
    "Hybrid":  f"(({_B2_HYBRID}) OR ({_B3_HYBRID}))",
    "Seater":  f"(({_B4_SEATER}) OR ({_B6_SEATER}))",
    # Volvo is orthogonal — applies on top of any seat layout.
    "Volvo":   f"{_BT} LIKE '%volvo%'",
}


def _enriched_cte(dep_clause: str) -> str:
    """SQL fragment that emits an `enriched` CTE with the latest
    bus_product_type per (relation_name, departure_date, service_id).

    Callers LEFT JOIN this CTE onto their per-bus stage USING those three
    columns. The CTE's own dep_clause filter narrows the enriched read to the
    same departure window as the outer query so we don't scan the full table.

    The dep_clause parameter is the same PARSE_DATE window predicate the outer
    query uses — bus_inventory_enriched shares the '%d-%b-%Y' departure_date
    format so it applies verbatim.
    """
    return f"""
        enriched AS (
          SELECT * FROM (
            SELECT
              relation_name,
              PARSE_DATE('%d-%b-%Y', departure_date) AS departure_date,
              service_id,
              bus_product_type,
              ROW_NUMBER() OVER (
                PARTITION BY relation_name, departure_date, service_id
                ORDER BY scrape_timestamp DESC
              ) AS rn
            FROM `redbus-agent-490708.redbus.bus_inventory_enriched`
            WHERE {dep_clause}
              AND service_id    IS NOT NULL AND service_id != ''
              AND relation_name IS NOT NULL
          )
          WHERE rn = 1
        )
    """

def _bq_client():
    sa_info = json.loads(os.environ["SA_CREDENTIALS_JSON"])
    creds   = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return bigquery.Client(project=PROJECT, credentials=creds)


def _sql_string_escape(s: str) -> str:
    return s.replace("'", "''")


_DATE_RE = __import__("re").compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_date_range(params: dict) -> tuple[str | None, str | None]:
    """Read dep_from / dep_to from params. Validate YYYY-MM-DD shape; return
    (None, None) if either is missing/malformed so callers fall back to the
    default rolling window."""
    a = (params.get("dep_from") or "").strip()
    b = (params.get("dep_to")   or "").strip()
    if _DATE_RE.match(a) and _DATE_RE.match(b) and a <= b:
        return a, b
    return None, None


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

    dep_from, dep_to = _parse_date_range(params)

    applied = {
        "operators":    op_keys,
        "product_type": products,
        "corridor":     params.get("corridor"),
        "origin_hub":   params.get("origin_hub"),
        "line_code":    params.get("line_code"),
        "dep_from":     dep_from,
        "dep_to":       dep_to,
    }
    return operator_filter, product_filter, " ".join(extra_filters), applied


def _window_clauses(history_days: int, dep_from: str | None, dep_to: str | None) -> tuple[str, str]:
    """Return (scrape_window_clause, departure_window_clause) for a window.

    If dep_from/dep_to are supplied, the departure window honours those bounds
    and the scrape window extends 15 days past dep_to (in case the data was
    crawled before dep_to but the bus_inventory archive caught it later).
    Otherwise we fall back to the rolling history_days window relative to
    today.
    """
    if dep_from and dep_to:
        scrape_clause = (
            f"scrape_timestamp BETWEEN TIMESTAMP_SUB(TIMESTAMP('{dep_from}'), INTERVAL 15 DAY) "
            f"AND TIMESTAMP_ADD(TIMESTAMP('{dep_to}'), INTERVAL 15 DAY)"
        )
        dep_clause = f"PARSE_DATE('%d-%b-%Y', departure_date) BETWEEN DATE '{dep_from}' AND DATE '{dep_to}'"
    else:
        scrape_clause = (
            f"scrape_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ({history_days} + 15) DAY)"
        )
        dep_clause = (
            f"PARSE_DATE('%d-%b-%Y', departure_date) "
            f"BETWEEN DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL {history_days} DAY) "
            f"    AND DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 1 DAY)"
        )
    return scrape_clause, dep_clause


def _build_trend_sql(history_days: int, op_filter: str, prod_filter: str, extra_filter: str,
                     dep_from: str | None = None, dep_to: str | None = None) -> str:
    """Daily departure counts per operator across the selected window.

    departure_count = COUNT(DISTINCT service_id) per operator on that date.
    A single service running multiple departure_time slots on the same day
    counts as ONE departure — same row of inventory under different
    schedules.
    """
    scrape_clause, dep_clause = _window_clauses(history_days, dep_from, dep_to)
    return f"""
        -- bus_inventory is an SRP scrape archive. A service appears in this
        -- table only if a crawl listed it on the SRP. Once a bus sells out
        -- it drops off later scrapes. To answer "how many services ran on
        -- this date?" we UNION all scrapes that ever sighted the service —
        -- a service counts as long as ANY crawl saw it. Each per-scrape
        -- crawl strictly ADDS signal, so we deliberately don't gate on
        -- partial-crawl day thresholds here (a 40K-row scrape still tells
        -- us "this service existed on this date").
        WITH base AS (
          SELECT
            scrape_timestamp,
            relation_name,
            PARSE_DATE('%d-%b-%Y', departure_date)  AS departure_date,
            departure_time,
            service_id,
            travels_name,
            bus_type,
            is_seater,
            is_sleeper,
            SAFE_CAST(total_seats AS INT64)         AS total_seats
          FROM `redbus-agent-490708.redbus.bus_inventory`
          WHERE {scrape_clause}
            AND ({op_filter})
            {extra_filter}
            AND {dep_clause}
        ),
        per_bus AS (
          -- Identity = (relation, date, service_id). departure_time is NOT
          -- part of the partition: with multiple daily crawls the same
          -- service_id can return with a slightly drifted departure_time,
          -- and including it splits one bus into many "latest" rows. Latest
          -- scrape per service_id wins.
          SELECT * FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY relation_name, departure_date, service_id
                ORDER BY scrape_timestamp DESC
              ) AS rn
            FROM base
            WHERE service_id IS NOT NULL AND service_id != ''
              AND relation_name IS NOT NULL
          )
          WHERE rn = 1
        ),
        -- Join curated bus_product_type from bus_inventory_enriched so the
        -- {{prod_filter}} clause can prefer 'seater'/'sleeper'/'hybrid' from
        -- the enriched pipeline over bus_type string heuristics. LEFT JOIN
        -- so buses missing from enriched still flow through (fallback uses
        -- the bus_type string via PRODUCT_TYPE_CLAUSES).
        {_enriched_cte(dep_clause)},
        dedup AS (
          SELECT p.*, e.bus_product_type
          FROM per_bus p
          LEFT JOIN enriched e
            USING (relation_name, departure_date, service_id)
          WHERE 1=1 {prod_filter}
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
          COUNT(DISTINCT service_id) AS departure_count
        FROM dedup
        GROUP BY departure_date, operator
        ORDER BY departure_date, operator
    """


def _do_trend_45(params: dict, applied: dict, op_filter: str, prod_filter: str, extra_filter: str):
    dep_from, dep_to = applied.get("dep_from"), applied.get("dep_to")
    sql = _build_trend_sql(45, op_filter, prod_filter, extra_filter, dep_from, dep_to)
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
    # WoW always uses the rolling 14d window (current vs prior 7d) regardless
    # of dep_from/dep_to — the KPIs answer "how is this week vs last week"
    # against today, not against an arbitrary user-selected window.
    sql = _build_trend_sql(14, op_filter, prod_filter, extra_filter, None, None)
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


# ── New views: share, day-of-week, top relations, WoW per relation ────────


def _trend_rows(history_days: int, op_filter, prod_filter, extra_filter, dep_from, dep_to):
    """Run _build_trend_sql and return list of dicts (excluding 'other')."""
    sql = _build_trend_sql(history_days, op_filter, prod_filter, extra_filter, dep_from, dep_to)
    return [dict(r) for r in _bq_client().query(sql).result() if r["operator"] != "other"]


def _do_share(params, applied, op_filter, prod_filter, extra_filter):
    """Daily operator-share: % of total daily departures per operator.

    Window: dep_from/dep_to if set, else last 45 days. Returns a series of
    {date, operator, share_pct, count} rows the frontend can stack.
    """
    rows = _trend_rows(45, op_filter, prod_filter, extra_filter,
                       applied.get("dep_from"), applied.get("dep_to"))
    by_date = defaultdict(lambda: defaultdict(int))
    for r in rows:
        by_date[r["departure_date"]][r["operator"]] += r["departure_count"]

    points = []
    for d, ops in sorted(by_date.items()):
        tot = sum(ops.values()) or 1
        for op, n in ops.items():
            points.append({
                "departure_date": d.isoformat(),
                "operator":       op,
                "count":          n,
                "share_pct":      round(100.0 * n / tot, 1),
            })
    return {
        "ok":              True,
        "view":            "share",
        "filters_applied": applied,
        "points":          points,
    }


_DOW_NAMES = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]


def _do_dow(params, applied, op_filter, prod_filter, extra_filter):
    """Average daily departures per (operator × day-of-week) over the window.

    "Average" = sum of daily counts on that DOW divided by the number of
    distinct dates of that DOW in the window. So "Tuesday" reads as a
    typical Tuesday — not the sum across all Tuesdays.
    """
    rows = _trend_rows(45, op_filter, prod_filter, extra_filter,
                       applied.get("dep_from"), applied.get("dep_to"))
    # operator -> dow_index -> [daily counts]
    bucket = defaultdict(lambda: defaultdict(list))
    for r in rows:
        d = r["departure_date"]
        bucket[r["operator"]][d.weekday()].append(r["departure_count"])

    points = []
    for op, by_dow in bucket.items():
        for dow, counts in by_dow.items():
            avg = sum(counts) / len(counts) if counts else 0
            points.append({
                "operator":  op,
                "dow":       dow,            # 0=Mon
                "dow_label": _DOW_NAMES[dow],
                "avg":       round(avg, 1),
                "n_dates":   len(counts),
            })
    points.sort(key=lambda r: (r["operator"], r["dow"]))
    return {
        "ok":              True,
        "view":            "dow",
        "filters_applied": applied,
        "points":          points,
    }


def _do_top_relations(params, applied, op_filter, prod_filter, extra_filter):
    """Top relations by total deduped departures over the window — for the
    selected operator set. Returns up to 10 relations with per-operator
    breakdown.

    A separate SQL: we need relation_name in the GROUP BY, so we re-issue a
    similar query rather than reusing _build_trend_sql which collapses
    relation. Window honours dep_from/dep_to.
    """
    history_days = 45
    scrape_clause, dep_clause = _window_clauses(history_days,
                                                applied.get("dep_from"),
                                                applied.get("dep_to"))
    sql = f"""
        WITH base AS (
          SELECT scrape_timestamp, relation_name, service_id, travels_name, bus_type, is_seater, is_sleeper,
                 SAFE_CAST(total_seats AS INT64) AS total_seats,
                 PARSE_DATE('%d-%b-%Y', departure_date) AS departure_date
          FROM `redbus-agent-490708.redbus.bus_inventory`
          WHERE {scrape_clause}
            AND ({op_filter})
            {extra_filter}
            AND {dep_clause}
        ),
        per_bus AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY relation_name, departure_date, service_id
              ORDER BY scrape_timestamp DESC
            ) AS rn
            FROM base
            WHERE service_id IS NOT NULL AND service_id != ''
              AND relation_name IS NOT NULL
          ) WHERE rn = 1
        ),
        {_enriched_cte(dep_clause)},
        dedup AS (
          SELECT p.*, e.bus_product_type
          FROM per_bus p
          LEFT JOIN enriched e
            USING (relation_name, departure_date, service_id)
          WHERE 1=1 {prod_filter}
        )
        SELECT
          relation_name,
          CASE WHEN LOWER(travels_name) LIKE '%flix%'                                              THEN 'flix'
               WHEN LOWER(travels_name) LIKE '%intrcity%'                                          THEN 'intrcity'
               WHEN LOWER(travels_name) LIKE '%zingbus%' AND LOWER(travels_name) NOT LIKE '%maxx%' THEN 'zingbus'
               WHEN LOWER(travels_name) LIKE '%nuego%'                                             THEN 'nuego'
               WHEN LOWER(travels_name) LIKE '%freshbus%'                                          THEN 'freshbus'
               WHEN LOWER(travels_name) LIKE '%laxmi holidays%' AND LOWER(travels_name) NOT LIKE '%pvt%' THEN 'laxmi'
               ELSE 'other' END AS operator,
          COUNT(DISTINCT service_id) AS departure_count
        FROM dedup
        GROUP BY relation_name, operator
    """
    rows = [dict(r) for r in _bq_client().query(sql).result() if r["operator"] != "other"]

    # Total per relation, then top 10. For each relation, per-operator counts.
    rel_totals = defaultdict(int)
    rel_by_op  = defaultdict(lambda: defaultdict(int))
    for r in rows:
        rel = r["relation_name"]
        rel_totals[rel] += r["departure_count"]
        rel_by_op[rel][r["operator"]] += r["departure_count"]
    top = sorted(rel_totals.items(), key=lambda kv: -kv[1])[:10]

    out = []
    for rel, total in top:
        out.append({
            "relation":    rel,
            "total":       total,
            "by_operator": dict(rel_by_op[rel]),
        })
    return {
        "ok":              True,
        "view":            "top_relations",
        "filters_applied": applied,
        "rows":            out,
    }


def _do_util(params, applied, op_filter, prod_filter, extra_filter):
    """Daily seat-weighted utilization for Flix vs Competitors over the window.

    util_pct = 1 - sum(available_seats) / sum(total_seats), aggregated across
    all services on that date. Latest scrape per (relation, date, service_id)
    wins — same dedup logic as the trend view. The latest scrape's
    available_seats is the closest proxy we have for "actually sold seats" by
    departure time, since once a bus sells out it drops off subsequent scrapes
    and its last-seen avail count is what stuck.
    """
    history_days = 45
    scrape_clause, dep_clause = _window_clauses(history_days,
                                                applied.get("dep_from"),
                                                applied.get("dep_to"))
    sql = f"""
        WITH base AS (
          SELECT scrape_timestamp, relation_name, service_id, travels_name, bus_type,
                 is_seater, is_sleeper,
                 SAFE_CAST(total_seats     AS INT64) AS total_seats,
                 SAFE_CAST(available_seats AS INT64) AS available_seats,
                 PARSE_DATE('%d-%b-%Y', departure_date) AS departure_date
          FROM `redbus-agent-490708.redbus.bus_inventory`
          WHERE {scrape_clause}
            AND ({op_filter})
            {extra_filter}
            AND {dep_clause}
        ),
        per_bus AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY relation_name, departure_date, service_id
              ORDER BY scrape_timestamp DESC
            ) AS rn
            FROM base
            WHERE service_id IS NOT NULL AND service_id != ''
              AND relation_name IS NOT NULL
              AND total_seats IS NOT NULL AND total_seats > 0
              AND available_seats IS NOT NULL
          ) WHERE rn = 1
        ),
        {_enriched_cte(dep_clause)},
        dedup AS (
          SELECT p.*, e.bus_product_type
          FROM per_bus p
          LEFT JOIN enriched e
            USING (relation_name, departure_date, service_id)
          WHERE 1=1 {prod_filter}
        )
        SELECT
          departure_date,
          CASE WHEN LOWER(travels_name) LIKE '%flix%' THEN 'flix' ELSE 'comp' END AS operator_group,
          SUM(total_seats)     AS total_seats,
          SUM(available_seats) AS available_seats
        FROM dedup
        GROUP BY departure_date, operator_group
        ORDER BY departure_date, operator_group
    """
    rows = [dict(r) for r in _bq_client().query(sql).result()]
    points = []
    for r in rows:
        total = r["total_seats"] or 0
        avail = r["available_seats"] or 0
        util_pct = round((1 - avail / total) * 100, 1) if total else None
        points.append({
            "departure_date": r["departure_date"].isoformat(),
            "operator_group": r["operator_group"],
            "total_seats":    int(total),
            "avail_seats":    int(avail),
            "util_pct":       util_pct,
        })
    return {
        "ok":              True,
        "view":            "util",
        "filters_applied": applied,
        "points":          points,
    }


def _do_wow_relation(params, applied, op_filter, prod_filter, extra_filter):
    """Per-relation WoW change. Always rolls 14d (last 7d vs prior 7d, today
    being the anchor) regardless of dep_from/dep_to — answers "this week vs
    last week, which routes shifted most".

    Output: rows of {relation, current, prior, abs_delta, pct_delta} sorted
    by abs(pct_delta) descending. Top 10 movers per direction (gainers +
    losers) returned.
    """
    history_days = 14
    scrape_clause, dep_clause = _window_clauses(history_days, None, None)
    sql = f"""
        WITH base AS (
          SELECT scrape_timestamp, relation_name, service_id, travels_name, bus_type, is_seater, is_sleeper,
                 SAFE_CAST(total_seats AS INT64) AS total_seats,
                 PARSE_DATE('%d-%b-%Y', departure_date) AS departure_date
          FROM `redbus-agent-490708.redbus.bus_inventory`
          WHERE {scrape_clause}
            AND ({op_filter})
            {extra_filter}
            AND {dep_clause}
        ),
        per_bus AS (
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY relation_name, departure_date, service_id
              ORDER BY scrape_timestamp DESC
            ) AS rn
            FROM base
            WHERE service_id IS NOT NULL AND service_id != ''
              AND relation_name IS NOT NULL
          ) WHERE rn = 1
        ),
        {_enriched_cte(dep_clause)},
        dedup AS (
          SELECT p.*, e.bus_product_type
          FROM per_bus p
          LEFT JOIN enriched e
            USING (relation_name, departure_date, service_id)
          WHERE 1=1 {prod_filter}
        )
        SELECT relation_name, departure_date,
               COUNT(DISTINCT service_id) AS n
        FROM dedup
        GROUP BY relation_name, departure_date
    """
    rows = [dict(r) for r in _bq_client().query(sql).result()]

    today        = date.today()
    cur_start    = today - timedelta(days=7)
    cur_end      = today - timedelta(days=1)
    prior_start  = today - timedelta(days=14)
    prior_end    = today - timedelta(days=8)

    cur   = defaultdict(int)
    prior = defaultdict(int)
    for r in rows:
        d   = r["departure_date"]
        rel = r["relation_name"]
        if cur_start <= d <= cur_end:
            cur[rel] += r["n"]
        elif prior_start <= d <= prior_end:
            prior[rel] += r["n"]

    deltas = []
    for rel in set(cur) | set(prior):
        c, p = cur.get(rel, 0), prior.get(rel, 0)
        deltas.append({
            "relation":  rel,
            "current":   c,
            "prior":     p,
            "abs_delta": c - p,
            "pct_delta": round(((c - p) / p) * 100, 1) if p else None,
        })

    # Top 10 gainers (largest positive pct_delta where prior > 0) and top 10
    # losers (largest negative pct_delta). Skip relations with prior=0 in the
    # gainers list — pct is undefined and we don't want zero-baseline noise.
    rel_with_pct = [d for d in deltas if d["pct_delta"] is not None]
    gainers = sorted(rel_with_pct, key=lambda d: -d["pct_delta"])[:10]
    losers  = sorted(rel_with_pct, key=lambda d:  d["pct_delta"])[:10]
    return {
        "ok":              True,
        "view":            "wow_relation",
        "filters_applied": applied,
        "current_week":    {"start": cur_start.isoformat(),   "end": cur_end.isoformat()},
        "prior_week":      {"start": prior_start.isoformat(), "end": prior_end.isoformat()},
        "gainers":         gainers,
        "losers":          losers,
    }


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        qs     = parse_qs(self.path.split("?", 1)[-1]) if "?" in self.path else {}
        params = {k: v[0] for k, v in qs.items()}
        view   = params.get("view", "wow")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.end_headers()

        try:
            op_filter, prod_filter, extra_filter, applied = _build_filter_clauses(params)

            if view == "trend_45":
                result = _do_trend_45(params, applied, op_filter, prod_filter, extra_filter)
            elif view == "share":
                result = _do_share(params, applied, op_filter, prod_filter, extra_filter)
            elif view == "dow":
                result = _do_dow(params, applied, op_filter, prod_filter, extra_filter)
            elif view == "top_relations":
                result = _do_top_relations(params, applied, op_filter, prod_filter, extra_filter)
            elif view == "wow_relation":
                result = _do_wow_relation(params, applied, op_filter, prod_filter, extra_filter)
            elif view == "util":
                result = _do_util(params, applied, op_filter, prod_filter, extra_filter)
            elif view == "mom":
                result = _do_mom_placeholder()
            else:
                result = _do_wow(params, applied, op_filter, prod_filter, extra_filter)

            self.wfile.write(json.dumps(result, default=str).encode())

        except Exception as exc:
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode())

    def log_message(self, *_):
        pass
