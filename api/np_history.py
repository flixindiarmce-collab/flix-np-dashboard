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
  For each historical departure_date, pick the LATEST scrape_timestamp
  (PARTITION BY ... ORDER BY scrape_timestamp DESC). This makes the metric
  resilient to the 2026-04-01 crawl-volume regime change.

  Excludes partial-crawl scrape_dates (n < 150K rows or forward span < 29 days).

Returns (wow):
  {
    "ok":           true,
    "current_week": {start, end, departure_count, by_operator: {...}},
    "prior_week":   {start, end, departure_count, by_operator: {...}},
    "delta_pct":    {flix: ..., zingbus: ..., ...}
  }

Returns (trend_45):
  {
    "ok":     true,
    "points": [{departure_date, operator, departure_count}]   # 45 most recent clean days
  }

NOTE: This is a stub. SQL lives in SQL/np_history.sql once Phase 2 starts.
"""
import json
import os
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        qs     = parse_qs(self.path.split("?", 1)[-1]) if "?" in self.path else {}
        params = {k: v[0] for k, v in qs.items()}
        view   = params.get("view", "wow")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        self.wfile.write(json.dumps({
            "ok":              True,
            "stub":            True,
            "message":         f"Phase 2 — np_history endpoint ({view}) not yet implemented",
            "filters_applied": params,
            "view":            view,
        }).encode())

    def log_message(self, *_):
        pass
