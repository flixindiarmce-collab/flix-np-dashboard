"""
/api/np-departures
  GET — forward-looking departure counts from bus_inventory (D0-D30).

Query params:
  pbd_range     d0-d4 | d5-d30 | d0-d30        default: d0-d30
  origin_hub    City name (matches relation_name origin) optional
  corridor      Full relation_name                       optional
  line_code     Line code                                optional
  product_type  Comma-separated: Seater,Sleeper,Hybrid,Volvo  optional (default: all)
  operators     Comma-separated operator keys           optional (default: all)

Returns:
  {
    "ok": true,
    "filters_applied": {...},
    "by_operator_hourband": [{operator, hour_band, departure_count, seats_offered, avg_load_pct}],
    "by_day":               [{date, operator, departure_count}],
    "by_line":              [{line_code, corridor, operator, departure_count, departure_times[]}]
  }

NOTE: This is a stub. SQL lives in SQL/np_departures.sql once Phase 2 starts.
"""
import json
import os
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        qs     = parse_qs(self.path.split("?", 1)[-1]) if "?" in self.path else {}
        params = {k: v[0] for k, v in qs.items()}

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        self.wfile.write(json.dumps({
            "ok":              True,
            "stub":            True,
            "message":         "Phase 2 — np_departures endpoint not yet implemented",
            "filters_applied": params,
            "by_operator_hourband": [],
            "by_day":               [],
            "by_line":              [],
        }).encode())

    def log_message(self, *_):
        pass
