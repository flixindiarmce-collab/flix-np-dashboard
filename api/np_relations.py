"""
/api/np-relations
  GET — distinct relation_names from bus_inventory, for populating the
        From/To dropdowns on the frontend.

  Cached aggressively (24h) — relation list barely changes day to day.
"""
import json
import os
from http.server import BaseHTTPRequestHandler

from google.cloud import bigquery
from google.oauth2 import service_account


PROJECT = "redbus-agent-490708"
SCOPES  = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
]


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()

        try:
            sa_info = json.loads(os.environ["SA_CREDENTIALS_JSON"])
            creds   = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
            client  = bigquery.Client(project=PROJECT, credentials=creds)

            sql = """
                SELECT DISTINCT relation_name
                FROM `redbus-agent-490708.redbus.bus_inventory`
                WHERE scrape_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
                  AND relation_name IS NOT NULL
                ORDER BY relation_name
            """
            rows = [r["relation_name"] for r in client.query(sql).result()]
            self.wfile.write(json.dumps({
                "ok":        True,
                "relations": rows,
                "count":     len(rows),
            }).encode())

        except Exception as exc:
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode())

    def log_message(self, *_):
        pass
