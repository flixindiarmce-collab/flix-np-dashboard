"""
/api/freshness
  GET — returns latest scrape timestamp from bus_inventory, plus list of
        recently-excluded partial-crawl dates so the UI can show a banner.

Returns:
  {
    "ok":           true,
    "latest":       "2026-05-14 09:42",   # most-recent scrape (IST)
    "excluded":     [{"date": "2026-04-05", "rows": 33669, "reason": "low row count"}],
    "history_days": 55                    # how many clean days of history we have
  }
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

MIN_ROWS_PER_DAY = 150_000
MIN_FORWARD_DAYS = 29


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        try:
            sa_info = json.loads(os.environ["SA_CREDENTIALS_JSON"])
            creds   = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
            client  = bigquery.Client(project=PROJECT, credentials=creds)

            latest_sql = """
                SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', MAX(scrape_timestamp), 'Asia/Kolkata') AS ts
                FROM `redbus-agent-490708.redbus.bus_inventory`
            """

            daily_sql = f"""
                SELECT
                  DATE(scrape_timestamp, 'Asia/Kolkata') AS scrape_date,
                  COUNT(*) AS n,
                  DATE_DIFF(
                    MAX(PARSE_DATE('%d-%b-%Y', departure_date)),
                    MIN(PARSE_DATE('%d-%b-%Y', departure_date)),
                    DAY
                  ) AS forward_span
                FROM `redbus-agent-490708.redbus.bus_inventory`
                WHERE scrape_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
                GROUP BY scrape_date
                ORDER BY scrape_date DESC
            """

            q_latest = client.query(latest_sql)
            q_daily  = client.query(daily_sql)

            latest = next(iter(q_latest.result())).ts or ""

            clean, excluded = 0, []
            for row in q_daily.result():
                bad_count   = row.n < MIN_ROWS_PER_DAY
                bad_forward = (row.forward_span or 0) < MIN_FORWARD_DAYS
                if bad_count or bad_forward:
                    reasons = []
                    if bad_count:   reasons.append(f"only {row.n:,} rows")
                    if bad_forward: reasons.append(f"{row.forward_span}-day forward window")
                    excluded.append({
                        "date":   row.scrape_date.isoformat(),
                        "rows":   row.n,
                        "reason": "; ".join(reasons),
                    })
                else:
                    clean += 1

            self.wfile.write(json.dumps({
                "ok":           True,
                "latest":       latest,
                "excluded":     excluded,
                "history_days": clean,
            }).encode())

        except Exception as exc:
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode())

    def log_message(self, *_):
        pass
