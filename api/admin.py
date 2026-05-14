"""
/api/admin  — requires admin JWT in Authorization: Bearer header
  GET                   — list all users
  GET?action=status     — system status (DB ping, user count)
  POST                  — create user  { user_id, name, password, role, tabs[] }
  PUT                   — update user  { user_id, name?, role?, tabs?, active?, password? }
  DELETE?user_id=xxx    — deactivate user (soft delete)

Password hashing uses pgcrypto bcrypt — no Python crypto needed.
"""
import base64, hashlib, hmac, json, os, time
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse


_SECRET = os.environ.get("JWT_SECRET", "change-me-in-vercel-env")


def _verify_token(token: str) -> dict | None:
    try:
        b64, sig = token.rsplit(".", 1)
        expected = hmac.new(_SECRET.encode(), b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        payload = json.loads(base64.urlsafe_b64decode(b64 + "=="))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


def _db():
    import pg8000.native
    url = urlparse(os.environ["RAILWAY_DATABASE_URL"])
    return pg8000.native.Connection(
        host=url.hostname,
        port=url.port or 5432,
        database=url.path.lstrip("/"),
        user=url.username,
        password=url.password,
        ssl_context=True,
    )


def _require_admin(h):
    token   = h.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    payload = _verify_token(token)
    if not payload or payload.get("role") != "admin":
        h._json({"ok": False, "error": "Admin access required"}, 403)
        return None
    return payload


def _row_to_dict(row):
    uid, name, role, tabs, active, created_at, created_by, last_login = row
    return {
        "user_id":    uid,
        "name":       name,
        "role":       role,
        "tabs":       list(tabs) if tabs else None,
        "active":     active,
        "created_at": created_at.isoformat() if created_at else None,
        "created_by": created_by,
        "last_login": last_login.isoformat() if last_login else None,
    }


class handler(BaseHTTPRequestHandler):

    def _json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()

    # ── GET ───────────────────────────────────────────────────────────────────
    def do_GET(self):
        if not _require_admin(self):
            return
        qs     = parse_qs(self.path.split("?", 1)[-1]) if "?" in self.path else {}
        action = qs.get("action", [""])[0]
        try:
            db = _db()
            if action == "status":
                count   = db.run("SELECT COUNT(*) FROM dashboard_users")[0][0]
                actives = db.run("SELECT COUNT(*) FROM dashboard_users WHERE active = TRUE")[0][0]
                bq_ok   = False
                try:
                    from google.cloud import bigquery
                    from google.oauth2 import service_account
                    sa  = service_account.Credentials.from_service_account_info(
                        json.loads(os.environ["SA_CREDENTIALS_JSON"]),
                        scopes=["https://www.googleapis.com/auth/bigquery"]
                    )
                    bq  = bigquery.Client(project="redbus-agent-490708", credentials=sa)
                    list(bq.query("SELECT 1").result())
                    bq_ok = True
                except Exception:
                    pass
                return self._json({"ok": True, "status": {
                    "db": "connected", "bigquery": "connected" if bq_ok else "error",
                    "total_users": int(count), "active_users": int(actives),
                }})

            rows = db.run(
                "SELECT user_id, name, role, tabs, active, created_at, created_by, last_login "
                "FROM dashboard_users ORDER BY created_at ASC"
            )
            self._json({"ok": True, "users": [_row_to_dict(r) for r in rows]})
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 500)

    # ── POST: create user ─────────────────────────────────────────────────────
    def do_POST(self):
        caller = _require_admin(self)
        if not caller:
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length)) if length else {}
            uid    = body.get("user_id",  "").strip()
            name   = body.get("name",     "").strip()
            pwd    = body.get("password", "")
            role   = body.get("role",     "user")
            tabs   = body.get("tabs")

            if not uid or not name or not pwd:
                return self._json({"ok": False, "error": "user_id, name, password required"}, 400)
            if role not in ("admin", "np", "rm", "user"):
                return self._json({"ok": False, "error": "role must be admin | np | rm | user"}, 400)

            db = _db()
            # pgcrypto bcrypt via crypt() — password never stored in plain text
            db.run(
                "INSERT INTO dashboard_users (user_id, name, password_hash, role, tabs, created_by) "
                "VALUES (:uid, :name, crypt(:pwd, gen_salt('bf', 12)), :role, :tabs, :by)",
                uid=uid, name=name, pwd=pwd, role=role,
                tabs=tabs if tabs else None, by=caller["uid"]
            )
            self._json({"ok": True, "message": f"User '{uid}' created"})
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 500)

    # ── PUT: update user ──────────────────────────────────────────────────────
    def do_PUT(self):
        caller = _require_admin(self)
        if not caller:
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length)) if length else {}
            uid    = body.get("user_id", "").strip()
            if not uid:
                return self._json({"ok": False, "error": "user_id required"}, 400)

            db     = _db()
            sets   = []
            params = {"uid": uid}

            if "name" in body:
                sets.append("name = :name"); params["name"] = body["name"]
            if "role" in body:
                if body["role"] not in ("admin", "np", "rm", "user"):
                    return self._json({"ok": False, "error": "Invalid role"}, 400)
                sets.append("role = :role"); params["role"] = body["role"]
            if "tabs" in body:
                sets.append("tabs = :tabs"); params["tabs"] = body["tabs"]
            if "active" in body:
                sets.append("active = :active"); params["active"] = body["active"]
            if body.get("password"):
                # pgcrypto bcrypt
                sets.append("password_hash = crypt(:pwd, gen_salt('bf', 12))")
                params["pwd"] = body["password"]

            if not sets:
                return self._json({"ok": False, "error": "Nothing to update"}, 400)

            db.run(f"UPDATE dashboard_users SET {', '.join(sets)} WHERE user_id = :uid", **params)
            self._json({"ok": True, "message": f"User '{uid}' updated"})
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 500)

    # ── DELETE: deactivate user ───────────────────────────────────────────────
    def do_DELETE(self):
        caller = _require_admin(self)
        if not caller:
            return
        try:
            qs  = parse_qs(self.path.split("?", 1)[-1]) if "?" in self.path else {}
            uid = qs.get("user_id", [""])[0].strip()
            if not uid:
                return self._json({"ok": False, "error": "user_id param required"}, 400)
            if uid == caller["uid"]:
                return self._json({"ok": False, "error": "Cannot deactivate your own account"}, 400)
            _db().run("UPDATE dashboard_users SET active = FALSE WHERE user_id = :uid", uid=uid)
            self._json({"ok": True, "message": f"User '{uid}' deactivated"})
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 500)

    def log_message(self, *_):
        pass
