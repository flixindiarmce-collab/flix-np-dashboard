"""
/api/auth
  GET              — verify token (Authorization: Bearer <token>)
  POST             — login  { user_id, password }

Password hashing is handled entirely by PostgreSQL pgcrypto (bcrypt).
To create users directly in Railway SQL console:
  INSERT INTO dashboard_users (user_id, name, password_hash, role)
  VALUES ('admin', 'Master Admin', crypt('your_password', gen_salt('bf', 12)), 'admin');
"""
import base64, hashlib, hmac, json, os, time
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse


_SECRET = os.environ.get("JWT_SECRET", "change-me-in-vercel-env")


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


def _make_token(payload: dict) -> str:
    payload["exp"] = int(time.time()) + 7 * 86_400
    raw = json.dumps(payload, separators=(",", ":"))
    b64 = base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")
    sig = hmac.new(_SECRET.encode(), b64.encode(), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"


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


class handler(BaseHTTPRequestHandler):

    def _json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()

    # ── GET: verify token ─────────────────────────────────────────────────────
    def do_GET(self):
        token = self.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        payload = _verify_token(token)
        if not payload:
            return self._json({"ok": False, "error": "Invalid or expired token"}, 401)
        self._json({"ok": True, "user": payload})

    # ── POST: login ───────────────────────────────────────────────────────────
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length)) if length else {}

            uid = body.get("user_id", "").strip()
            pwd = body.get("password", "")
            if not uid or not pwd:
                return self._json({"ok": False, "error": "user_id and password required"}, 400)

            db = _db()

            # Verify password via pgcrypto — crypt() rehashes with stored salt and compares
            rows = db.run(
                "SELECT user_id, name, role, tabs, active "
                "FROM dashboard_users "
                "WHERE user_id = :uid AND password_hash = crypt(:pwd, password_hash)",
                uid=uid, pwd=pwd
            )

            if not rows:
                return self._json({"ok": False, "error": "Invalid credentials"}, 401)

            r_uid, r_name, r_role, r_tabs, r_active = rows[0]
            if not r_active:
                return self._json({"ok": False, "error": "Account disabled — contact admin"}, 403)

            db.run("UPDATE dashboard_users SET last_login = NOW() WHERE user_id = :uid", uid=uid)

            tabs  = list(r_tabs) if r_tabs else None
            token = _make_token({"uid": r_uid, "name": r_name, "role": r_role, "tabs": tabs})
            self._json({"ok": True, "token": token, "user": {
                "uid": r_uid, "name": r_name, "role": r_role, "tabs": tabs
            }})

        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, 500)

    def log_message(self, *_):
        pass
