# Flix · Network Planning Dashboard

Departures-focused intelligence dashboard for the Network Planning team. Tracks Flix and competitor departures across D0-D30, with backward-looking views for WoW/45-day trend.

**Data source:** `redbus-agent-490708.redbus.bus_inventory` (BigQuery, single source, covers D0-D30).

**Why a separate project:** This is for NP team strategy (corridor coverage, timing competition, hub planning). The sister project `flix-price-alerts` is for pricing/parity teams. They share the same BigQuery service account but have independent user DBs and Vercel deployments.

---

## Local layout

```
flix-np-dashboard/
├── api/
│   ├── auth.py            # JWT login (pgcrypto/bcrypt) — copied from price-alerts
│   ├── admin.py           # user CRUD (admin only)
│   ├── freshness.py       # bus_inventory latest scrape + list of excluded partial days
│   ├── np_departures.py   # forward-looking endpoint (STUB — Phase 2)
│   ├── np_history.py      # WoW / 45d trend endpoint (STUB — Phase 2)
│   └── requirements.txt
├── SQL/
│   ├── setup_users_db.sql # run once against the NP-team Railway Postgres
│   ├── np_departures.sql  # forward-looking BQ query (template)
│   └── np_history.sql     # backward-looking BQ query (template)
├── config.py
├── dashboard.html         # SPA shell (3 tabs)
├── favicon.ico
├── requirements.txt
└── vercel.json
```

---

## Environment variables (set in Vercel project settings)

| Variable | Purpose |
| --- | --- |
| `SA_CREDENTIALS_JSON` | BigQuery service account JSON (same SA as `flix-price-alerts`) |
| `RAILWAY_DATABASE_URL` | Postgres URL for the NP-team users DB (new instance, separate from price-alerts) |
| `JWT_SECRET` | random 32+ char string for JWT signing |

---

## Deploy steps

1. **Provision a fresh Postgres** (Railway / Neon). Run [SQL/setup_users_db.sql](SQL/setup_users_db.sql) in its console after changing `CHANGE_ME_BEFORE_RUNNING` to a real password.
2. **Push this repo to GitHub.**
3. **Import to Vercel** as a new project. Vercel auto-detects `vercel.json`.
4. Set the three env vars above in Vercel.
5. Deploy. The dashboard should be live with a login screen.

---

## Roadmap

- [x] **Phase 1** — scaffold (this commit)
- [ ] **Phase 2** — wire `np_departures.py` and `np_history.py` to the SQL templates
- [ ] **Phase 3** — build out Tab 1 charts (D0-D4 by hour-band, by day, per-line table)
- [ ] **Phase 4** — Tab 2 charts (weekly roll-up, day×hour heatmap, corridor table)
- [ ] **Phase 5** — Tab 3 charts (WoW cards, 45-day trend line, MoM after June 2026)
- [ ] **Phase 6** — production deploy + share with NP team

---

## Data caveats baked into the design

- **D0-D4 staleness ≈ 24h.** `bus_inventory` refreshes once per day. For NP planning (week-out, month-out decisions) this is fine. Sister project `flix-price-alerts` uses `mini_crawl_latest` for fresher D0-D4 — we deliberately don't, to keep the data model single-source.
- **Crawl-volume regime change on 2026-04-01** is *neutralised by design*: we always pick the latest scrape per `(relation_name, departure_date, service_id, departure_time)` and count distinct buses. Raw row counts are never summed.
- **Partial-crawl days are excluded** from history (any scrape day with <150K rows or <29-day forward window). The freshness endpoint reports excluded dates so the UI can show a transparent banner.
- **MoM** is shown as placeholder until 2026-06 (need ≥2 months of clean data).
- **YoY** is hidden until 2027-03 (crawls started 2026-03-20).
