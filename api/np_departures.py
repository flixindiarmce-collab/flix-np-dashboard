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

# Flix line UUID (first 36 chars of service_id) -> line code.
# Static mapping shared by the Flix team; no longer depends on mini_crawl_latest.
UUID_TO_LINE = {
    "b496b789-c346-4334-b548-395646d9d6ee": "IN0110",
    "95572923-ef71-4cf3-8ac7-77104851b017": "IN0120",
    "592aa978-56af-4a3c-98bd-3451653d91b0": "IN0120A",
    "db480497-d8fd-4f0f-835a-163875d40459": "IN0120C",
    "69096e6f-56c7-4b5a-857c-d9670f8d852f": "IN0120D",
    "ec18d028-29b3-413a-a15a-7efa504ad5f3": "IN0121A",
    "317a9a51-0de6-4b73-921e-dae6334f916b": "IN0210",
    "84dfe4fa-9623-4cf7-a21c-268b8eeaf71a": "IN0210A",
    "75d99d81-e5d9-4428-b4c2-823720dc2095": "IN0210C",
    "23750bb4-cc6b-461e-ab14-21c636f7b70d": "IN0211",
    "89567f53-2da1-4b1b-baaf-aefbdcda5ee1": "IN0211B",
    "b0331778-942d-478c-b751-cac180ce1d7a": "IN0212",
    "aded58fd-bb0c-45e3-b6cd-51638968ff3d": "IN0212A",
    "130d0b12-f2b0-4783-8005-8573ca680346": "IN0212B",
    "22a4a751-d72a-416f-a480-2c803b0f8401": "IN0213",
    "3c7c4278-dcef-43db-92c1-3a55b0f87eed": "IN0213A",
    "0cb111c4-0e4f-4fc9-9918-32caa6576a6d": "IN0214",
    "72c51962-5e58-492a-921e-7cdd68f161d5": "IN0310",
    "218453a2-cc03-44d1-8840-cbdb60c8e079": "IN0310A",
    "2a69f65d-f9b2-4ad4-b657-948d554c37de": "IN0310B",
    "bfe5b1e4-bff3-4715-8b73-5de6ff98cf77": "IN0310C",
    "92e62b01-1052-44a2-9810-9f93add164e3": "IN0310D",
    "77223855-3b4e-4242-9ffe-1a0d1d9f566f": "IN0310E",
    "fe17591f-fbfc-4479-9897-203d362471ef": "IN0310F",
    "ae2c281e-85ac-4c0e-8649-41ee6b48a3ef": "IN0320",
    "268abe54-c1f5-41cb-885a-ce999a05a1cc": "IN0320A",
    "0c672a51-e005-427b-947b-a39f738a747c": "IN0320B",
    "e24197e0-2dbf-43e2-9c61-76c819360b8a": "IN0320C",
    "288732f9-52e4-445e-9112-3629ecb95fbf": "IN0320D",
    "e1fa0e0a-a170-4ed5-b95f-b7f88f252f46": "IN0410",
    "462461db-7aa9-4130-b372-af49e7e8d823": "IN0410A",
    "36e3fc1c-60da-496a-a8fc-b0bf8defb983": "IN0421A",
    "4568ed20-be7c-4375-ac36-d901b4315de3": "IN0421B",
    "fefdb46f-92d0-4c89-bde6-98ea092e3af5": "IN0421C",
    "0392d8fe-1749-48c7-88d0-c2212c937cdf": "IN0423",
    "7d4b75c9-a36d-4962-b8c3-a6908c38e6f1": "IN0424",
    "d05ef689-3f28-48a7-804f-f3e464e22d4b": "IN0424A",
    "63f58157-1a22-4c5b-abdf-1da503de7a97": "IN0425",
    "cff5094d-9672-4705-8b45-775f29c0b22b": "IN0426",
    "7e5794e8-2c6c-418c-97b5-35a6654e54f7": "IN0426A",
    "efbe9628-2fe0-4a4c-9b79-d978153620d4": "IN0430",
    "a2e1e9bc-c373-4b5a-937c-ce069a1c03be": "IN0430A",
    "29ab8429-4444-454f-bd00-d9e4cd94679c": "IN0520",
    "556f243e-bdde-4a9a-83b0-83f96fafd1d0": "IN0520A",
    "04970adb-b0e6-4357-8618-50767d2a42e3": "IN0610",
    "efba0533-009b-4f9a-b150-33955802adf0": "IN0610A",
    "ebd82b99-ae5d-4b2b-8008-df65c71bedca": "IN0614",
    "9958d1ef-7216-43f2-b19c-4a0dfb81fd08": "IN0614A",
    "3c8926ab-e467-47bc-8125-392d6a1e5285": "IN1011",
    "472eb112-4d5b-44c8-8751-4cae526a5454": "IN2100",
    "2cd8377c-d26a-4882-a330-433360361769": "IN2120",
    "bd936576-d3ce-4da1-940b-88ece36fd6f0": "IN2310",
    "cd140128-87c6-4aad-be17-7514f14f6597": "IN2310A",
    "36c17060-0ad8-4fa1-921e-fe32c34fed98": "IN2320",
    "598d6222-0f02-4c8d-9e25-d497ce6cebba": "IN2510",
    "d6a697a4-55a6-46ca-a764-7481f2eb93ec": "IN2511",
    "682633a1-33d2-4342-a965-8cd80f07b515": "IN2530",
    "85d0ee98-c9af-42c9-8959-c81a9a3e5080": "IN2810",
    "53b6f8d4-5bc2-4ca4-90b2-a455449e0826": "IN2820",
    "fdd58745-c866-437c-9263-451b35294d19": "IN2820A",
    "a6bbc851-7eb1-45d6-825d-1d5b6250f92f": "IN4108",
    "9cc512a7-3ecb-11ea-8017-02437075395e": "IN4110",
    "f13bbb5e-185e-4ca0-96ce-7c74f09854d9": "IN4111",
    "2eb1b3c6-3601-44c9-aad0-40c59f5a1747": "IN4112",
    "577ca07c-193f-40b6-9215-ea7a782e9212": "IN4115",
    "cc4a2370-69b9-4037-836b-170c3150fbf7": "IN4118",
    "64fda8cc-cacf-4ab1-b033-b89ca09338df": "IN4210",
    "dd48d18c-3df1-433a-956b-9ca9145915a2": "IN4220",
    "1a1860bf-864e-4e08-83c2-8c6da5298fa6": "IN4221",
    "3c1ca721-7e6b-42a2-a991-79d888f46d3e": "IN4222",
    "ec6b7803-498a-411c-afb1-a32e323192e5": "IN4225",
    "676e56eb-d7f1-4c8b-81dd-303e72a781c6": "IN4230",
    "c7f82323-aee5-48e4-ac53-639241594abd": "IN4309",
    "48e985f6-3f4f-4a92-b229-cac0c6e8b86e": "IN4310",
    "d392a36e-b893-476d-a090-11a180a6be07": "IN4311",
    "79e73708-7baa-4a97-9096-bb17c4345dc6": "IN4312",
    "c5dfc0b8-fbe4-4cb0-a00a-c0170c3fba50": "IN4313",
    "00c7282e-d2fc-4e7f-8f16-7ea32d582975": "IN4314",
    "b859338e-b7b4-4613-9463-4fe8fcdd9bcf": "IN4316",
    "223d6130-0393-431e-bb5f-6d55e2372804": "IN4317",
    "54dc5ef8-274c-4b2f-b3f1-41ec174cdae5": "IN4318",
    "6f5ad709-cde0-4ca3-b7df-7fc3c0b1a099": "IN4319",
    "32237ee2-288e-4577-b38a-2b9868534c99": "IN4320",
    "9cc51dd4-3ecb-11ea-8017-02437075395e": "IN4330",
    "852c456e-c71e-48e5-9b8b-31d761afc11c": "IN4332",
    "65949ade-ace5-44f8-9d84-a0519fa63309": "IN4335",
    "9cc51727-3ecb-11ea-8017-02437075395e": "IN4340",
    "731c7322-876d-48cb-aded-369dee74996f": "IN4341",
    "5935fa29-8a40-402c-88d8-c88f275b14d7": "IN4352",
    "7651465b-c42f-4bca-82a3-c78e08e1a5f0": "IN4411",
    "907a0df7-1080-4747-b35a-bf2b72887c9b": "IN4420",
    "5c133168-991b-40ce-bc40-7a8332580dc6": "IN4430",
    "90d5ce1c-cbc6-4213-992b-b1f38e1e0640": "IN4450",
    "d79ad666-2692-47cb-b734-76bbb9e03b94": "IN4520",
    "f2837286-ed58-4e11-bb47-d7cb67d3d254": "IN4540",
    "02693259-1c91-43a7-8395-ff783c50891b": "IN4580",
    "9cc52bcb-3ecb-11ea-8017-02437075395e": "IN4610",
    "9ec1f7c1-5500-436f-8490-71d63b520457": "IN4710",
    "7bc3da9e-4c48-4546-a5fc-33621d759268": "IN4710A",
    "81b8291d-f497-47b5-a32b-9676866a1a91": "IN5111",
    "427f70ba-9eeb-4240-88a4-41b0652a7d66": "IN5112",
    "762d1155-436f-493f-9824-a1c9a4f5aeea": "IN5113",
    "f2ad7617-742d-4985-82da-e0391d199766": "IN5113A",
    "4c38fb87-f1ef-4b15-a53e-74ded21207b9": "IN5116",
    "9cc48ca0-3ecb-11ea-8017-02437075395e": "IN5120",
    "0a8f18ca-a0a2-4644-9690-4a744d91c2d1": "IN5129",
    "9cc521ac-3ecb-11ea-8017-02437075395e": "IN5130",
    "6d190aa0-f6ad-41d5-a580-551005218d86": "IN5131",
    "1bbe5e44-b74c-4152-9909-4642051906ea": "IN5136",
    "442d26cf-99c4-464f-81fd-43a758c535cc": "IN5137",
    "a0b9de22-dd7c-4df5-a608-977f9e121267": "IN5138",
    "064d69ff-3d4a-4377-8e0a-4568876a7ad0": "IN5139",
    "9cc520c4-3ecb-11ea-8017-02437075395e": "IN5140",
    "e3bbff44-9e76-4428-a643-074cdeec5bb4": "IN5141",
    "a902bd44-8fc1-4e45-a40f-df0e553e22de": "IN5143",
    "1249bc0e-84f3-4ba3-aa00-ce4cde748cb5": "IN5210",
    "32335923-1316-4698-9fae-8c7b5aa31650": "IN5211",
    "5a48da56-06da-41c4-b617-f52b3e3b882e": "IN5212",
    "54298455-2a47-43c9-9c2d-10a857625d8b": "IN5213",
    "0f3258fb-05fa-4c7b-9bd0-41cb1a77f58e": "IN5214",
    "9cf92883-64a9-43d8-b49c-6f1e34427c45": "IN5215",
    "9cc51436-3ecb-11ea-8017-02437075395e": "IN5216",
    "b783336d-488a-4f08-a2c0-c20f1b5d38fc": "IN5220",
    "10a8e2b7-4ed8-490a-a051-31301c2a56f2": "IN5221",
    "1529b6fe-6267-44d2-a2ba-c768d966e61e": "IN5229",
    "8c35113f-0797-4d62-9527-b6c786d43b99": "IN5230",
    "eb443c86-d7e8-4899-9fc8-da71d925399e": "IN5270",
    "1cf5b768-e72b-418d-b5a4-d763c7539cc2": "IN5310",
    "9cc5480b-3ecb-11ea-8017-02437075395e": "IN5320",
    "98033658-877c-4871-8551-2d18581101e8": "IN5322",
    "832db26f-d11c-4fc9-b223-77bc8478a648": "IN5323",
    "2b48dade-e29f-43f5-a606-583f6112e9b0": "IN5324",
    "aa5c4f07-c56a-4242-91a8-e68fc1cbadb5": "IN5325",
    "27f23e53-8a13-44a8-a637-07d94ca2e126": "IN5328",
    "d5874429-f79b-433d-b290-66717331b5e9": "IN5335",
    "c9b3795f-5a4f-4f9b-a8fe-aae244df5610": "IN5336",
    "25f0d228-a9ae-431a-8cc1-5241e0f52082": "IN5340",
    "096ee21b-b463-4a0e-9b73-7a8f9e88957e": "IN5340A",
    "0651dc83-01f7-472b-ad4a-afcb8394d734": "IN5410",
    "8ce9eda7-2c38-4104-8656-4e350e361e5a": "IN5510",
    "dc9cdf10-58e9-47e0-8ced-783a7e1bf07d": "IN5530",
    "f5d1dad4-587b-4bd1-8d33-1f52574c9591": "IN5536",
    "d5683ca6-f02b-4a61-8453-3e04119ba480": "IN5910",
    "4b619c49-8ed6-4fbd-981d-3486091b5110": "IN0310M",
    "ca273143-8395-43db-81ce-76f0697b756e": "IN4117",
    "dc44da0f-097a-4ab6-9d1f-2c14efb410b3": "IN4109",
    "0b440cee-c22f-43a8-be27-8cda7b05a990": "IN4114",
    "03a11d52-135d-4409-aafe-a5c6663497b4": "IN5217",
    "703ff0d0-67ac-4d47-9a4c-314f6a2eda2c": "IN4305",
    "d3ab98ce-00cd-4273-9eb4-68004a81f285": "IN5126",
}

# Reverse lookup so users can filter by IN-code from the Line input.
LINE_TO_UUID = {v: k for k, v in UUID_TO_LINE.items()}

# Product-type classification derived from raw bus_type string. Matches the
# comp-parity dashboard's convention so counts reconcile across dashboards:
#   Seater  = "seater" OR "semi sleeper" (semi-sleeper is a reclining seat)
#             AND does NOT contain a genuine "sleeper" designation
#   Sleeper = "sleeper" and NOT "seater" and NOT "semi sleeper"
#   Hybrid  = dual-cabin bus with BOTH "seater" AND "sleeper" tokens
#             (excludes "semi sleeper" so it doesn't leak in as Hybrid)
#   Volvo   = OEM tag, orthogonal — applies on top of any seat layout
#
# This intentionally departs from Flix's is_seater/is_sleeper boolean encoding
# (which classes semi-sleeper as Hybrid). Using bus_type strings on both
# dashboards guarantees a bus in one is the same category in the other.
def _bt_seater():
    return ("(LOWER(bus_type) LIKE '%seater%' OR LOWER(bus_type) LIKE '%semi sleeper%') "
            "AND NOT (LOWER(bus_type) LIKE '%sleeper%' AND LOWER(bus_type) NOT LIKE '%semi sleeper%')")

def _bt_sleeper():
    return ("LOWER(bus_type) LIKE '%sleeper%' "
            "AND LOWER(bus_type) NOT LIKE '%semi sleeper%' "
            "AND LOWER(bus_type) NOT LIKE '%seater%'")

def _bt_hybrid():
    return ("LOWER(bus_type) LIKE '%seater%' AND LOWER(bus_type) LIKE '%sleeper%' "
            "AND LOWER(bus_type) NOT LIKE '%semi sleeper%'")

PRODUCT_TYPE_CLAUSES = {
    "Seater":  f"((LOWER(bus_product_type) = 'seater')  OR (bus_product_type IS NULL AND ({_bt_seater()})))",
    "Sleeper": f"((LOWER(bus_product_type) = 'sleeper') OR (bus_product_type IS NULL AND ({_bt_sleeper()})))",
    "Hybrid":  f"((LOWER(bus_product_type) = 'hybrid')  OR (bus_product_type IS NULL AND ({_bt_hybrid()})))",
    # Volvo is orthogonal — applies on top of any seat layout.
    "Volvo":   "LOWER(bus_type) LIKE '%volvo%'",
}


def _bq_client():
    sa_info = json.loads(os.environ["SA_CREDENTIALS_JSON"])
    creds   = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return bigquery.Client(project=PROJECT, credentials=creds)


def _sql_string_escape(s: str) -> str:
    return s.replace("'", "''")


def _safe_hour(v) -> int | None:
    """Validate an hour-of-day query param. Returns int 0-23 or None."""
    try:
        h = int(v)
        return h if 0 <= h <= 23 else None
    except (TypeError, ValueError):
        return None


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

    # Product type filter — derived from bus_type string (matches comp-parity)
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
        raw = params["line_code"].strip()
        uuid = LINE_TO_UUID.get(raw.upper())
        if uuid:
            # Input is an IN-code like "IN0421B" — match by service_id UUID prefix
            extra_filters.append(f"AND SUBSTR(service_id, 1, 36) = '{uuid}'")
        else:
            # Treat as a literal service_id
            extra_filters.append(f"AND service_id = '{_sql_string_escape(raw)}'")

    # Hour range filter — applied to the extracted departure hour
    hr_from = _safe_hour(params.get("hr_from"))
    hr_to   = _safe_hour(params.get("hr_to"))
    if hr_from is not None and hr_to is not None:
        extra_filters.append(
            f"AND SAFE_CAST(SUBSTR(departure_time, STRPOS(departure_time, ':') - 2, 2) AS INT64) "
            f"BETWEEN {hr_from} AND {hr_to}"
        )

    extra_filter = " ".join(extra_filters)

    dep_clause_enriched = f"PARSE_DATE('%d-%b-%Y', departure_date) BETWEEN {date_lower_sql} AND {date_upper_sql}"

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
            {extra_filter}
        ),
        -- bus_product_type from bus_inventory_enriched — curated 'seater' /
        -- 'sleeper' / 'hybrid' verdict per service. LEFT JOIN so buses missing
        -- from enriched still flow (fallback in PRODUCT_TYPE_CLAUSES uses the
        -- bus_type string). This is the same pattern comp-parity uses, so a
        -- given bus lands in the same bucket on both dashboards.
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
              ) AS enr_rn
            FROM `redbus-agent-490708.redbus.bus_inventory_enriched`
            WHERE {dep_clause_enriched}
              AND service_id    IS NOT NULL AND service_id != ''
              AND relation_name IS NOT NULL
          )
          WHERE enr_rn = 1
        ),
        joined AS (
          SELECT base.*, enriched.bus_product_type
          FROM base
          LEFT JOIN enriched
            USING (relation_name, departure_date, service_id)
          WHERE base.rn = 1 {product_filter}
        )
        SELECT
          joined.relation_name,
          joined.departure_date,
          joined.departure_time,
          joined.service_id,
          joined.travels_name,
          joined.bus_type,
          joined.is_seater,
          joined.is_sleeper,
          joined.bus_product_type,
          joined.is_ac,
          joined.available_seats,
          joined.total_seats,
          CASE WHEN LOWER(joined.travels_name) LIKE '%flix%'                                                   THEN 'flix'
               WHEN LOWER(joined.travels_name) LIKE '%intrcity%'                                               THEN 'intrcity'
               WHEN LOWER(joined.travels_name) LIKE '%zingbus%' AND LOWER(joined.travels_name) NOT LIKE '%maxx%' THEN 'zingbus'
               WHEN LOWER(joined.travels_name) LIKE '%nuego%'                                                  THEN 'nuego'
               WHEN LOWER(joined.travels_name) LIKE '%freshbus%'                                               THEN 'freshbus'
               WHEN LOWER(joined.travels_name) LIKE '%laxmi holidays%' AND LOWER(joined.travels_name) NOT LIKE '%pvt%' THEN 'laxmi'
               ELSE 'other' END                                                                                AS operator,
          -- Robust hour extraction: STRPOS finds the first colon (HH:MM:SS or YYYY-MM-DD HH:MM:SS),
          -- SUBSTR grabs the 2 chars before it. SAFE_CAST handles bad rows by returning NULL.
          CASE
            WHEN joined.departure_time IS NULL OR STRPOS(joined.departure_time, ':') < 3 THEN 'unknown'
            WHEN SAFE_CAST(SUBSTR(joined.departure_time, STRPOS(joined.departure_time, ':') - 2, 2) AS INT64) BETWEEN 0  AND 5  THEN '00:00-05:59'
            WHEN SAFE_CAST(SUBSTR(joined.departure_time, STRPOS(joined.departure_time, ':') - 2, 2) AS INT64) BETWEEN 6  AND 11 THEN '06:00-11:59'
            WHEN SAFE_CAST(SUBSTR(joined.departure_time, STRPOS(joined.departure_time, ':') - 2, 2) AS INT64) BETWEEN 12 AND 17 THEN '12:00-17:59'
            WHEN SAFE_CAST(SUBSTR(joined.departure_time, STRPOS(joined.departure_time, ':') - 2, 2) AS INT64) BETWEEN 18 AND 23 THEN '18:00-23:59'
            ELSE 'unknown'
          END AS hour_band
        FROM joined
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
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.end_headers()

        try:
            sql, applied = _build_query(params)
            client = _bq_client()
            rows   = [dict(r) for r in client.query(sql).result()]

            by_oh, by_day, by_line = _aggregate(rows)

            # Per-bus rows for the schedule table view (the NP-team primary view).
            # Cap at 15K — practical Vercel response-size + browser DOM limit. Beyond this, filter.
            ROW_CAP = 15000
            per_bus = []
            for r in rows[:ROW_CAP]:
                if r["operator"] == "other":
                    continue
                seats_total = int(r["total_seats"]) if r["total_seats"] else None
                seats_avail = int(r["available_seats"]) if r["available_seats"] is not None else None
                load_pct = None
                if seats_total and seats_avail is not None:
                    load_pct = round((1 - seats_avail / seats_total) * 100, 1)
                # Two-tier classification, mirroring the SQL filter:
                #  1) Prefer bus_product_type from bus_inventory_enriched
                #  2) Fall back to bus_type string parsing when enriched has
                #     no verdict (older scrapes / unmatched services).
                bpt = (r["bus_product_type"] or "").strip().lower() if r.get("bus_product_type") else ""
                if bpt in ("seater", "sleeper", "hybrid"):
                    product = bpt.capitalize()
                else:
                    bt_lower = (r["bus_type"] or "").lower()
                    has_semi    = "semi sleeper" in bt_lower
                    has_seater  = "seater" in bt_lower
                    has_sleeper = "sleeper" in bt_lower and not has_semi
                    if has_seater and has_sleeper:
                        product = "Hybrid"
                    elif has_sleeper:
                        product = "Sleeper"
                    elif has_seater or has_semi:
                        product = "Seater"
                    else:
                        product = "Unknown"
                svc = r["service_id"] or ""
                line_number = UUID_TO_LINE.get(svc[:36]) if r["operator"] == "flix" else None
                per_bus.append({
                    "departure_date": r["departure_date"].isoformat(),
                    "departure_time": r["departure_time"],
                    "operator":       r["operator"],
                    "relation_name":  r["relation_name"],
                    "service_id":     r["service_id"],
                    "line_number":    line_number,
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
