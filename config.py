PROJECT_ID = "redbus-agent-490708"
DATASET    = "redbus"
TABLE      = "bus_inventory"

FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"

OPERATORS = {
    "flix":      ["flix"],
    "intrcity":  ["intrcity"],
    "zingbus":   ["zingbus"],
    "nuego":     ["nuego"],
    "freshbus":  ["freshbus"],
    "laxmi":     ["laxmi holidays"],
}

MIN_ROWS_PER_DAY    = 150_000
MIN_FORWARD_DAYS    = 29
HISTORY_START_DATE  = None
