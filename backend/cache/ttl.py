# cache/ttl.py

class TTL:
    QUERY_RESULT   = 900    # 15 min  — generic route responses
    ANCESTRY_PATH  = 1800   # 30 min  — recursive CTE results
    NL2SQL         = 3600   # 60 min  — translated SQL strings
    HAPLOGROUP     = 3600   # 60 min  — haplogroup member sets
    CLUSTER        = 3600   # 60 min  — cluster member sets
    SEARCH         = 600    # 10 min  — FTS results
    RATE_LIMIT     = 60     # 60 sec  — per-minute sliding window