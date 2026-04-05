import hashlib


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


# Query result cache
def person_key(person_id: str) -> str:
    return f"qcache:person:{person_id}"

def genome_key(person_id: str) -> str:
    return f"qcache:genome:{person_id}"

def matches_key(person_id: str, params_hash: str) -> str:
    return f"qcache:matches:{person_id}:{params_hash}"

def lineage_key(person_id: str) -> str:
    return f"qcache:lineage:{person_id}"

def match_key(match_id: str) -> str:
    return f"qcache:match:{match_id}"

def relationship_key(rel_id: str) -> str:
    return f"qcache:rel:{rel_id}"

def historical_key(params_hash: str) -> str:
    return f"qcache:historical:{params_hash}"

def search_key(query: str) -> str:
    return f"qcache:search:{_sha(query)}"

def timeline_key(person_id: str, from_year: int, to_year: int) -> str:
    return f"qcache:timeline:{person_id}:{from_year}:{to_year}"

# Ancestry path cache
def key_ancestry(person_id: str) -> str:
    return f"ancestry:{person_id}"

# NL-to-SQL translation cache
def key_nl2sql(nl_query: str) -> str:
    return f"nl2sql:{_sha(nl_query)}"

# Haplogroup members cache
def key_haplogroup(haplogroup_code: str) -> str:
    return f"haplo:{haplogroup_code}"

# Cluster members cache
def key_cluster(cluster_id: str) -> str:
    return f"cluster:{cluster_id}"

# Rate limiting
def key_rate_limit(api_key: str, window_minute: int) -> str:
    return f"ratelimit:{api_key}:{window_minute}"

# Params hash helper
def params_hash(*args) -> str:
    return _sha(":".join(str(a) for a in args))

# haplogroup members cache key
def haplogroup_key(code: str) -> str:
    return f"haplo:{code}"