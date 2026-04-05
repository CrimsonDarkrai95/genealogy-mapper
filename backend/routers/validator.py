import re
import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import Keyword, DDL, DML


BLOCKLIST_PATTERN = re.compile(
    r"\b(DROP|DELETE|ALTER|TRUNCATE|CREATE|UPDATE|INSERT|GRANT|REVOKE|EXEC|EXECUTE|COPY|VACUUM|ANALYZE|REINDEX)\b",
    re.IGNORECASE,
)

COMMENT_PATTERN = re.compile(r"(--[^\n]*|/\*.*?\*/)", re.DOTALL)

MAX_QUERY_LENGTH = 4000

ALLOWED_TABLES = {
    "person", "person_alias", "person_external_id",
    "genome", "snp_marker", "genome_snp", "haplogroup_reference",
    "genome_match", "genome_cluster_assignment", "population_cluster",
    "inferred_relationship", "dynasty", "dynasty_membership",
    "region", "person_location_timeline", "historical_conflict",
    "dataset_source",
}


def _strip_comments(sql: str) -> str:
    return COMMENT_PATTERN.sub("", sql)


def _check_select_only(sql: str) -> tuple[bool, str]:
    stripped = sql.strip().rstrip(";")
    parsed = sqlparse.parse(stripped)
    if not parsed:
        return False, "Could not parse SQL"

    statement: Statement = parsed[0]
    first_token = statement.token_first(skip_cm=True)

    if first_token is None:
        return False, "Empty statement"

    if first_token.ttype not in (DML,) or first_token.normalized.upper() != "SELECT":
        return False, f"Only SELECT statements are permitted. Got: {first_token.normalized}"

    return True, ""


def _check_no_mid_semicolons(sql: str) -> tuple[bool, str]:
    stripped = sql.strip().rstrip(";")
    if ";" in stripped:
        return False, "Stacked queries (multiple semicolons) are not permitted"
    return True, ""


def _check_blocklist(sql: str) -> tuple[bool, str]:
    match = BLOCKLIST_PATTERN.search(sql)
    if match:
        return False, f"Blocked keyword detected: {match.group(0).upper()}"
    return True, ""


def _check_length(sql: str) -> tuple[bool, str]:
    if len(sql) > MAX_QUERY_LENGTH:
        return False, f"Query exceeds maximum length of {MAX_QUERY_LENGTH} characters"
    return True, ""


def _check_table_whitelist(sql: str) -> tuple[bool, str]:
    lower = sql.lower()
    # Extract candidate table names using a loose FROM/JOIN pattern
    candidates = re.findall(
        r"(?:from|join)\s+([a-z_][a-z0-9_]*)",
        lower,
    )
    for name in candidates:
        if name not in ALLOWED_TABLES:
            return False, f"Reference to unknown table: {name}"
    return True, ""


def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Returns (is_valid: bool, reason: str).
    reason is empty string on success.
    """
    sql = _strip_comments(sql)

    checks = [
        _check_length(sql),
        _check_no_mid_semicolons(sql),
        _check_blocklist(sql),
        _check_select_only(sql),
        _check_table_whitelist(sql),
    ]

    for passed, reason in checks:
        if not passed:
            return False, reason

    return True, ""