import re
import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import Keyword, DDL, DML

ALLOWED_TABLES = {
    "person", "person_alias", "person_external_id",
    "genome", "snp_marker", "genome_snp", "haplogroup_reference",
    "genome_match", "population_cluster", "genome_cluster_assignment",
    "inferred_relationship",
    "dynasty", "dynasty_membership",
    "region", "person_location_timeline", "historical_conflict",
    "dataset_source",
}

BLOCKED_KEYWORDS = {
    "DROP", "DELETE", "ALTER", "TRUNCATE", "UPDATE",
    "INSERT", "CREATE", "GRANT", "REVOKE", "REPLACE",
    "EXEC", "EXECUTE", "CALL", "COPY",
}

MAX_QUERY_LENGTH = 4000


class ValidationError(Exception):
    pass


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return sql.strip()


def _check_starts_with_select(sql: str) -> None:
    if not re.match(r"^\s*SELECT\b", sql, re.IGNORECASE):
        raise ValidationError("Only SELECT statements are permitted.")


def _check_no_mid_semicolons(sql: str) -> None:
    # Strip trailing semicolon, then check for any remaining
    cleaned = sql.rstrip().rstrip(";")
    if ";" in cleaned:
        raise ValidationError("Stacked queries (multiple semicolons) are not permitted.")


def _check_blocked_keywords(sql: str) -> None:
    # Tokenise with sqlparse for accurate keyword detection
    parsed = sqlparse.parse(sql)
    for statement in parsed:
        for token in statement.flatten():
            if token.ttype in (Keyword, DDL, DML):
                if token.normalized.upper() in BLOCKED_KEYWORDS:
                    raise ValidationError(
                        f"Blocked keyword detected: {token.normalized.upper()}"
                    )


def _check_length(sql: str) -> None:
    if len(sql) > MAX_QUERY_LENGTH:
        raise ValidationError(
            f"Query exceeds maximum length of {MAX_QUERY_LENGTH} characters."
        )


def _check_table_whitelist(sql: str) -> None:
    # Extract all word tokens that could be table names
    # Look for FROM and JOIN clauses
    table_pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE
    )
    referenced = {m.group(1).lower() for m in table_pattern.finditer(sql)}
    disallowed = referenced - ALLOWED_TABLES
    if disallowed:
        raise ValidationError(
            f"Query references disallowed table(s): {', '.join(disallowed)}"
        )


def validate_sql(raw_sql: str) -> str:
    """
    Validates and cleans a Gemini-generated SQL string.
    Returns the cleaned SQL string on success.
    Raises ValidationError with a descriptive message on failure.
    """
    # Step 1: strip comments
    sql = _strip_comments(raw_sql)

    # Step 2: strip trailing semicolon for clean storage
    sql = sql.rstrip().rstrip(";").strip()

    # Step 3: run all checks
    _check_length(sql)
    _check_starts_with_select(sql)
    _check_no_mid_semicolons(sql)
    _check_blocked_keywords(sql)
    _check_table_whitelist(sql)

    return sql