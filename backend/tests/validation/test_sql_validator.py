import pytest
from backend.nl.validator import validate_sql, ValidationError, MAX_QUERY_LENGTH, ALLOWED_TABLES


# ── Return contract ────────────────────────────────────────────────────────
# validate_sql(sql: str) -> str   on success (cleaned SQL, no trailing semicolon)
# validate_sql(sql: str) -> raises ValidationError on failure

def test_returns_string_on_valid():
    result = validate_sql("SELECT person_id FROM person")
    assert isinstance(result, str)

def test_raises_on_invalid():
    with pytest.raises(ValidationError):
        validate_sql("DROP TABLE person")

def test_raises_is_validation_error_not_generic():
    # Must be ValidationError specifically, not a bare Exception
    with pytest.raises(ValidationError) as exc_info:
        validate_sql("DELETE FROM genome")
    assert exc_info.type is ValidationError


# ── Whitespace and case normalisation ──────────────────────────────────────

def test_uppercase_select_passes():
    result = validate_sql("SELECT full_name FROM person")
    assert result.lower().startswith("select")

def test_lowercase_select_passes():
    result = validate_sql("select full_name from person")
    assert isinstance(result, str)

def test_mixed_case_drop_blocked():
    with pytest.raises(ValidationError):
        validate_sql("DrOp TaBlE person")

def test_mixed_case_delete_blocked():
    with pytest.raises(ValidationError):
        validate_sql("dElEtE FROM person")


# ── Trailing semicolon stripping ───────────────────────────────────────────

def test_trailing_semicolon_stripped():
    result = validate_sql("SELECT full_name FROM person;")
    assert not result.endswith(";")

def test_no_semicolon_unchanged_end():
    result = validate_sql("SELECT full_name FROM person")
    assert not result.endswith(";")

def test_mid_semicolon_blocked():
    with pytest.raises(ValidationError):
        validate_sql("SELECT 1; SELECT 2")


# ── Comment stripping and blocking ────────────────────────────────────────

def test_inline_comment_on_valid_query_passes():
    # Comments stripped before check — SELECT survives
    result = validate_sql("SELECT full_name FROM person -- get all names")
    assert isinstance(result, str)

def test_block_comment_on_valid_query_passes():
    result = validate_sql("SELECT full_name /* get names */ FROM person")
    assert isinstance(result, str)

def test_comment_masking_drop_still_blocked():
    # After comment stripping, DROP is still the real statement
    with pytest.raises(ValidationError):
        validate_sql("-- safe\nDROP TABLE person")


# ── Length limit ───────────────────────────────────────────────────────────

def test_at_max_length_boundary():
    # Construct a SELECT that is exactly MAX_QUERY_LENGTH chars
    base = "SELECT full_name FROM person WHERE full_name = '"
    padding = "x" * (MAX_QUERY_LENGTH - len(base) - 1) + "'"
    sql = base + padding
    assert len(sql) == MAX_QUERY_LENGTH
    # Should pass (at limit, not over)
    result = validate_sql(sql)
    assert isinstance(result, str)

def test_over_max_length_blocked():
    long_sql = "SELECT full_name FROM person WHERE " + ("full_name = 'x' OR " * 300)
    assert len(long_sql) > MAX_QUERY_LENGTH
    with pytest.raises(ValidationError):
        validate_sql(long_sql)


# ── Table whitelist ────────────────────────────────────────────────────────

def test_unknown_table_blocked():
    with pytest.raises(ValidationError):
        validate_sql("SELECT * FROM system_credentials")

def test_unknown_table_in_join_blocked():
    with pytest.raises(ValidationError):
        validate_sql("SELECT * FROM person JOIN shadow_table ON 1=1")

def test_all_allowed_tables_pass():
    for table in ALLOWED_TABLES:
        result = validate_sql(f"SELECT * FROM {table} LIMIT 1")
        assert isinstance(result, str), f"Table '{table}' was incorrectly blocked"

def test_subquery_with_allowed_table_passes():
    sql = (
        "SELECT full_name FROM person "
        "WHERE person_id IN (SELECT person_id FROM genome WHERE y_haplogroup = 'R1b')"
    )
    result = validate_sql(sql)
    assert isinstance(result, str)