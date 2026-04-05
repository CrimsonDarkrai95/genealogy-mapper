import pytest
from backend.nl.validator import validate_sql, ValidationError


# ── Valid SELECT statements — must return cleaned SQL string ──────────────

def test_valid_simple_select():
    result = validate_sql("SELECT full_name FROM person WHERE is_historical = true")
    assert isinstance(result, str)
    assert result.lower().startswith("select")

def test_valid_select_with_join():
    sql = (
        "SELECT p.full_name, r.region_name "
        "FROM person p JOIN region r ON p.birth_region_id = r.region_id "
        "WHERE p.birth_year < -300"
    )
    result = validate_sql(sql)
    assert isinstance(result, str)

def test_valid_select_with_limit():
    result = validate_sql("SELECT person_id, full_name FROM person LIMIT 50")
    assert isinstance(result, str)

def test_valid_select_with_order():
    result = validate_sql("SELECT full_name, birth_year FROM person ORDER BY birth_year ASC")
    assert isinstance(result, str)

def test_valid_select_strips_trailing_semicolon():
    result = validate_sql("SELECT full_name FROM person;")
    assert not result.endswith(";")


# ── Destructive statements — must raise ValidationError ───────────────────

def test_blocks_drop():
    with pytest.raises(ValidationError):
        validate_sql("DROP TABLE person")

def test_blocks_delete():
    with pytest.raises(ValidationError):
        validate_sql("DELETE FROM person WHERE person_id = 'abc'")

def test_blocks_update():
    with pytest.raises(ValidationError):
        validate_sql("UPDATE person SET full_name = 'hacked' WHERE 1=1")

def test_blocks_truncate():
    with pytest.raises(ValidationError):
        validate_sql("TRUNCATE TABLE genome")

def test_blocks_alter():
    with pytest.raises(ValidationError):
        validate_sql("ALTER TABLE person ADD COLUMN pwned TEXT")


# ── Injection patterns — must raise ValidationError ───────────────────────

def test_blocks_stacked_query():
    with pytest.raises(ValidationError):
        validate_sql("SELECT 1; DROP TABLE person")

def test_blocks_comment_injection():
    with pytest.raises(ValidationError):
        validate_sql("DROP TABLE person -- safe comment")

def test_blocks_block_comment():
    with pytest.raises(ValidationError):
        validate_sql("DROP TABLE person /* injected */")

def test_blocks_unknown_table():
    with pytest.raises(ValidationError):
        validate_sql("SELECT * FROM secret_admin_table")

def test_blocks_oversized_query():
    long_sql = "SELECT full_name FROM person WHERE " + ("full_name = 'x' OR " * 300)
    with pytest.raises(ValidationError):
        validate_sql(long_sql)