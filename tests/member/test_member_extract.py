import pytest

from src.member.member_extract import member_extract_sql


def test_member_extract_sql_uses_requested_date():
    sql = member_extract_sql(
        catalog="payer_01_dev",
        schema="silver",
        as_of_date="2026-10-23",
    )

    assert "coverage_start_date <= DATE '2026-10-23'" in sql
    assert "coverage_end_date >= DATE '2026-10-23'" in sql


def test_member_extract_sql_filters_medicare():
    sql = member_extract_sql(as_of_date="2026-10-23")

    assert "upper(trim(line_of_business)) = 'MEDICARE'" in sql


def test_member_extract_sql_filters_active_coverage():
    sql = member_extract_sql(as_of_date="2026-10-23")

    assert "upper(trim(eligibility_status)) = 'ACTIVE'" in sql


def test_member_extract_sql_returns_all_history_for_active_coverage():
    sql = member_extract_sql(as_of_date="2026-10-23")

    assert "LEFT SEMI JOIN active_coverages" in sql
    assert "SELECT e.*" in sql


def test_member_extract_sql_uses_requested_catalog_and_schema():
    sql = member_extract_sql(
        catalog="payer_01_test",
        schema="silver",
        as_of_date="2026-10-23",
    )

    assert "`payer_01_test`.`silver`.`eligibility`" in sql


def test_member_extract_sql_rejects_invalid_catalog_name():
    with pytest.raises(ValueError):
        member_extract_sql(
            catalog="payer-01-dev",
            schema="silver",
            as_of_date="2026-10-23",
        )