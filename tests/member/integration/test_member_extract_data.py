import os

from databricks.connect import DatabricksSession


CATALOG = os.environ["TEST_CATALOG"]
SOURCE_SCHEMA = "silver"
EXTRACT_SCHEMA = "extract_stg"

EXTRACT_TABLE = f"{CATALOG}.{EXTRACT_SCHEMA}.member_extract"
ELIGIBILITY_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.eligibility"


def get_spark():
    return DatabricksSession.builder.getOrCreate()


def test_member_extract_is_populated():
    spark = get_spark()

    row_count = spark.sql(f"""
        SELECT COUNT(*) AS row_count
        FROM {EXTRACT_TABLE}
    """).first()["row_count"]

    assert row_count > 0


def test_member_extract_has_one_as_of_date():
    spark = get_spark()

    result = spark.sql(f"""
        SELECT
            COUNT(DISTINCT extract_as_of_date) AS distinct_dates,
            SUM(
                CASE
                    WHEN extract_as_of_date IS NULL THEN 1
                    ELSE 0
                END
            ) AS null_dates
        FROM {EXTRACT_TABLE}
    """).first()

    assert result["distinct_dates"] == 1
    assert result["null_dates"] == 0


def test_all_extract_coverages_are_qualified_active_medicare():
    spark = get_spark()

    invalid_count = spark.sql(f"""
        WITH as_of_date AS (
            SELECT MAX(extract_as_of_date) AS business_date
            FROM {EXTRACT_TABLE}
        ),

        active_coverages AS (
            SELECT DISTINCT
                e.member_id,
                e.product_id,
                e.benefit_plan_id,
                e.group_id,
                e.subgroup_id
            FROM {ELIGIBILITY_TABLE} e
            CROSS JOIN as_of_date d
            WHERE e.source_current_record_flag = true
              AND upper(trim(e.line_of_business)) = 'MEDICARE'
              AND upper(trim(e.eligibility_status)) = 'ACTIVE'
              AND e.coverage_start_date <= d.business_date
              AND (
                    e.coverage_end_date IS NULL
                    OR e.coverage_end_date >= d.business_date
                  )
        ),

        extract_coverages AS (
            SELECT DISTINCT
                member_id,
                product_id,
                benefit_plan_id,
                group_id,
                subgroup_id
            FROM {EXTRACT_TABLE}
        )

        SELECT COUNT(*) AS invalid_count
        FROM extract_coverages x
        LEFT ANTI JOIN active_coverages a
          ON x.member_id       <=> a.member_id
         AND x.product_id      <=> a.product_id
         AND x.benefit_plan_id <=> a.benefit_plan_id
         AND x.group_id        <=> a.group_id
         AND x.subgroup_id     <=> a.subgroup_id
    """).first()["invalid_count"]

    assert invalid_count == 0


def test_all_eligibility_history_is_in_extract():
    spark = get_spark()

    missing_count = spark.sql(f"""
        WITH as_of_date AS (
            SELECT MAX(extract_as_of_date) AS business_date
            FROM {EXTRACT_TABLE}
        ),

        active_coverages AS (
            SELECT DISTINCT
                e.member_id,
                e.product_id,
                e.benefit_plan_id,
                e.group_id,
                e.subgroup_id
            FROM {ELIGIBILITY_TABLE} e
            CROSS JOIN as_of_date d
            WHERE e.source_current_record_flag = true
              AND upper(trim(e.line_of_business)) = 'MEDICARE'
              AND upper(trim(e.eligibility_status)) = 'ACTIVE'
              AND e.coverage_start_date <= d.business_date
              AND (
                    e.coverage_end_date IS NULL
                    OR e.coverage_end_date >= d.business_date
                  )
        ),

        expected_history AS (
            SELECT e.eligibility_id
            FROM {ELIGIBILITY_TABLE} e
            LEFT SEMI JOIN active_coverages a
              ON e.member_id       <=> a.member_id
             AND e.product_id      <=> a.product_id
             AND e.benefit_plan_id <=> a.benefit_plan_id
             AND e.group_id        <=> a.group_id
             AND e.subgroup_id     <=> a.subgroup_id
        ),

        actual_history AS (
            SELECT DISTINCT eligibility_id
            FROM {EXTRACT_TABLE}
        )

        SELECT COUNT(*) AS missing_count
        FROM expected_history e
        LEFT ANTI JOIN actual_history a
          ON e.eligibility_id = a.eligibility_id
    """).first()["missing_count"]

    assert missing_count == 0