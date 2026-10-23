"""Member Extract at eligibility-event grain; source tables are read-only.

Qualify current ACTIVE Medicare coverages on an inclusive business-date interval,
then return ALL eligibility records for those coverage combinations. These are
source lifecycle records, not reconstructed or coalesced enrollment intervals.
Dimensions use their current SCD2 version (__END_AT IS NULL); their attributes
describe the latest known state, not the state at each historical event date.
"""

import argparse
# import csv  # Enable with the CSV export block.
from datetime import date, datetime
# from pathlib import Path  # Enable with the CSV export block.
import re
import os
from zoneinfo import ZoneInfo


COVERAGE_KEYS = (
    "member_id", "product_id", "benefit_plan_id", "group_id", "subgroup_id"
)
ELIGIBILITY_COLUMNS = (
    "eligibility_id", "member_id", "subscriber_id", "family_id",
    "relationship_to_subscriber", "group_id", "subgroup_id", "product_id",
    "benefit_plan_id", "line_of_business", "product_type", "network_id",
    "coverage_tier", "coverage_start_date", "coverage_end_date",
    "eligibility_status", "event_type", "event_effective_date", "transaction_date",
    "termination_reason", "retroactive_flag", "future_enrollment_flag",
    "reinstatement_flag", "medical_coverage_flag", "pharmacy_coverage_flag",
    "dental_coverage_flag", "vision_coverage_flag", "record_version",
    "source_current_record_flag", "source_system", "source_eligibility_key",
)
DIMENSIONS = {
    "member": ("member_id", "m", (
        "first_name", "middle_initial", "last_name", "date_of_birth", "gender",
        "marital_status", "preferred_language", "phone_number", "email_address",
        "member_status", "deceased_flag", "death_date",
    )),
    "product": ("product_id", "p", ("product_code", "product_name")),
    "benefit_plan": ("benefit_plan_id", "bp", ("plan_code", "plan_name")),
    "group": ("group_id", "g", ("group_number", "group_name")),
    "subgroup": ("subgroup_id", "sg", ("subgroup_code", "subgroup_name")),
}


def member_extract_sql(catalog="payer_01_dev", schema="silver", as_of_date=None):
    """Build a SELECT only. Date defaults to today's America/New_York date."""
    
    for identifier in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Invalid catalog/schema identifier: {identifier!r}")
    
    business_date = (
        date.fromisoformat(str(as_of_date)) if as_of_date is not None
        else datetime.now(ZoneInfo("America/New_York")).date()
    )
    
    source = f"`{catalog}`.`{schema}`"
    match = " AND ".join(f"e.{key} <=> a.{key}" for key in COVERAGE_KEYS)
    columns = [f"e.{column}" for column in ELIGIBILITY_COLUMNS]
    joins = []
    
    for table, (key, alias, attributes) in DIMENSIONS.items():
        columns.extend(f"{alias}.{column}" for column in attributes)
        joins.append(
            f"LEFT JOIN {source}.`{table}` {alias} "
            f"ON e.{key} = {alias}.{key} AND {alias}.__END_AT IS NULL"
        )
    
    columns.append(f"DATE '{business_date.isoformat()}' AS extract_as_of_date")
    projection = ",\n    ".join(columns)
    dimension_joins = "\n".join(joins)
    
    return f"""WITH active_coverages AS (
    
    SELECT DISTINCT {', '.join(COVERAGE_KEYS)}
    FROM {source}.`eligibility`
    WHERE source_current_record_flag = true
      AND upper(trim(line_of_business)) = 'MEDICARE'
      AND upper(trim(eligibility_status)) = 'ACTIVE'
      AND coverage_start_date <= DATE '{business_date.isoformat()}'
      AND (coverage_end_date IS NULL
           OR coverage_end_date >= DATE '{business_date.isoformat()}')
), qualifying_history AS (
    SELECT e.*
    FROM {source}.`eligibility` e
    LEFT SEMI JOIN active_coverages a ON {match}
)
SELECT
    {projection}
FROM qualifying_history e
{dimension_joins}"""


def build_member_extract(spark, catalog="payer_01_dev", schema="silver", as_of_date=None):
    """Return a DataFrame without writing tables or files.

    Fail on duplicate current dimension keys instead of silently multiplying
    enrollment records or choosing an arbitrary dimension version.
    """
    from pyspark.sql import functions as F

    sql = member_extract_sql(catalog, schema, as_of_date)

    print(f"member extract sql is : \n {sql}")

    for table, (key, _, _) in DIMENSIONS.items():
        duplicates = (
            spark.table(f"`{catalog}`.`{schema}`.`{table}`")
            .filter(F.col("__END_AT").isNull())
            .groupBy(key).count().filter(F.col("count") > 1)
        )
        if duplicates.limit(1).count():
            raise ValueError(f"Duplicate current {table}.{key}; extract would fan out")
    return spark.sql(sql)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="payer_01_dev")
    parser.add_argument("--source-schema", default="silver")
    parser.add_argument("--extract-schema", default="extract_stg")
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--profile", default="DATABRICKS_FREE")
    parser.add_argument("--output-dir", help="Reserved for CSV export, which is currently disabled")
    args = parser.parse_args()

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.extract_schema):
        parser.error("Invalid extract schema identifier")
    
    if args.extract_schema.lower() == args.source_schema.lower():
        parser.error("Extract schema must differ from source schema")
    
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    else:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.profile(args.profile).getOrCreate()

    result = build_member_extract(spark, args.catalog, args.source_schema, args.as_of_date)
    destination_schema = f"`{args.catalog}`.`{args.extract_schema}`"
    destination = f"{destination_schema}.`member_extract`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {destination_schema}")
    
    # Full refresh of the staging snapshot, including when no coverage qualifies.
    result.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(destination)

    print(f"Member Extract saved to {destination}")
    # CSV export disabled for deployment; retained below for future use.
    # if args.output_dir:
    #     output_dir = Path(args.output_dir)
    # # Retained for future Databricks volume exports; disabled for now.
    # # elif os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    # #     spark.sql(f"CREATE VOLUME IF NOT EXISTS {destination_schema}.`output_extracts`")
    # #     output_dir = Path(f"/Volumes/{args.catalog}/{args.extract_schema}/output_extracts")
    # else:
    #     output_dir = Path("output_extracts")
    #
    # output_dir.mkdir(parents=True, exist_ok=True)
    # timestamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d_%H%M%S")
    # csv_path = output_dir / f"member_extract_{timestamp}.csv"
    # # Export the persisted snapshot, rather than recomputing the source query.
    # snapshot = spark.table(destination)
    # created = False
    # try:
    #     with csv_path.open("x", newline="", encoding="utf-8") as handle:
    #         created = True
    #         writer = csv.writer(handle)
    #         writer.writerow(snapshot.columns)
    #         for row in snapshot.toLocalIterator():
    #             writer.writerow(row)
    # except Exception:
    #     if created:
    #         csv_path.unlink(missing_ok=True)
    #     raise
    # print(f"Member Extract CSV saved to {csv_path}")


if __name__ == "__main__":
    main()
