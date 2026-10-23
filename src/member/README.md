# Member Extract

Inspected `payer_01_dev.silver` through CLI profile `DATABRICKS_FREE` on
2026-10-23 using metadata and read-only SQL. No source tables were modified.

## Relationships and selected columns

| Table | Join / purpose | Selected columns |
| --- | --- | --- |
| eligibility | Driving event/history table | Eligibility ID, member/subscriber/family IDs, relationship, group/subgroup/product/plan/network IDs, line of business, product type, tier, coverage dates, status, event type/effective date, transaction date, termination reason, lifecycle and benefit flags, version, current flag, source system/key |
| member | `eligibility.member_id = member.member_id` | Name, birth date, gender, marital status, language, phone/email, member status, deceased flag/death date |
| product | `eligibility.product_id = product.product_id` | Product code/name |
| benefit_plan | `eligibility.benefit_plan_id = benefit_plan.benefit_plan_id` | Plan code/name |
| group | `eligibility.group_id = group.group_id` | Group number/name |
| subgroup | `eligibility.subgroup_id = subgroup.subgroup_id` | Subgroup code/name |

All dimension joins are LEFT joins with `__END_AT IS NULL`. This preserves
eligibility history when an enrichment is absent and avoids obsolete SCD2
versions. Dimension values are latest-known attributes, not historical event-time
attributes. Eligibility retains its own historical product, plan and group IDs.
The Python constants enumerate the exact selected column names.

Other inspected tables: `address`, `entity_address`, `claim_header`, `claim_line`,
`practitioner`, `practitioner_role`, `supplier`, `supplier_location`. They are not
needed for coverage qualification or the baseline member/enrollment projection.
Address enrichment would require a separate rule choosing address type, primary
flag and effective dates via the polymorphic entity-address bridge; it is not
joined implicitly. No member-to-PCP assignment table was present.

## Business logic

The user confirmed coverage identity as the combination of `member_id`,
`product_id`, `benefit_plan_id`, `group_id`, `subgroup_id`. Silver has no explicit
stable coverage ID: all 225,600 eligibility IDs are unique event IDs, rather than
IDs shared across lifecycle versions. Sample reinstatement histories retain
the coverage combination while changing eligibility IDs and coverage dates.

1. Find source-current eligibility rows whose normalized line of business is
   `MEDICARE`, normalized status is `ACTIVE`, start date is on/before the as-of
   date, and end date is null or on/after it. Boundaries are inclusive.
2. Select distinct qualifying coverage combinations.
3. Null-safe semi-join the full eligibility table to these combinations. Do not
   filter returned history on status, current flag, version, or coverage dates.
   Do not join on member alone, which would pull different coverage combinations.
4. Enrich with current dimensions. Do not use member status, product/plan status,
   or dimension effective dates to remove qualifying enrollment history.

The output grain is one **source eligibility event**, including corrections and
demographic events. It preserves all source enrollment-period records; it does
not infer nonoverlapping intervals, deduplicate repeated date pairs, or treat
superseded open-ended events as independently active coverages. Status `FUTURE`
is excluded from qualification even if its start date has passed.

Profiling for 2026-10-23 found 5,236 qualifying current rows and 13,794 matching
history rows, versus 15,620 history rows if joined by member alone. There were
no duplicate member/version pairs, duplicate current member IDs, missing active
members, missing product IDs, plan/product inconsistencies, or subgroup/group
inconsistencies in the checks performed.

The generated extract SQL executed successfully against the warehouse: 13,794
rows and 13,794 distinct eligibility IDs across 5,192 members, including 8,558
noncurrent records and 625 terminated history records. Equal row/ID counts
confirm the dimension joins did not multiply these records. Python syntax,
SQL generation, date override, and identifier validation also passed locally.

## Run

From the repository root with Databricks Connect installed and authenticated:

```powershell
.venv/Scripts/python.exe src/member/member_extract.py --profile DATABRICKS_FREE
```

Optional: `--catalog payer_01_dev --source-schema silver --as-of-date 2026-10-23`.
The default date is evaluated in `America/New_York`. In a Databricks runtime,
the module uses the runtime Spark session; local execution uses Connect.
`build_member_extract(spark, ...)` returns a DataFrame for downstream consumers;
the command line saves a managed Delta snapshot to
`<catalog>.extract_stg.member_extract`. Every run overwrites the previous snapshot,
including an empty result, and allows the output schema to be refreshed. The
destination schema is created if absent. Source tables remain read-only.
The executing identity needs permission to create the schema/table and write
the destination table. Use `--extract-schema` to override the staging schema.

`resources/member_extract_job.yml` passes the bundle's `catalog_name`,
`source_schema`, and `extract_schema` variables to the script. The dev/test/prod
targets therefore read their respective catalog's Silver tables and write to
that same catalog's `extract_stg.member_extract` table. Local runs can select the
catalog with `--catalog payer_01_dev` (the default).

## CSV export disabled

CSV file-writing code and the Databricks volume code are retained as comments.
No CSV files, local folders, or volumes are created by the script. This applies
to both laptop and Databricks job runs. The `--output-dir` argument is retained
for compatibility but has no effect while CSV export is disabled.

The Delta overwrite creates the table on first run and fully replaces its data
and schema on subsequent runs, including empty extracts. It does not append
records or require a separate TRUNCATE/DROP operation.
