from laminar.utils.bigquery import BigQueryUtility


def strip_spaces(query: str) -> str:
    return " ".join(line.strip() for line in query.splitlines()).strip()

def test_BigQueryUtility_construct_backfill_query():
    raw_table_id = "raw_table",
    l1_table_id = "l1_table",
    start_date = "2022-01-01",
    end_date = "2023-01-01"
    actual: str = strip_spaces(BigQueryUtility.construct_backfill_query(
        raw_table_id=raw_table_id,
        l1_table_id=l1_table_id,
        start_date=start_date,
        end_date=end_date
    ))
    expected: str = strip_spaces(f"""
        WITH raw_parsed_ts AS(
            SELECT 
                *,
                CASE 
                    WHEN SAFE_CAST(ts AS INT64) IS NULL THEN CAST(ts AS TIMESTAMP)
                    ELSE TIMESTAMP_SECONDS(CAST(ts AS INT64))
                END AS parsed_ts
            FROM `{raw_table_id}`
            WHERE _date_partition BETWEEN "{start_date}" AND "{end_date}"
        )
        SELECT * EXCEPT(parsed_ts)
        FROM raw_parsed_ts
        WHERE CONCAT(id, parsed_ts) IN (
            SELECT CONCAT(id, parsed_ts)
            FROM raw_parsed_ts
            EXCEPT DISTINCT 
            SELECT CONCAT(_raw_id, _raw_ts)
            FROM `{l1_table_id}`
            WHERE DATE(_raw_ts) BETWEEN "{start_date}" AND "{end_date}"
        )
        """
    )
    assert actual == expected
