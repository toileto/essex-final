
class BigQueryUtility:

    """BigQuery type mappings."""
    INTEGER_TYPES: list = ["INT64", "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT"]
    FLOAT_TYPES: list = ['FLOAT64', 'FLOAT']
    BOOLEAN_TYPES: list = ["BOOLEAN", "BOOL"]
    TIMESTAMP_TYPES: list = ["TIMESTAMP"]
    DATE: list = ["DATE"]
    EMPTY_STRING: list = [""]

    @staticmethod
    def construct_backfill_query(
        raw_table_id: str, 
        l1_table_id: str, 
        start_date: str, 
        end_date: str
    ) -> str:
        """
        Construct query to retrieve data from Raw Datalake.

        Args:
            raw_table_id: Raw table ID having the following format "<project>:<dataset>.<table>"
            l1_table_id: L1 table ID having the following format "<project>:<dataset>.<table>"
            start_date: Start date to retrieve data from source.
            end_date: End date to retrieve data from source.

        Returns:
            Constructed query to get retrieve data from Raw Datalake.
        """
        return f"""
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
