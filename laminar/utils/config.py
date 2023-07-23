
class ConfigDeployment:
    
    """Required fields in deployment configuration YAML."""
    REQUIRED_STREAMING_FIELDS: list = [
        "beam_runner",
        "beam_streaming",
        "dataflow_job_name",
        "dataflow_project_id",
        "dataflow_region",
        "dataflow_temp_location",
        "dataflow_staging_location",
        "dataflow_subnetwork",
        "dataflow_no_use_public_ips",
        "dataflow_save_main_session",
        "dataflow_machine_type",
        "dataflow_max_num_workers",
        "dataflow_autoscaling_algorithm",
        "dataflow_labels",
        "dataflow_drain_wait_check_interval_seconds",
        "dataflow_drain_wait_max_seconds",
        "dataflow_drain_wait_max_threads",
        "dataflow_running_wait_check_interval_seconds",
        "dataflow_running_wait_max_seconds",
        "dataflow_running_wait_max_threads",
        "dataflow_service_account",
        "dataflow_enable_streaming_engine",
        "dataflow_experiments",
        "pubsub_project_id",
        "pubsub_subscription",
        "bigquery_raw_project_id",
        "bigquery_raw_dataset",
        "bigquery_raw_dead_letter_dataset",
        "bigquery_raw_dead_letter_failures_table",
        "bigquery_l1_project_id",
        "bigquery_l1_schema_bucket",
        "bigquery_l1_schema_folder",
        "bigquery_l1_dead_letter_dataset",
        "bigquery_l1_dead_letter_failures_table",
        "bigquery_l1_dead_letter_unconfigured_table"
    ]
    REQUIRED_BATCH_FIELDS: list = [
        "beam_runner",
        "beam_streaming",
        "dataflow_job_name",
        "dataflow_project_id",
        "dataflow_region",
        "dataflow_temp_location",
        "dataflow_staging_location",
        "dataflow_subnetwork",
        "dataflow_no_use_public_ips",
        "dataflow_save_main_session",
        "dataflow_machine_type",
        "dataflow_max_num_workers",
        "dataflow_autoscaling_algorithm",
        "dataflow_labels",
        "dataflow_drain_wait_check_interval_seconds",
        "dataflow_drain_wait_max_seconds",
        "dataflow_drain_wait_max_threads",
        "dataflow_running_wait_check_interval_seconds",
        "dataflow_running_wait_max_seconds",
        "dataflow_running_wait_max_threads",
        "dataflow_service_account",
        "dataflow_enable_streaming_engine",
        "dataflow_experiments",
        "bigquery_raw_project_id",
        "bigquery_raw_dataset",
        "bigquery_l1_project_id",
        "bigquery_l1_schema_bucket",
        "bigquery_l1_schema_folder",
        "bigquery_l1_dead_letter_dataset",
        "bigquery_l1_dead_letter_failures_table",
        "bigquery_l1_dead_letter_unconfigured_table"
    ]

    @staticmethod
    def check_streaming_deployment_config(deployment_config: dict) -> None:
        """
        Check the validity deployment configuration for streaming pipeline.

        Args:
            deployment_config: Deployment configuration content.
        """
        deployment_fields: list = deployment_config.keys()
        for required_field in ConfigDeployment.REQUIRED_STREAMING_FIELDS:
            if required_field not in deployment_fields:
                raise Exception(f"Field '{required_field}' is required in deployment configuration!")
        print("Deployment configuration is valid!")
    
    @staticmethod
    def check_batch_deployment_config(deployment_config: dict) -> None:
        """
        Check the validity deployment configuration for batch pipeline.

        Args:
            deployment_config: Deployment configuration content.
        """
        deployment_fields: list = deployment_config.keys()
        for required_field in ConfigDeployment.REQUIRED_BATCH_FIELDS:
            if required_field not in deployment_fields:
                raise Exception(f"Field '{required_field}' is required in deployment configuration!")
        print("Deployment configuration is valid!")


class ConfigRaw:
    def __init__(self) -> None:
        """
        Initialize table schema and details for Raw tables.
        """
        self.table_schema: dict = {
            "fields": [
                {"name": "id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "table", "type": "STRING", "mode": "NULLABLE"},
                {"name": "db", "type": "STRING", "mode": "NULLABLE"},
                {"name": "op", "type": "STRING", "mode": "NULLABLE"},
                {"name": "data", "type": "STRING", "mode": "NULLABLE"},
                {"name": "ts", "type": "STRING", "mode": "NULLABLE"},
                {"name": "ts_ms", "type": "INT64", "mode": "NULLABLE"},
                {"name": "metadata", "type": "STRING", "mode": "NULLABLE"},
                {"name": "_ingest_ts_ms", "type": "INT64", "mode": "NULLABLE"},
                {"name": "_date_partition", "type": "DATE", "mode": "NULLABLE"}
            ]
        }
        self.table_details: dict = {
            "timePartitioning": {
                "type": "DAY",
                "field": "_date_partition"
            },
            "requirePartitionFilter": True,
        }
    
    def get_table_schema(self) -> dict:
        """
        Get Raw table schema.

        Returns:
            Table schema of Raw tables.
        """
        return self.table_schema
    
    def get_table_details(self) -> dict:
        """
        Get Raw table details.

        Returns:
            Table details of Raw tables.
        """
        return self.table_details


class ConfigL1:
    def __init__(self) -> None:
        """
        Initialize L1 configurations that will have the following format:
        {
            "<source_db1>.<source_table1>": {
                "table_schema": {...},
                "table_details": {...},
                "custom_functions": {...}
            },
            "<source_db2>.<source_table2>": {
                "table_schema": {...},
                "table_details": {...},
                "custom_functions": {...}
            }, ...
        }
        """
        self.l1_configs: dict = {}

    def register_config(self, l1_config_yaml: dict) -> None:
        """
        Set table configuration for a specific configuration YAML.

        Args:
            l1_config_yaml: L1 YAML configuration of a table.
        """
        l1_table_schema: dict = ConfigL1.construct_table_schema(l1_config_fields=l1_config_yaml["fields"])
        l1_table_details: dict = ConfigL1.construct_table_details(l1_config_yaml=l1_config_yaml)
        custom_functions: dict = ConfigL1.construct_custom_functions(l1_config_yaml=l1_config_yaml)
        table_config: dict = {
            "table_schema": l1_table_schema,
            "table_details": l1_table_details,
            "custom_functions": custom_functions
        }
        source_id: str = f"{l1_table_details['source']['database']}.{l1_table_details['source']['table']}"
        self.l1_configs[source_id] = table_config
    
    @staticmethod
    def construct_table_details(l1_config_yaml: dict) -> dict:
        """
        Construct L1 table details.

        Args:
            l1_config_yaml: L1 YAML configuration of a table.

        Returns:
            Table details for an L1 table.
        """
        return {key: value for key, value in l1_config_yaml.items() if key != "fields" and key != "custom_functions"}
 
    @staticmethod
    def construct_table_schema(l1_config_fields: dict) -> dict:
        """
        Construct L1 table schema.

        Args:
            l1_config_fields: The "fields" dictionary in a YAML configuration.

        Returns:
            Table schema for an L1 table.
        """
        table_schema: dict = {}
        for column_schema in l1_config_fields:
            source_column: str = column_schema["source"]
            is_nested_column: bool = True if "fields" in column_schema else False
            if is_nested_column:
                nested_schema: dict = ConfigL1.construct_table_schema(l1_config_fields=column_schema["fields"])
                nested_column_schema: dict = {
                    key: value for key, value in column_schema.items()
                    if key != "fields"
                }
                nested_column_schema["fields"] = nested_schema
                table_schema[source_column] = nested_column_schema
            else:
                table_schema[source_column] = column_schema
        return table_schema

    @staticmethod
    def construct_custom_functions(l1_config_yaml: dict) -> dict:
        """
        Construct custom functions mapping specified in the YAML.

        Args:
            l1_config_yaml: L1 YAML configuration of a table.
        
        Returns:
            Custom functions map having the following format:
            {
                <custom_function1>: [<column1>, ...], 
                <custom_function2>: [<column2>, ...], 
                ...
            }
        """
        return l1_config_yaml["custom_functions"] if "custom_functions" in l1_config_yaml else {}

    def is_source_exist(self, source_id: str) -> bool:
        """
        Check whether the source_id is registered in L1 configurations

        Args:
            source_id: Source table ID having the following format "<source_db>.<source_table>".
        
        Returns:
            Boolean indicating whether the source_id is registered in L1 configurations.
        """
        return True if source_id in self.l1_configs else False
    
    def get_table_config(self, source_id: str) -> dict:
        """
        Get L1 table configuration for a specific source_id.

        Args:
            source_id: Source table ID having the following format "<source_db>.<source_table>".
        
        Returns:
            Table configuration for a specific source_id.
        """
        return self.l1_configs[source_id]
