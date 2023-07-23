import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import re

from laminar.utils.bigquery import BigQueryUtility
from laminar.utils.config import ConfigDeployment, ConfigL1
from laminar.utils.gcs import GCSUtility
from laminar.utils.pipeline import PipelineUtility
from laminar.utils.yaml import YAMLUtility


def main(
    deployment_config_path: str, 
    dataflow_container: str, 
    table: str, 
    start_date: str, 
    end_date: str, 
    dryrun: bool = False
) -> None:
    """
    Entrypoint for batch pipeline deployment.

    Args:
        deployment_config_path: Deployment configuration path.
        dataflow_container: Docker registry location of container image to be used by the Dataflow worker.
        table: Destination BigQuery table.
        start_date: Start date to retrieve data from source.
        end_date: End date to retrieve data from source.
        dryrun: If True then pipeline deployment will be skipped.
    """
    if not dryrun:
        assert "GOOGLE_APPLICATION_CREDENTIALS" in os.environ, \
            "Please set the GOOGLE_APPLICATION_CREDENTIALS environment variable"
        
    deployment_config: dict = YAMLUtility.read_yaml_from_file(file_path=deployment_config_path)

    bigquery_l1_schema_bucket: str = deployment_config["bigquery_l1_schema_bucket"]
    bigquery_l1_schema_folder: str = deployment_config["bigquery_l1_schema_folder"]
    bigquery_raw_project: str = deployment_config["bigquery_raw_project_id"]
    bigquery_raw_dataset: str = deployment_config["bigquery_raw_dataset"]
    bigquery_l1_project: str = deployment_config["bigquery_l1_project_id"]

    deployment_config["dataflow_job_name"] = f'{deployment_config["dataflow_job_name"]}-{table.replace("_", "-")}'

    ConfigDeployment.check_batch_deployment_config(deployment_config=deployment_config)

    pipeline_options: PipelineOptions = PipelineUtility.construct_pipeline_options(
        deployment_config=deployment_config,
        dataflow_container=dataflow_container
    )

    gcs = GCSUtility()
    l1_config_files: list = gcs.list_files(
        bucket_name=bigquery_l1_schema_bucket,
        folder_path=bigquery_l1_schema_folder
    )

    l1_configs: ConfigL1 = ConfigL1()
    
    for l1_config_file in l1_config_files:
        if l1_config_file.split("/")[-1].split(".")[0] == table \
            and re.search(".(yaml|yml)", l1_config_file):
            l1_config_content: str = gcs.load_as_string(
                bucket_name=bigquery_l1_schema_bucket,
                blob_path=l1_config_file
            )
            l1_config_yaml: dict = YAMLUtility.read_yaml_from_string(content=l1_config_content)
            l1_configs.register_config(l1_config_yaml=l1_config_yaml)

            source_db: str = l1_config_yaml["source"]["database"]
            source_table: str = l1_config_yaml["source"]["table"]
            bigquery_raw_table: str = f"{source_db}_{source_table}"

            bigquery_l1_dataset: str = l1_config_yaml["destination"]["dataset"]
            bigquery_l1_table: str = l1_config_yaml["destination"]["table"]

            raw_table_id: str = f"{bigquery_raw_project}.{bigquery_raw_dataset}.{bigquery_raw_table}"
            l1_table_id: str = f"{bigquery_l1_project}.{bigquery_l1_dataset}.{bigquery_l1_table}"
            break

    batch_query: str = BigQueryUtility.construct_backfill_query(
        raw_table_id=raw_table_id,
        l1_table_id=l1_table_id,
        start_date=start_date,
        end_date=end_date
    )

    destination_table_id: str = f'{bigquery_l1_project}:{bigquery_l1_dataset}.{table}'

    pipeline: beam.Pipeline = PipelineUtility.construct_batch_pipeline(
        pipeline_options=pipeline_options,
        deployment_config=deployment_config,
        l1_configs=l1_configs,
        batch_query=batch_query,
        destination_table_id=destination_table_id
    )

    if not dryrun:
        pipeline.run()
    else:
        print("run() is not called since dryrun is true.")
