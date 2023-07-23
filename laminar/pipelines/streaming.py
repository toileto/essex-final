import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import re

from laminar.utils.config import ConfigDeployment, ConfigRaw, ConfigL1
from laminar.utils.gcs import GCSUtility
from laminar.utils.pipeline import PipelineUtility
from laminar.utils.yaml import YAMLUtility


def main(
    deployment_config_path: str, 
    dataflow_container: str, 
    dryrun: bool = False
) -> None:
    """
    Entrypoint for streaming pipeline deployment.

    Args:
        deployment_config_path: Deployment configuration path.
        dataflow_container: Docker registry location of container image to be used by the Dataflow worker.
        dryrun: If True then pipeline deployment will be skipped.
    """
    if not dryrun:
        assert "GOOGLE_APPLICATION_CREDENTIALS" in os.environ, \
            "Please set the GOOGLE_APPLICATION_CREDENTIALS environment variable"
        
    deployment_config: dict = YAMLUtility.read_yaml_from_file(file_path=deployment_config_path)

    bigquery_l1_schema_bucket: str = deployment_config["bigquery_l1_schema_bucket"]
    bigquery_l1_schema_folder: str = deployment_config["bigquery_l1_schema_folder"]

    ConfigDeployment.check_streaming_deployment_config(deployment_config=deployment_config)
    
    pipeline_options: PipelineOptions = PipelineUtility.construct_pipeline_options(
        deployment_config=deployment_config,
        dataflow_container=dataflow_container
    )

    gcs = GCSUtility()
    l1_config_files: list = gcs.list_files(
        bucket_name=bigquery_l1_schema_bucket,
        folder_path=bigquery_l1_schema_folder
    )

    raw_configs: ConfigRaw = ConfigRaw()
    l1_configs: ConfigL1 = ConfigL1()
    
    for l1_config_file in l1_config_files:
        if re.search(".(yaml|yml)", l1_config_file):
            l1_config_content: str = gcs.load_as_string(
                bucket_name=bigquery_l1_schema_bucket,
                blob_path=l1_config_file
            )
            l1_config_yaml: dict = YAMLUtility.read_yaml_from_string(content=l1_config_content)
            l1_configs.register_config(l1_config_yaml=l1_config_yaml)
    
    pipeline: beam.Pipeline = PipelineUtility.construct_streaming_pipeline(
        pipeline_options=pipeline_options,
        deployment_config=deployment_config,
        raw_configs=raw_configs,
        l1_configs=l1_configs
    )

    if not dryrun:
        pipeline.run()
    else:
        print("run() is not called since dryrun is true.")
