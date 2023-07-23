import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from laminar.utils.pipeline import PipelineUtility
from laminar.utils.config import ConfigRaw, ConfigL1
from laminar.utils.yaml import YAMLUtility
import os
import pytest

@pytest.fixture
def dummy_deployment_streaming_config() -> dict:
    file_path: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "resources",
        'dummy.deployment.streaming.yaml',
    )
    return YAMLUtility.read_yaml_from_file(file_path=file_path)

@pytest.fixture
def dummy_deployment_batch_config() -> dict:
    file_path: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "resources",
        "dummy.deployment.batch.yaml",
    )
    return YAMLUtility.read_yaml_from_file(file_path=file_path)

@pytest.fixture
def dummy_schema_config() -> dict:
    file_path: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "resources",
        'dummy_schema.yaml',
    )
    return YAMLUtility.read_yaml_from_file(file_path=file_path)

def test_PipelineUtility_construct_pipeline_options(dummy_deployment_streaming_config: dict):
    pipeline_options: PipelineOptions = PipelineUtility.construct_pipeline_options(
        deployment_config=dummy_deployment_streaming_config,
        dataflow_container="dummy-image",
    )
    assert isinstance(pipeline_options, PipelineOptions)

def test_PipelineUtility_construct_streaming_pipeline(dummy_deployment_streaming_config: dict, dummy_schema_config: dict):
    pipeline_options: PipelineOptions = PipelineUtility.construct_pipeline_options(
        deployment_config=dummy_deployment_streaming_config,
        dataflow_container="dummy-image",
    )
    raw_configs: ConfigRaw = ConfigRaw()
    l1_configs: ConfigL1 = ConfigL1()
    l1_configs.register_config(dummy_schema_config)
    pipeline: beam.Pipeline = PipelineUtility.construct_streaming_pipeline(
        pipeline_options=pipeline_options,
        deployment_config=dummy_deployment_streaming_config,
        raw_configs=raw_configs,
        l1_configs=l1_configs,
    )
    assert isinstance(pipeline, beam.Pipeline)

def test_PipelineUtility_construct_batch_pipeline(dummy_deployment_batch_config: dict, dummy_schema_config: dict):
    pipeline_options: PipelineOptions = PipelineUtility.construct_pipeline_options(
        deployment_config=dummy_deployment_batch_config,
        dataflow_container="dummy-image",
    )
    l1_configs: ConfigL1 = ConfigL1()
    l1_configs.register_config(dummy_schema_config)
    pipeline: beam.Pipeline = PipelineUtility.construct_batch_pipeline(
        pipeline_options=pipeline_options,
        deployment_config=dummy_deployment_batch_config,
        l1_configs=l1_configs,
        batch_query="dummy-query",
        destination_table_id="dummy-project:dummy_dataset.dummy_table"
    )
    assert isinstance(pipeline, beam.Pipeline)

def test_PipelineUtility_route_raw_table(dummy_deployment_streaming_config: dict):
    bigquery_project_id: str = dummy_deployment_streaming_config["bigquery_raw_project_id"]
    bigquery_dataset: str = dummy_deployment_streaming_config["bigquery_raw_dataset"]
    row: dict = {
        "db": "dummy_db",
        "table": "dummy_table",
    }
    actual: str = PipelineUtility.route_raw_table(
        bigquery_project_id=bigquery_project_id,
        bigquery_dataset=bigquery_dataset,
    )(row)
    expected: str = f"{bigquery_project_id}:{bigquery_dataset}.{row['db']}_{row['table']}"
    assert actual == expected

def test_PipelineUtility_route_l1_table(dummy_deployment_streaming_config: dict):
    metadata: dict = {
        "project": dummy_deployment_streaming_config["bigquery_l1_project_id"],
        "dataset": "dummy_dataset",
        "table": "dummy_table"
    }
    row: dict = {
        "_metadata": json.dumps(metadata)
    }
    actual: str = PipelineUtility.route_l1_table()(row)
    expected: str = f'{metadata["project"]}:{metadata["dataset"]}.{metadata["table"]}'
    assert actual == expected
