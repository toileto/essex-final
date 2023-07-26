from laminar.utils.config import ConfigDeployment, ConfigRaw, ConfigL1
from laminar.utils.yaml import YAMLUtility
import os
import pytest


@pytest.fixture
def dummy_schema_config() -> dict:
    file_path: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "resources",
        'dummy_schema.yaml',
    )
    return YAMLUtility.read_yaml_from_file(file_path=file_path)

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

def test_ConfigDeployment_check_streaming_deployment_config(dummy_deployment_streaming_config: dict):
    ConfigDeployment.check_streaming_deployment_config(dummy_deployment_streaming_config)

def test_ConfigDeployment_check_streaming_deployment_config_exception():
    with pytest.raises(Exception):
        ConfigDeployment.check_streaming_deployment_config({})

def test_ConfigDeployment_check_batch_deployment_config(dummy_deployment_batch_config: dict):
    ConfigDeployment.check_batch_deployment_config(dummy_deployment_batch_config)

def test_ConfigDeployment_check_batch_deployment_config_exception():
    with pytest.raises(Exception):
        ConfigDeployment.check_batch_deployment_config({})

def test_ConfigRaw_get_table_schema():
    actual: dict = ConfigRaw().get_table_schema()
    expected: dict = {
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
    assert actual == expected

def test_ConfigRaw_get_table_details():
    actual: dict = ConfigRaw().get_table_details()
    expected: dict = {
        "timePartitioning": {
            "type": "DAY",
            "field": "_date_partition"
        },
        "requirePartitionFilter": True,
    }
    assert actual == expected

def test_ConfigL1_construct_table_details(dummy_schema_config: dict):
    table_details: dict = ConfigL1.construct_table_details(dummy_schema_config)
    actual: list = list(table_details.keys())
    expected: list = ["source", "destination", "partition_type", "partition_by", "labels", "custom_functions"]
    assert actual.sort() == expected.sort()

def test_ConfigL1_construct_table_schema(dummy_schema_config: dict):
    table_schema: dict = ConfigL1.construct_table_schema(dummy_schema_config["fields"])
    assert type(table_schema) == dict

# def test_ConfigL1_construct_custom_functions(dummy_schema_config: dict):
#     actual: dict = ConfigL1.construct_custom_functions(dummy_schema_config)
#     expected: dict = {"raw_to_l1_hash": ["columnString"]}
#     assert actual == expected

def test_ConfigL1_construct_custom_functions_empty():
    actual: dict = ConfigL1.construct_custom_functions({})
    expected: dict = {}
    assert actual == expected

def test_ConfigL1_is_source_exist(dummy_schema_config: dict):
    l1_configs: ConfigL1 = ConfigL1()
    l1_configs.register_config(dummy_schema_config)
    source_id: str = f'{dummy_schema_config["source"]["database"]}.{dummy_schema_config["source"]["table"]}'
    actual: bool = l1_configs.is_source_exist(source_id)
    expected: bool = True
    assert actual == expected

def test_ConfigL1_get_table_config(dummy_schema_config: dict):
    l1_configs: ConfigL1 = ConfigL1()
    l1_configs.register_config(dummy_schema_config)
    source_id: str = f'{dummy_schema_config["source"]["database"]}.{dummy_schema_config["source"]["table"]}'
    actual: list = list(l1_configs.get_table_config(source_id).keys())
    expected: list = ["table_schema", "table_details", "custom_functions"]
    assert actual == expected
