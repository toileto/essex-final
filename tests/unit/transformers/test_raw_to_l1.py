from datetime import datetime, timezone
import json
from laminar.transformers.raw_to_l1 import RawToL1Transformer, RawToL1WriteErrorTransformer
from laminar.utils.config import ConfigL1
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
def dummy_element_payload() -> dict:
    return {
        "columnString": "123",
        "columnInt": 123,
        "columnFloat": 123.0,
        "columnTimestamp": "2023-01-01T07:00:00.000Z",
        "columnDate": "2023-01-01",
        "columnBool": True,
        "columnArrayString": ["123", "456"],
        "columnArrayInteger": None,
        "columnRecord": '{"columnString": "123","columnInt": 123}',
        "columnRecordRepeated": [{
            "columnString": "",
            "columnInt": 123
        }],
    }

@pytest.fixture
def dummy_element(dummy_element_payload: dict) -> dict:
    return {
        "id": "123",
        "table": "dummy_table",
        "db": "dummy_db",
        "op": "c",
        "data": json.dumps(dummy_element_payload),
        "ts": "2023-01-01T07:00:00.000Z",
        "ts_ms": 1672542000000,
        "metadata": json.dumps({
            "version": "1.0"
        }),
        "_ingest_ts_ms": 1672542000000,
        "_date_partition": "2023-01-01",
    }

def test_RawToL1Transformer_process_configured(dummy_element: dict, dummy_schema_config: dict):
    l1_configs: ConfigL1 = ConfigL1()
    l1_configs.register_config(dummy_schema_config)
    actual = list(RawToL1Transformer(
        l1_configs=l1_configs,
        bigquery_project_id="dummy-project",
        kms_project_id="dummy-project",
        kms_region="dummy-region"
    ).process(
        element_raw=dummy_element
    ))[0]
    expected_tag: str = "RC"
    assert actual.tag == expected_tag

def test_RawToL1Transformer_process_unconfigured(dummy_element: dict, dummy_schema_config: dict):
    l1_configs: ConfigL1 = ConfigL1()
    l1_configs.register_config(dummy_schema_config)
    dummy_element["table"] = "dummy_table_unconfigured"
    actual = list(RawToL1Transformer(
        l1_configs=l1_configs,
        bigquery_project_id="dummy-project",
        kms_project_id="dummy-project",
        kms_region="dummy-region"
    ).process(
        element_raw=dummy_element
    ))[0]
    expected_tag: str = "RU"
    assert actual.tag == expected_tag

def test_RawToL1Transformer_process_failures(dummy_element: dict, dummy_schema_config: dict):
    l1_configs: ConfigL1 = ConfigL1()
    l1_configs.register_config(dummy_schema_config)
    dummy_element["data"] = dummy_element["data"].replace(
        '"columnInt": 123', '"columnInt": "wrong_type"'
    )
    actual = list(RawToL1Transformer(
        l1_configs=l1_configs,
        bigquery_project_id="dummy-project",
        kms_project_id="dummy-project",
        kms_region="dummy-region"
    ).process(
        element_raw=dummy_element
    ))[0]
    expected_tag: str = "F"
    assert actual.tag == expected_tag

def test_RawToL1Transformer_get_unconfigured_element(dummy_element: dict):
    actual: dict = RawToL1Transformer.get_unconfigured_element(element_raw=dummy_element)
    expected: dict = {
        "id": "123",
        "table": "dummy_table",
        "database": "dummy_db",
        "ts": datetime(2023,1,1,7,0,0,tzinfo=timezone.utc),
        "ingest_ts": actual["ingest_ts"]
    }
    assert actual == expected

def test_RawToL1Transformer_get_failed_element(dummy_element: dict):
    actual: dict = RawToL1Transformer.get_failed_element(
        element_raw=dummy_element, source_id="dummy_db.dummy_table", 
        destination_id="dummy_dataset.dummy_table", exception="dummy error"
    )
    expected: dict = {
        "id": "123",
        "source_database": "dummy_db",
        "source_table": "dummy_table",
        "destination_dataset":"dummy_dataset",
        "destination_table": "dummy_table",
        "ts": datetime(2023,1,1,7,0,0,tzinfo=timezone.utc),
        "exception_type": "ParDo",
        "exception": actual["exception"],
        "traceback": actual["traceback"],
        "ingest_ts": actual["ingest_ts"],
    }
    assert actual == expected

def test_RawToL1Transformer_parse_raw_to_l1(dummy_element: dict, dummy_schema_config: dict):
    table_config: dict = {
        "table_details": ConfigL1.construct_table_details(l1_config_yaml=dummy_schema_config)
    }
    transformed_element: dict = RawToL1Transformer.parse_raw_to_l1(
        element=dummy_element, table_config=table_config, project_id="dummy-project" 
    )
    assert type(transformed_element) == dict

def test_RawToL1Transformer_cast_raw_to_l1(dummy_element_payload: dict, dummy_schema_config: dict):
    table_config: dict = {
        "table_schema": ConfigL1.construct_table_schema(l1_config_fields=dummy_schema_config["fields"]),
        "table_details": {}
    }
    dummy_element_payload["_metadata"] =  "dummy_metadata"
    transformed_element: dict = RawToL1Transformer(
        l1_configs=table_config, bigquery_project_id="dummy-project",
        kms_project_id="dummy-project", kms_region="dummy-region"
    ).cast_raw_to_l1(
        element=dummy_element_payload, table_config=table_config,
        raw_id="123", raw_ts="2023-01-01T07:00:00.000Z"
    )
    assert type(transformed_element) == dict

def test_RawToL1Transformer_set_default_none_repeated():
    assert RawToL1Transformer.set_default_none(data_mode="REPEATED", data_type="RECORD") == []

def test_RawToL1Transformer_set_default_none_record():
    assert RawToL1Transformer.set_default_none(data_mode="NULLABLE", data_type="RECORD") == {}

def test_RawToL1Transformer_set_default_none():
    assert RawToL1Transformer.set_default_none(data_mode="NULLABLE", data_type="STRING") == None

def test_RawToL1Transformer_evaluate_string_repeated():
    assert RawToL1Transformer.evaluate_string(
        data="[1,2,3]",
        data_type="INTEGER",
        data_mode="REPEATED"
    ) == [1,2,3]

def test_RawToL1Transformer_evaluate_string_record():
    assert RawToL1Transformer.evaluate_string(
        data='{"a": 1, "b": 2}',
        data_type="RECORD",
        data_mode="NULLABLE"
    ) == {"a": 1, "b": 2}

def test_RawToL1Transformer_evaluate_string():
    assert RawToL1Transformer.evaluate_string(
        data="dummy",
        data_type="STRING",
        data_mode="NULLABLE"
    ) == "dummy"

def test_RawToL1Transformer_cast_scalar_integer():
    data = 1
    assert RawToL1Transformer.cast_scalar(
        data=data, data_type="INTEGER", dek="dummy-key", is_sensitive=False
    ) == data

def test_RawToL1Transformer_cast_scalar_float():
    data = 1.0
    assert RawToL1Transformer.cast_scalar(
        data=data, data_type="FLOAT", dek="dummy-key", is_sensitive=False
    ) == data

def test_RawToL1Transformer_cast_scalar_timestamp():
    data = "2023-01-01T07:00:00.000Z"
    assert type(RawToL1Transformer.cast_scalar(
        data=data, data_type="TIMESTAMP", dek="dummy-key", is_sensitive=False
    )) == datetime

def test_RawToL1Transformer_cast_scalar_date():
    data = "2023-01-01T07:00:00.000Z"
    assert RawToL1Transformer.cast_scalar(
        data=data, data_type="DATE", dek="dummy-key", is_sensitive=False
    ) == data[:10]

def test_RawToL1Transformer_cast_scalar_bool():
    data = True
    assert RawToL1Transformer.cast_scalar(
        data=data, data_type="BOOL", dek="dummy-key", is_sensitive=False
    ) == data

def test_RawToL1Transformer_cast_scalar():
    data = "dummy"
    assert RawToL1Transformer.cast_scalar(
        data=data, data_type="STRING", dek="dummy-key", is_sensitive=False
    ) == data

def test_RawToL1Transformer_cast_array():
    data = ["1", "2", "3"]
    assert RawToL1Transformer.cast_array(
        data_array=data, data_type="INTEGER", dek="dummy-key", is_sensitive=False
    ) == [1, 2, 3]

def test_RawToL1WriteErrorDoFn_process():
    transformed_element: dict = list(RawToL1WriteErrorTransformer().process(
        element_write_error= [
            "dummy-info",
            {
                "_raw_id": "123",
                "_raw_ts": "dummy-timestamp",
                "_metadata": '{"source_database":"dummy_db", "source_table": "dummy_table", "dataset": "dummy_dataset", "table": "dummy_table"}'
            },
            [{"message": "dummy error message"}]
        ]
    ))[0]
    assert type(transformed_element) == dict
