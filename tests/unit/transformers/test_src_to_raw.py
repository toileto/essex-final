from apache_beam.utils.timestamp import Timestamp
from datetime import datetime
import json
from laminar.transformers.src_to_raw import SrcToRawTransformer
import pytest


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

def test_SrcToRawTransformer_process_success(dummy_element: dict):
    element: bytes = json.dumps(dummy_element).encode("utf-8")
    dofn_timestamp: Timestamp = Timestamp.from_rfc3339("2023-01-01T07:00:00.000Z")
    actual = list(SrcToRawTransformer().process(element=element, timestamp=dofn_timestamp))[0]
    expected_value: dict = dummy_element.copy()
    expected_value.update({
        "_ingest_ts_ms": 1672556400000,
        "_date_partition": datetime.fromtimestamp(dummy_element["ts_ms"] / 1000).date(),
        "metadata": json.dumps(dummy_element["metadata"]),
    })
    expected_tag: str = "S"
    assert actual.value == expected_value and actual.tag == expected_tag

def test_SrcToRawTransformer_process_failure(dummy_element: dict):
    dummy_element["ts_ms"] = "wrong_type"
    element: bytes = json.dumps(dummy_element).encode("utf-8")
    dofn_timestamp: Timestamp = Timestamp.from_rfc3339("2023-01-01T07:00:00.000Z")
    actual = list(SrcToRawTransformer().process(element=element, timestamp=dofn_timestamp))[0]
    expected_tag: str = "F"
    assert actual.tag == expected_tag

def test_SrcToRawTransformer_add_metadata_src_to_raw(dummy_element: dict):
    element: bytes = json.dumps(dummy_element).encode("utf-8")
    dofn_timestamp: Timestamp = Timestamp.from_rfc3339("2023-01-01T07:00:00.000Z")
    actual = SrcToRawTransformer.add_metadata_src_to_raw(
        element=element, timestamp=dofn_timestamp, window=None
    )
    expected: dict = dummy_element.copy()
    expected.update({
        "_ingest_ts_ms": 1672556400000,
        "_date_partition": datetime.fromtimestamp(dummy_element["ts_ms"] / 1000).date(),
        "metadata": json.dumps(dummy_element["metadata"]),
    })
    assert actual == expected
