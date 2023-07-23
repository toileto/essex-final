from laminar.utils.time import TimeUtility
from datetime import datetime, timezone

 
def test_TimeUtility_get_current_ts_utc():
    assert type(TimeUtility.get_current_ts_utc()) == datetime

def test_TimeUtility_cast_as_datetime():
    actual: datetime = TimeUtility.cast_as_datetime(data="2023-01-01T10:00:00.00Z")
    expected: datetime = datetime(2023,1,1,10,0,0,tzinfo=timezone.utc)
    assert actual == expected

def test_TimeUtility_cast_as_date_string():
    actual: str = TimeUtility.cast_as_date_string(data="2023-01-01T10:00:00.00Z")
    expected: str = "2023-01-01"
    assert actual == expected

def test_TimeUtility_convert_unix_to_ts():
    actual: datetime = TimeUtility.convert_unix_to_ts(data="1672542000")
    expected: datetime = datetime(2023,1,1,3,0,0)
    assert actual == expected

def test_TimeUtility_safe_cast_ts_to_datetime_datetime():
    actual: datetime = TimeUtility.safe_cast_ts_to_datetime(data="2023-01-01T10:00:00.00Z")
    expected: datetime = datetime(2023,1,1,10,0,0,tzinfo=timezone.utc)
    assert actual == expected

def test_TimeUtility_safe_cast_ts_to_datetime_unix():
    actual: datetime = TimeUtility.safe_cast_ts_to_datetime(data="1672542000")
    expected: datetime = datetime(2023,1,1,3,0,0)
    assert actual == expected
