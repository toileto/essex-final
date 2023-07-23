from datetime import datetime, timezone
from dateutil import parser as dateutil_parser
from typing import Union


class TimeUtility:
    @staticmethod
    def get_current_ts_utc() -> datetime:
        """
        Get current timestamp.

        Returns:
            Current UTC timestamp.
        """
        return datetime.now(tz=timezone.utc)
    
    @staticmethod
    def cast_as_datetime(data: str) -> datetime:
        """
        Cast timestamp string into a datetime.

        Args:
            data: String containing an ISO-8601 datetime format.

        Returns:
            datetime representing the string.
        """
        return dateutil_parser.isoparse(data)
    
    @staticmethod
    def cast_as_date_string(data: str) -> str:
        """
        Cast timestamp string into a date string.

        Args:
            data: String containing an ISO-8601 datetime format.
        
        Returns:
            date string representing the string.
        """
        return dateutil_parser.isoparse(data).strftime("%Y-%m-%d")

    @staticmethod
    def convert_unix_to_ts(data: Union[int, float, str]) -> datetime:
        """
        Convert Unix timestamp to a datetime.

        Args:
            data: Unix timestamp.
        
        Returns:
            datetime representing the Unix timestamp.
        """
        return datetime.fromtimestamp(float(data))

    @staticmethod
    def safe_cast_ts_to_datetime(data: Union[int, str]) -> datetime:
        """
        Conditional cast Unix timestamp or timestamp string into a datetime.

        Args:
            data: Unix timestamp or ISO-8601 datetime format string.
        
        Returns:
            datetime representing the input data.
        """
        if data is not None:
            try: 
                data: datetime = TimeUtility.cast_as_datetime(data=data)
            except:
                data: datetime = TimeUtility.convert_unix_to_ts(data=data)
        return data
