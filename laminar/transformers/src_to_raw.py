import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from datetime import datetime
import json
import traceback
from laminar.utils.time import TimeUtility


class SrcToRawTransformer(beam.DoFn):

    """Processing tags."""
    SUCCESS: str = 'S'
    FAILURES: str = 'F'

    def process(
        self, 
        element: bytes, 
        timestamp=beam.DoFn.TimestampParam, 
        window=beam.DoFn.WindowParam
    ):
        """
        Transform Source to Raw element.

        Args:
            element: Source PCollection element.
        
        Returns:
            Tagged value and transformed PCollection element.
        """
        try:
            element_transformed: dict = SrcToRawTransformer.add_metadata_src_to_raw(
                element=element,
                timestamp=timestamp,
                window=window
            )
            yield TaggedOutput(SrcToRawTransformer.SUCCESS, element_transformed)
        
        except Exception as exception:
            yield TaggedOutput(
                SrcToRawTransformer.FAILURES,
                {
                    "message": element.decode("utf-8"),
                    "exception": str(exception),
                    "traceback": traceback.format_exc(),
                    "ingest_ts": TimeUtility.get_current_ts_utc()
                }
            )
    
    @staticmethod
    def add_metadata_src_to_raw(element: bytes, timestamp, window) -> dict:
        """
        Parse and add metadata to source PCollection.

        Args:
            element: Source PCollection element.

        Returns:
            Raw PCollection element with metadata.
        """
        element_decoded: str = element.decode("utf-8")
        element_parsed: dict = json.loads(element_decoded)
        element_parsed["_ingest_ts_ms"] = int(timestamp.micros / 1000)
        element_parsed["_date_partition"] = datetime.fromtimestamp(element_parsed["ts_ms"] / 1000.0).date()
        element_parsed["metadata"] = json.dumps(element_parsed["metadata"])
        return element_parsed
