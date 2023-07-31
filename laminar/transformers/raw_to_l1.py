import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
import ast
from datetime import datetime
import importlib
import json
import operator
import traceback
from typing import Callable, Union

from base64 import b64encode
from Crypto.Cipher import ChaCha20_Poly1305
from Crypto.Random import get_random_bytes
from google.cloud.kms import CryptoKey, CryptoKeyVersion, KeyManagementServiceClient

from laminar.utils.config import ConfigL1
from laminar.utils.bigquery import BigQueryUtility
from laminar.utils.time import TimeUtility


class RawToL1Transformer(beam.DoFn):
    
    """Processing tags."""
    ROUTE_CONFIGURED: str = 'RC'
    ROUTE_UNCONFIGURED: str = 'RU'
    FAILURES: str = 'F'

    """Path to custom function file."""
    CUSTOM_FUNCTIONS_BASE_PATH: str = "laminar.transformers.custom.raw_to_l1"

    def __init__(
        self, 
        l1_configs: ConfigL1, 
        bigquery_project_id: str,
        kms_project_id: str,
        kms_region: str
    ) -> None:
        """
        Initialize RawToL1Transformer.

        Args:
            l1_configs: L1 configurations.
            bigquery_project_id: Project ID of destination L1 tables.
        """
        self.l1_configs: ConfigL1 = l1_configs
        self.bigquery_project_id: str = bigquery_project_id
        self.kms_project_id: str = kms_project_id
        self.kms_region: str = kms_region

    def process(self, element_raw: dict):
        """
        Transform Raw to L1 element.

        Args:
            element_raw: Raw PCollection element.
        
        Returns:
            Tagged value and transformed PCollection element.
        """
        source_id: str = f"{element_raw['db']}.{element_raw['table']}"
        try:
            is_source_exist: bool = self.l1_configs.is_source_exist(source_id=source_id)
            if not is_source_exist:
                yield TaggedOutput(
                    RawToL1Transformer.ROUTE_UNCONFIGURED, 
                    RawToL1Transformer.get_unconfigured_element(element_raw=element_raw)
                )
            else:
                table_config: dict = self.l1_configs.get_table_config(source_id=source_id)
                raw_id: str = element_raw.get("id")
                raw_ts: str = element_raw.get("ts")
                element_transformed: dict = RawToL1Transformer.parse_raw_to_l1(
                    element=element_raw, 
                    table_config=table_config, 
                    project_id=self.bigquery_project_id
                )

                element_transformed: dict = self.cast_raw_to_l1(
                    element=element_transformed,
                    table_config=table_config,
                    raw_id=raw_id,
                    raw_ts=raw_ts
                )
                yield TaggedOutput(
                    RawToL1Transformer.ROUTE_CONFIGURED, 
                    element_transformed
                )
        
        except Exception as exception:
            table_details: dict = self.l1_configs.get_table_config(source_id=source_id)["table_details"]
            destination_id: str = f'{table_details["destination"]["dataset"]}.{table_details["destination"]["table"]}'
            yield TaggedOutput(
                RawToL1Transformer.FAILURES,
                RawToL1Transformer.get_failed_element(
                    element_raw=element_raw,
                    source_id=source_id,
                    destination_id=destination_id,
                    exception=exception
                )
            )

    @staticmethod
    def get_unconfigured_element(element_raw: dict) -> dict:
        """
        Get PCollection for elements with no destination table.

        Args:
            element_raw: Raw PCollection element.

        Returns:
            PCollection element for unconfigured table.
        """
        return {
            "id": element_raw.get("id"),
            "table": element_raw.get("table"),
            "database": element_raw.get("db"),
            "ts": TimeUtility.safe_cast_ts_to_datetime(data=element_raw.get("ts")),
            "ingest_ts": TimeUtility.get_current_ts_utc()
        }
    
    @staticmethod
    def get_failed_element(element_raw: dict, source_id: str, destination_id: str, exception: str) -> dict:
        """
        Get PCollection for elements with logic error when transforming raw to L1 element.
        
        Args:
            element_raw: Raw PCollection element.
            source_id: Source table ID, i.e., "<source_database>.<source_table>".
            destination_id: Destination table ID, i.e., "<destination_dataset>.<destination_table>".
            exception: Exception error message.

        Returns:
            PCollection element for logic failures.
        """
        source_database, source_table = source_id.split(".")
        destination_dataset, destination_table = destination_id.split(".")
        return {
            "id": element_raw.get("id"),
            "source_database": source_database,
            "source_table": source_table,
            "destination_dataset":destination_dataset,
            "destination_table": destination_table,
            "ts": TimeUtility.safe_cast_ts_to_datetime(data=element_raw.get("ts")),
            "exception_type": "ParDo",
            "exception": str(exception),
            "traceback": traceback.format_exc(),
            "ingest_ts": TimeUtility.get_current_ts_utc(),
        }

    @staticmethod
    def parse_raw_to_l1(element: dict, table_config: dict, project_id: str) -> dict:
        """
        Parse "data" column of raw element (string) into L1 elements (dict). 
        
        Args:
            element: Raw PCollection element.
            table_config: L1 table configuration.
            project_id: Project ID of destination L1 tables.
        
        Returns:
            Parsed L1 element.
        """
        table_details: dict = table_config["table_details"]
        element_parsed: dict = json.loads(element["data"])
        element_parsed["_metadata"] = json.dumps(
            {
                "source_database": table_details["source"]["database"],
                "source_table": table_details["source"]["table"],
                "project": project_id,
                "dataset": table_details["destination"]["dataset"],
                "table": table_details["destination"]["table"]
            }
        )
        return element_parsed
    
    def cast_raw_to_l1(self, element: dict, table_config: dict, raw_id: str, raw_ts: str) -> dict:
        """
        Cast data types of parsed L1 element and map the column keys from source to destination.

        Args: 
            element: Parsed L1 PCollection element.
            table_config: L1 table configuration.
            raw_id: "id" column of Raw element.
            raw_ts: "ts" column of Raw element.

        Returns:
            Casted L1 element.
        """
        table_schema: dict = table_config["table_schema"]

        dek = get_random_bytes(32)

        # Encrypt data with DEK
        element_casted: dict = RawToL1Transformer.cast_element(element=element, table_schema=table_schema, dek=dek)

        kms_key_ring = table_config["table_details"]["labels"].get("product")
        kms_keys = table_config["table_details"].get("kms_key")

        wrapped_deks = []

        if kms_key_ring is not None and kms_keys is not None:

            kms_client = KeyManagementServiceClient()
            
            # Create key ring
            try:
                kms_client.create_key_ring(
                    request={
                        "parent": f"projects/{self.kms_project_id}/locations/{self.kms_region}",
                        "key_ring_id": kms_key_ring,
                        "key_ring": {},
                    }
                )
            except Exception as e:
                # The key ring already exists
                print(e)

            for kms_key in kms_keys:
                # Create KEK
                try:
                    kms_client.create_crypto_key(request={
                        'parent': kms_client.key_ring_path(self.kms_project_id, self.kms_region, kms_key_ring), 'crypto_key_id': element_casted[kms_key], 
                            'crypto_key': {
                                'purpose': CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT,
                                'version_template': {'algorithm': CryptoKeyVersion.CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION}
                            }
                        }
                    )
                except Exception as e:
                    # The KEK already exists
                    print(e)

                # Encrypt DEK with KEK
                wrapped_dek = kms_client.encrypt(
                    request={'name': kms_client.crypto_key_path(
                            self.kms_project_id, self.kms_region, kms_key_ring, element_casted[kms_key]
                        ), 'plaintext': dek
                    }
                ).ciphertext

                wrapped_deks.append(wrapped_dek)

        casted_raw_ts: datetime = TimeUtility.safe_cast_ts_to_datetime(data=raw_ts)
        element_casted.update({
            "_raw_ts": casted_raw_ts,
            "_raw_id": raw_id,
            "published_timestamp": casted_raw_ts,
            "_metadata": element["_metadata"]
        })
        if kms_key_ring is not None and kms_key is not None:
            element_casted.update({
                "_wrapped_dek": [b64encode(wrapped_dek).decode() for wrapped_dek in wrapped_deks]
            })            
        return element_casted

    @staticmethod
    def cast_element(element: dict, table_schema: dict, dek: bytes) -> dict:
        """
        Cast data type for each column in an element according to the table schema.

        Args:
            element: Parsed data element.
            table_schema: L1 table schema.

        Returns:
            Casted element.
        """
        element_casted: dict = {}
        for source_column, column_schema in table_schema.items():
            destination_column: str = column_schema["name"]
            data_type: str = column_schema["type"]
            data_mode: str = column_schema["mode"]
            child_table_schema: dict = column_schema.get("fields")

            if (element is None) or (element is not None and source_column not in element):
                element_casted[destination_column] = RawToL1Transformer.set_default_none(
                    data_mode=data_mode,
                    data_type=data_type
                )
            else:
                data: Union[int, float, bool, str, list, dict] = element[source_column]
                data = RawToL1Transformer.evaluate_string(
                    data=data,
                    data_type=data_type,
                    data_mode=data_mode
                )
                if data is None:
                    element_casted[destination_column] = RawToL1Transformer.set_default_none(
                        data_mode=data_mode,
                        data_type=data_type
                    )
                elif data in BigQueryUtility.EMPTY_STRING:
                    element_casted[destination_column] = None
                elif data_type != "RECORD" and data_mode == "REPEATED":
                    element_casted[destination_column] = RawToL1Transformer.cast_array(
                        data_array=data,
                        data_type=data_type,
                        dek=dek,
                        is_sensitive=column_schema["sensitive"]
                    )
                elif data_type == "RECORD" and data_mode == "REPEATED":
                    element_casted[destination_column] = [
                        RawToL1Transformer.cast_element(
                            element=child_element,
                            table_schema=child_table_schema,
                            dek=dek
                        ) for child_element in data
                    ]
                elif data_type == "RECORD":
                    element_casted[destination_column] = RawToL1Transformer.cast_element(
                        element=data,
                        table_schema=child_table_schema,
                        dek=dek
                    )
                else:
                    element_casted[destination_column] = RawToL1Transformer.cast_scalar(
                        data=data,
                        data_type=data_type,
                        dek=dek,
                        is_sensitive=column_schema["sensitive"]
                    )
        return element_casted

    @staticmethod
    def set_default_none(data_mode: str, data_type: str) -> Union[list, dict, None]:
        """
        Set default empty/null values based on the data type and mode.

        Args:
            data_mode: BigQuery data mode.
            data_type: BigQuery data type.

        Returns:
            Either empty list, empty dictionary, or None.
        """
        if data_mode == "REPEATED":
            return []
        elif data_type == "RECORD":
            return {}
        else:
            return None
        
    @staticmethod
    def evaluate_string(
        data: Union[int, float, bool, str, list, dict],
        data_type: str,
        data_mode: str
    ) -> Union[int, float, bool, str, list, dict]:
        """
        Evaluate list or dictionary enclosed in quotation marks, e.g. evaluate list [1,2,3] from string "[1,2,3]".

        Args:
            data: Data value for a single column.
            data_type: BigQuery data type.
            data_mode: BigQuery data mode.
        
        Returns:
            Either list, dictionary or the data itself.
        """
        if data_mode == "REPEATED" \
            and type(data) == str \
                and data not in BigQueryUtility.EMPTY_STRING:
            return ast.literal_eval(data)
        elif data_type == "RECORD" \
            and type(data) == str \
                and data not in BigQueryUtility.EMPTY_STRING:
            return ast.literal_eval(data)
        else:
            return data
        
    @staticmethod
    def cast_scalar(
        data: Union[int, float, bool, str], 
        data_type: str,
        dek: bytes,
        is_sensitive: bool
    ) -> Union[int, float, datetime, bool, str]:
        """
        Cast a scalar data in an element according to the table schema.

        Args:
            data: A single column data.
            data_type: Data type.

        Returns:
            Casted data.
        """
        if is_sensitive:
            cipher = ChaCha20_Poly1305.new(key=dek)
            nonce = cipher.nonce
            ciphertext, tag = cipher.encrypt_and_digest(str(data).encode())
            data_casted = b64encode(tag+nonce+ciphertext).decode()
        else:
            if data_type in BigQueryUtility.INTEGER_TYPES:
                data_casted: int = int(data)
            elif data_type in BigQueryUtility.FLOAT_TYPES:
                data_casted: float = float(data)
            elif data_type in BigQueryUtility.TIMESTAMP_TYPES:
                data_casted: datetime = TimeUtility.cast_as_datetime(data=data)
            elif data_type in BigQueryUtility.DATE:
                data_casted: str = TimeUtility.cast_as_date_string(data=data)
            elif data_type in BigQueryUtility.BOOLEAN_TYPES:
                data_casted: bool = bool(data)
            else:
                data_casted: str = str(data)
        return data_casted
    
    @staticmethod
    def cast_array(data_array: list, data_type: str, dek: bytes, is_sensitive: bool) -> list:
        """
        Cast an array data in an element according to the table schema.

        Args:
            data_array: Data array for a single column.
            data_type: Data type.

        Returns:
            Casted non-empty data list.
        """
        return [
            RawToL1Transformer.cast_scalar(data=data, data_type=data_type, dek=dek, is_sensitive=is_sensitive) 
            for data in data_array if data is not None
        ]

    @staticmethod
    def custom_process_raw_to_l1(element: dict, custom_functions: dict, extra_params: dict) -> dict:
        """
        Additional custom processing.

        Args:
            element: Parsed L1 element.
            custom_functions: Custom functions-to-column names mapping.

        Returns:
            Custom transformed L1 element.
        """
        for custom_function_name, columns in custom_functions.items():
            custom_function: Callable = operator.attrgetter(custom_function_name)\
                (importlib.import_module(RawToL1Transformer.CUSTOM_FUNCTIONS_BASE_PATH))
            # transformed_element: dict = {column: custom_function(data=element[column], extra_params=extra_params) for column in columns}
            transformed_element: dict = custom_function(data=element, columns=columns, extra_params=extra_params)
            element.update(transformed_element)
        return element


class RawToL1WriteErrorTransformer(beam.DoFn):
    def process(self, element_write_error: list):
        """
        Get PCollection for elements with write error when trying to insert into destination BigQuery tables.

        Args:
            element_write_error: Element generated by WriteToBigQuery when there is an error inserting data into BigQuery table.

        Returns:
            PCollection element for write failures.
        """
        data: dict = element_write_error[1]
        error_message: str = element_write_error[2][0]["message"]
        metadata: dict = json.loads(data["_metadata"])
        yield {        
            "id": data.get("_raw_id"),
            "source_database": metadata["source_database"],
            "source_table": metadata["source_table"],
            "destination_dataset":metadata["dataset"],
            "destination_table": metadata["table"],
            "ts": data.get("_raw_ts"),
            "exception_type": "WriteToBigQuery",
            "exception": error_message,
            "traceback": traceback.format_exc(),
            "ingest_ts": TimeUtility.get_current_ts_utc(),    
        }
