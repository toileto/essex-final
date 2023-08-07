import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from typing import Callable

from laminar.utils.config import ConfigRaw, ConfigL1
from laminar.transformers.src_to_raw import SrcToRawTransformer
from laminar.transformers.raw_to_l1 import RawToL1Transformer, RawToL1WriteErrorTransformer


class PipelineUtility:
    @staticmethod
    def construct_pipeline_options(deployment_config: dict, dataflow_container: str) -> PipelineOptions:
        """
        Construct PipelineOptions object.

        Args:
            deployment_config: Deployment configuration content.
            dataflow_container: Docker registry location of container image to be used by the Dataflow worker.

        Returns:
            PipelineOptions object containing Dataflow pipeline configurations.
        """
        dataflow_labels: list = [
            f"{key}={value}"
            for key, value in deployment_config["dataflow_labels"].items()
        ]
        return PipelineOptions.from_dictionary({
            "runner": deployment_config["beam_runner"],
            "streaming": deployment_config["beam_streaming"],
            "project": deployment_config["dataflow_project_id"],
            "region": deployment_config["dataflow_region"],
            # "worker_zone": deployment_config["dataflow_zone"],
            "job_name": deployment_config["dataflow_job_name"],
            "temp_location": deployment_config["dataflow_temp_location"],
            "staging_location": deployment_config["dataflow_staging_location"],
            "subnetwork": deployment_config["dataflow_subnetwork"],
            "machine_type": deployment_config["dataflow_machine_type"],
            "max_num_workers": deployment_config["dataflow_max_num_workers"],
            "autoscaling_algorithm": deployment_config["dataflow_autoscaling_algorithm"],
            "no_use_public_ips": deployment_config["dataflow_no_use_public_ips"],
            "save_main_session": deployment_config["dataflow_save_main_session"],
            "enable_streaming_engine": deployment_config["dataflow_enable_streaming_engine"],
            "experiments": deployment_config["dataflow_experiments"],
            "labels": dataflow_labels,
            "sdk_container_image": dataflow_container,
            "service_account_email": deployment_config["dataflow_service_account"]
        })
    
    @staticmethod
    def construct_streaming_pipeline(
        pipeline_options: PipelineOptions,
        deployment_config: dict,
        raw_configs: ConfigRaw,
        l1_configs: ConfigL1
    ) -> beam.Pipeline:
        """
        Construct streaming Dataflow pipeline.

        Args:
            pipeline_options: PipelineOptions that contains Dataflow pipeline configurations.
            deployment_config: Deployment configuration content.
            raw_configs: Raw configurations.
            l1_configs: L1 configurations.
        
        Returns:
            Streaming Dataflow pipeline.
        """
        pipeline: beam.Pipeline = beam.Pipeline(options=pipeline_options)
        pubsub_subscription: str = deployment_config["pubsub_subscription"]
        raw_project_id: str = deployment_config['bigquery_raw_project_id']
        raw_dlq_dataset: str = deployment_config['bigquery_raw_dead_letter_dataset']
        raw_dlq_table: str = deployment_config['bigquery_raw_dead_letter_failures_table']
        raw_dataset: str = deployment_config["bigquery_raw_dataset"]
        l1_project_id: str = deployment_config["bigquery_l1_project_id"]
        l1_dlq_dataset: str = deployment_config['bigquery_l1_dead_letter_dataset']
        l1_dlq_failures_table: str = deployment_config['bigquery_l1_dead_letter_failures_table']
        l1_dlq_unconfigured_table: str = deployment_config['bigquery_l1_dead_letter_unconfigured_table']

        kms_project_id: str = deployment_config["kms_project_id"]
        kms_region: str = deployment_config["kms_region"]
        # kms_key_ring: str = deployment_config["kms_key_ring"]
        
        elements_source = (
            pipeline
            | "ReadFromSrc" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription= pubsub_subscription,
                timestamp_attribute=None
            )
        )
        elements_raw = (
            elements_source
            | "TransformSrcToRaw" >> beam.ParDo(SrcToRawTransformer()).with_outputs(
                SrcToRawTransformer.SUCCESS,
                SrcToRawTransformer.FAILURES
            )
        )
        _ = (
            elements_raw[SrcToRawTransformer.FAILURES]
            | "WriteToSrcFailures" >> beam.io.WriteToBigQuery(
                table=f"{raw_project_id}:{raw_dlq_dataset}.{raw_dlq_table}",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
        _ = (
            elements_raw[SrcToRawTransformer.SUCCESS]
            | "WriteToRaw" >> beam.io.WriteToBigQuery(
                table=PipelineUtility.route_raw_table(
                    bigquery_project_id=raw_project_id,
                    bigquery_dataset=raw_dataset,
                ),
                schema=raw_configs.get_table_schema(),
                additional_bq_parameters=raw_configs.get_table_details(),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
        elements_l1 = (
            elements_raw[SrcToRawTransformer.SUCCESS]
            | "TransformRawToL1" >> beam.ParDo(
                RawToL1Transformer(
                    l1_configs=l1_configs, 
                    bigquery_project_id=l1_project_id,
                    kms_project_id=kms_project_id,
                    kms_region=kms_region
                    # kms_key_ring=kms_key_ring
                )
            ).with_outputs(
                RawToL1Transformer.ROUTE_CONFIGURED,
                RawToL1Transformer.ROUTE_UNCONFIGURED,
                RawToL1Transformer.FAILURES
            )
        )
        write_elements_l1 = (
            elements_l1[RawToL1Transformer.ROUTE_CONFIGURED]
            | "WriteToL1" >> beam.io.WriteToBigQuery(
                table=PipelineUtility.route_l1_table(),
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                insert_retry_strategy = beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_NEVER
            )
        )
        _ = (
            write_elements_l1.failed_rows_with_errors
            | "GetWriteErrorElements" >> beam.ParDo(RawToL1WriteErrorTransformer())
            | "WriteToL1FailuresW" >> beam.io.WriteToBigQuery(
                table=f"{l1_project_id}:{l1_dlq_dataset}.{l1_dlq_failures_table}",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
        _ = (
            elements_l1[RawToL1Transformer.FAILURES]
            | "WriteToL1FailuresL" >> beam.io.WriteToBigQuery(
                table=f"{l1_project_id}:{l1_dlq_dataset}.{l1_dlq_failures_table}",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
        _ = (
            elements_l1[RawToL1Transformer.ROUTE_UNCONFIGURED]
            | "WriteToL1Unconfigured" >> beam.io.WriteToBigQuery(
                table=f"{l1_project_id}:{l1_dlq_dataset}.{l1_dlq_unconfigured_table}",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
        return pipeline

    @staticmethod
    def construct_batch_pipeline(
        pipeline_options: PipelineOptions,
        deployment_config: dict,
        l1_configs: ConfigL1,
        batch_query: str,
        destination_table_id: str
    ) -> beam.Pipeline:
        """
        Construct Batch Dataflow pipeline.

        Args:
            pipeline_options: PipelineOptions object that contains Dataflow pipeline configurations.
            deployment_config: Deployment configuration content.
            l1_configs: L1 configurations.
            batch_query: Query to get data from source.
            destination_table_id: Destination L1 table.

        Returns:
            Batch Dataflow pipeline.
        """

        kms_project_id: str = deployment_config["kms_project_id"]
        kms_region: str = deployment_config["kms_region"]
        # kms_key_ring: str = deployment_config["kms_key_ring"]

        pipeline: beam.Pipeline = beam.Pipeline(options=pipeline_options)
        elements_raw = (
            pipeline
            | "ReadFromRaw" >> beam.io.ReadFromBigQuery(
                query=batch_query,
                use_standard_sql=True,
                gcs_location=deployment_config["dataflow_temp_location"]
            )
        )
        elements_l1 = (
            elements_raw
            | "TransformRawToL1" >> beam.ParDo(
                RawToL1Transformer(
                    l1_configs=l1_configs,
                    bigquery_project_id=deployment_config["bigquery_l1_project_id"],
                    kms_project_id=kms_project_id,
                    kms_region=kms_region
                    # kms_key_ring=kms_key_ring
                )
            ).with_outputs(
                RawToL1Transformer.ROUTE_CONFIGURED,
            )
        )
        _ = (
            elements_l1[RawToL1Transformer.ROUTE_CONFIGURED]
            | "WriteToL1" >> beam.io.WriteToBigQuery(
                table=destination_table_id,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        return pipeline

    @staticmethod
    def route_raw_table(bigquery_project_id: str, bigquery_dataset: str) -> Callable:
        """
        Route rows to their destination raw tables.

        Args:
            bigquery_project_id: Project of the destination raw tables.
            bigquery_dataset: Dataset of the destination raw tables.
        
        Returns:
            Callable containing the destination raw tables having the following format "<project>:<dataset>.<table>".
        """
        def curried_route_raw_table(row: dict) -> str:
            return f"{bigquery_project_id}:{bigquery_dataset}.{row['db']}_{row['table']}"
        return curried_route_raw_table
    
    @staticmethod
    def route_l1_table() -> Callable:
        """
        Route rows to their destination L1 tables.

        Returns:
            Callable containing the destination L1 tables having the following format "<project>:<dataset>.<table>".
        """
        def curried_route_l1_table(row: dict) -> str:
            row_metadata = json.loads(row["_metadata"])
            return f"{row_metadata['project']}:{row_metadata['dataset']}.{row_metadata['table']}"
        return curried_route_l1_table
