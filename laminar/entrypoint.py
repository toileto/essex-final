import click

from laminar.pipelines import batch, streaming


@click.group()
def cli(): pass

@cli.command()
@click.option("--deployment_config_path", type=str, required=True, help="Deployment configuration path.")
@click.option("--dataflow_container", type=str, required=True, help="Docker registry location of container image to be used by the Dataflow worker.")
@click.option("--dryrun", type=bool, required=True, help="If True then pipeline deployment will be skipped.")
def run_streaming_pipeline(
    deployment_config_path: str, 
    dataflow_container: str, 
    dryrun: bool
) -> None:
    """
    Command line interface to deploy streaming Laminar pipeline.

    Args:
        deployment_config_path: Deployment configuration path.
        dataflow_container: Docker registry location of container image to be used by the Dataflow worker.
        dryrun: If True then pipeline deployment will be skipped.
    """
    streaming.main(
        deployment_config_path=deployment_config_path, 
        dataflow_container=dataflow_container, 
        dryrun=dryrun
    )

@cli.command()
@click.option("--deployment_config_path", type=str, required=True, help="Deployment configuration path.")
@click.option("--dataflow_container", type=str, required=True, help="Docker registry location of container image to be used by the Dataflow worker.")
@click.option("--table", type=str, required=True, help="Destination BigQuery table.")
@click.option("--start_date", type=str, required=True, help="Start date to retrieve data from source.")
@click.option("--end_date", type=str, required=True, help="End date to retrieve data from source.")
@click.option("--dryrun", type=bool, required=True, help="If True then pipeline deployment will be skipped.")
def run_batch_pipeline(
    deployment_config_path: str, 
    dataflow_container: str, 
    table: str, 
    start_date: str, 
    end_date: str,
    dryrun: bool
) -> None:
    """
    Command line interface to deploy batch Laminar pipeline.

    Args:
        deployment_config_path: Deployment configuration path.
        dataflow_container: Docker registry location of container image to be used by the Dataflow worker.
        table: Destination BigQuery table.
        start_date: Start date to retrieve data from source.
        end_date: End date to retrieve data from source.
        dryrun: If True then pipeline deployment will be skipped.
    """
    batch.main(
        deployment_config_path=deployment_config_path, 
        dataflow_container=dataflow_container, 
        table=table, 
        start_date=start_date, 
        end_date=end_date, 
        dryrun=dryrun
    )

if __name__ == "__main__": cli()
