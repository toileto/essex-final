import click
from google.cloud import secretmanager
import json

@click.group()
def cli(): pass

@cli.command()
@click.option("--gsm_project_id", type=str, required=True, help="Project ID of the Secret Manager where the SA resides.")
@click.option("--gsm_secret_id", type=str, required=True, help="Secret ID of the Secret Manager where the SA resides.")
@click.option("--gsm_version_id", type=str, required=True, help="Version ID of the SA in Secret Manager.")
@click.option("--deployment", type=str, required=True, help="Either 'development', 'staging' or 'production'.")
def download_service_account(
    gsm_project_id: str,
    gsm_secret_id: str,
    gsm_version_id: str,
    deployment: str
) -> None:
    """
    Download service account (SA) for Laminar deployment.

    Args:
        gsm_project_id: Project ID of the Secret Manager where the SA resides.
        gsm_secret_id: Secret ID of the Secret Manager where the SA resides.
        gsm_version_id: Version ID of the SA in Secret Manager.
        deployment: The environment, either "development", "staging" or "production".
    """
    response = secretmanager.SecretManagerServiceClient().access_secret_version(
        request={"name": f"projects/{gsm_project_id}/secrets/{gsm_secret_id}/versions/{gsm_version_id}"}
    )
    service_account_dict: dict = json.loads(response.payload.data.decode("UTF-8"))
    with open(f"service_account.{deployment}.json", "w") as json_file:
        json.dump(service_account_dict, json_file, indent=4)

if __name__ == "__main__": cli()