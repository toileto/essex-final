import os
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict


def create_or_update_bigquery_table(table_metadata):
    client = bigquery.Client()

    dataset_name = table_metadata["destination"]["dataset"]
    table_name = table_metadata["destination"]["table"]
    dataset_ref = client.dataset(dataset_name)

    # Check if the dataset exists; if not, create it
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "asia-southeast2"

    try:
        client.create_dataset(dataset)
        print(f"Create dataset '{dataset}'")
    except Conflict:  # The dataset already exists
        print(f"Dataset '{dataset}' already exists")

    table_ref = dataset_ref.table(table_name)

    # Check if the table exists; if not, create it with the specified schema
    try:
        client.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField(field["name"], field["type"].upper())
            for field in table_metadata["fields"]
        ]
        metadata = [
            bigquery.SchemaField("_raw_ts", "TIMESTAMP"),
            bigquery.SchemaField("published_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("_raw_id", "STRING"),
            bigquery.SchemaField("_metadata", "STRING"),
            bigquery.SchemaField("_wrapped_dek", "BYTES")
        ]
        schema = schema + metadata
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Table {table_ref} exists...")
    else:
        # If the table already exists, update the schema with any new columns
        print(f"Table {table_ref} exists...")
        table = client.get_table(table_ref)
        existing_columns = set(field.name for field in table.schema)
        new_columns = [
            bigquery.SchemaField(field["name"], field["type"].upper())
            for field in table_metadata["fields"]
            if field["name"] not in existing_columns
        ]
        if new_columns:
            table.schema += new_columns
            table = client.update_table(table, ["schema"])


if __name__ == "__main__":
    # Replace 'path_to_yaml_folder' with the actual path to the folder containing YAML files
    yaml_folder = "schema"

    for filename in os.listdir(yaml_folder):
        if filename.endswith(".yaml") or filename.endswith(".yml"):
            with open(os.path.join(yaml_folder, filename), "r") as file:
                table_metadata = yaml.safe_load(file)
                create_or_update_bigquery_table(table_metadata)
