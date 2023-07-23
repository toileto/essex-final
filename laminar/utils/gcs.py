from google.cloud import storage

class GCSUtility:
    def __init__(self) -> None:
        """
        Initialize GCS client.
        """
        self.gcs_client: storage.client.Client = storage.Client()

    def list_files(self, bucket_name: str, folder_path: str) -> list:
        """
        List all files specified under the GCS folder.

        Args:
            bucket_name: GCS bucket name.
            folder_path: GCS folder name.
        
        Returns:
            GCS URI paths.
        """
        return [blob.name for blob in self.gcs_client.list_blobs(bucket_name, prefix=folder_path)]

    def load_as_string(self, bucket_name: str, blob_path: str) -> str:
        """
        Load string content of the GCS file.

        Args:
            bucket_name: GCS bucket name.
            blob_path: GCS file URI path.
        
        Returns:
            Loaded content of the GCS file.
        """
        gcs_bucket: storage.bucket.Bucket = self.gcs_client.bucket(bucket_name)
        gcs_blob: storage.blob.Blob = gcs_bucket.get_blob(blob_path)
        return gcs_blob.download_as_bytes().decode()
