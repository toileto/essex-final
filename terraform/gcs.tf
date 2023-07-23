resource "google_storage_bucket" "main" {
    name = "${var.gcp_project}-laminar-general"
    location = "${var.gcp_region}"
    project = "${var.gcp_project}"
    storage_class = "STANDARD"
    labels = {
        env = "${var.environment}"
        product = "${var.product}"
    }
    force_destroy = true
}

resource "google_storage_bucket" "schema" {
    name = "${var.gcp_project}-schema"
    location = "${var.gcp_region}"
    project = "${var.gcp_project}"
    storage_class = "STANDARD"
    labels = {
        env = "${var.environment}"
        product = "${var.product}"
    }
    force_destroy = true
}

resource "google_storage_bucket_object" "main" {
 name         = "L1/datalake/users.yaml"
 source       = "../schema/users.yaml"
 content_type = "text/plain"
 bucket       = google_storage_bucket.schema.id
}