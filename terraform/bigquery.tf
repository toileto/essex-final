resource "google_bigquery_dataset" "raw" {
    dataset_id = "raw_datalake"
    location = "${var.gcp_region}"
    labels = {
        env = "${var.environment}"
        product = "${var.product}"
    }
}

resource "google_bigquery_dataset" "l1" {
    dataset_id = "L1_datalake"
    location = "${var.gcp_region}"
    labels = {
        env = "${var.environment}"
        product = "${var.product}"
    }
}

resource "google_bigquery_dataset" "dlq" {
    dataset_id = "dead_letter"
    location = "${var.gcp_region}"
    labels = {
        env = "${var.environment}"
        product = "${var.product}"
    }
}

resource "google_bigquery_table" "raw_users" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "visibility_users"

  time_partitioning {
    type = "DAY"
    field = "_date_partition"
  }

  labels = {
    env = "${var.environment}"
  }

  schema = <<EOF
[
{"name": "id", "type": "STRING", "mode": "NULLABLE"},
{"name": "table", "type": "STRING", "mode": "NULLABLE"},
{"name": "db", "type": "STRING", "mode": "NULLABLE"},
{"name": "op", "type": "STRING", "mode": "NULLABLE"},
{"name": "data", "type": "STRING", "mode": "NULLABLE"},
{"name": "ts", "type": "STRING", "mode": "NULLABLE"},
{"name": "ts_ms", "type": "INT64", "mode": "NULLABLE"},
{"name": "metadata", "type": "STRING", "mode": "NULLABLE"},
{"name": "_ingest_ts_ms", "type": "INT64", "mode": "NULLABLE"},
{"name": "_date_partition", "type": "DATE", "mode": "NULLABLE"}
]
EOF
}

resource "google_bigquery_table" "l1_users" {
  dataset_id = google_bigquery_dataset.l1.dataset_id
  table_id   = "users"

  time_partitioning {
    type = "DAY"
    field = "modified_at"
  }

  labels = {
    env = "${var.environment}"
  }

  schema = <<EOF
[
{"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
{"name": "username", "type": "STRING", "mode": "NULLABLE"},
{"name": "email", "type": "STRING", "mode": "NULLABLE"},
{"name": "password", "type": "STRING", "mode": "NULLABLE"},
{"name": "login_count", "type": "INTEGER", "mode": "NULLABLE"},
{"name": "is_active", "type": "BOOLEAN", "mode": "NULLABLE"},
{"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
{"name": "modified_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
{"name": "_raw_ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
{"name": "published_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
{"name": "_raw_id", "type": "STRING", "mode": "NULLABLE"},
{"name": "_metadata", "type": "STRING", "mode": "NULLABLE"}
]
EOF
}

resource "google_bigquery_table" "l1_dlq_failures" {
  dataset_id = google_bigquery_dataset.dlq.dataset_id
  table_id   = "raw_to_l1_failures"

  time_partitioning {
    type = "DAY"
    field = "ts"
  }

  labels = {
    env = "${var.environment}"
  }

  schema = <<EOF
[
{"name": "id", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "source_database", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "source_table", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "destination_dataset", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "destination_table", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "ts", "type":	"TIMESTAMP", "mode":	"NULLABLE"},			
{"name": "exception_type", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "exception", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "traceback", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "ingest_ts", "type":	"TIMESTAMP", "mode":	"NULLABLE"}
]
EOF
}

resource "google_bigquery_table" "l1_dlq_unconfigured" {
  dataset_id = google_bigquery_dataset.dlq.dataset_id
  table_id   = "raw_to_l1_unconfigured"

  time_partitioning {
    type = "DAY"
    field = "ts"
  }

  labels = {
    env = "${var.environment}"
  }

  schema = <<EOF
[
{"name":"id", "type":	"STRING", "mode":	"NULLABLE"},			
{"name":"table", "type":	"STRING", "mode":	"NULLABLE"},			
{"name":"database", "type":	"STRING", "mode":	"NULLABLE"},			
{"name":"ts", "type":	"TIMESTAMP", "mode":	"NULLABLE"},			
{"name":"ingest_ts", "type":	"TIMESTAMP", "mode":	"NULLABLE"}
]
EOF
}

resource "google_bigquery_table" "raw_dlq_failures" {
  dataset_id = google_bigquery_dataset.dlq.dataset_id
  table_id   = "source_to_raw_failures"

  time_partitioning {
    type = "DAY"
    field = "ingest_ts"
  }

  labels = {
    env = "${var.environment}"
  }

  schema = <<EOF
[
{"name": "message", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "exception", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "traceback", "type":	"STRING", "mode":	"NULLABLE"},			
{"name": "ingest_ts", "type":	"TIMESTAMP", "mode":	"NULLABLE"}
]
EOF
}
