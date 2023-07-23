resource "google_pubsub_topic" "main" {
    name = "${var.product}-source-${var.environment}"
    project = "${var.gcp_project}"
    labels = {
      env = "${var.environment}"
      product = "${var.product}"
    }
}

resource "google_pubsub_subscription" "bigquery" {
    name = "${var.product}-source-${var.environment}-sub-bigquery"
    topic = google_pubsub_topic.main.name
    project = "${var.gcp_project}"
    labels = {
      env = "${var.environment}"
      product = "${var.product}"
    }
    ack_deadline_seconds = 600
}

resource "google_pubsub_subscription" "monitor" {
    name = "${var.product}-source-${var.environment}-sub-monitor"
    topic = google_pubsub_topic.main.name
    project = "${var.gcp_project}"
    labels = {
      env = "${var.environment}"
      product = "${var.product}"
    }
    ack_deadline_seconds = 600
}
