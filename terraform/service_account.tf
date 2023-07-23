resource "google_service_account" "main" {
    account_id = "laminar-general"
}

resource "google_project_iam_custom_role" "main" {
    role_id     = "laminarGeneral"
    title       = "Laminar General"
    permissions = [
        "bigquery.datasets.create",
        "bigquery.datasets.get",
        "bigquery.datasets.getIamPolicy",
        "bigquery.models.delete",
        "bigquery.routines.delete",
        "bigquery.tables.create",
        "bigquery.tables.delete",
        "bigquery.tables.export",
        "bigquery.tables.get",
        "bigquery.tables.getData",
        "bigquery.tables.updateData",
        "bigquery.jobs.create",
        "cloudbuild.builds.create",
        "dataflow.jobs.cancel",
        "dataflow.jobs.create",
        "dataflow.jobs.get",
        "dataflow.jobs.list",
        "dataflow.jobs.get",
        "dataflow.shuffle.read",
        "dataflow.shuffle.write",
        "dataflow.streamingWorkItems.commitWork",
        "dataflow.streamingWorkItems.getData",
        "dataflow.streamingWorkItems.getWork",
        "dataflow.workItems.lease",
        "dataflow.workItems.sendMessage",
        "dataflow.workItems.update",
        "pubsub.subscriptions.consume",
        "pubsub.subscriptions.get",
        "pubsub.topics.publish",
        "secretmanager.versions.access",
        "iam.serviceAccounts.actAs",
        "logging.logEntries.create",
        "monitoring.timeSeries.create",
        "storage.buckets.get",
        "storage.objects.create",
        "storage.objects.get",
        "storage.objects.list",
        "storage.objects.delete",
        "storage.objects.getIamPolicy"
    ]
}

resource "google_project_iam_member" "main" {
    project = "${var.gcp_project}"
    role = "projects/${var.gcp_project}/roles/${google_project_iam_custom_role.main.role_id}"
    member = "serviceAccount:${google_service_account.main.email}"
}
