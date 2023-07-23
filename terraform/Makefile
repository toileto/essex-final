# SA
plan_service_account:
	terraform plan \
	-target=google_service_account.main \
	-target=google_project_iam_custom_role.main \
	-target=google_project_iam_member.main

apply_service_account:
	terraform apply \
	-target=google_service_account.main \
	-target=google_project_iam_custom_role.main \
	-target=google_project_iam_member.main

# GCS
plan_gcs:
	terraform plan \
	-target=google_storage_bucket.main \
	-target=google_storage_bucket.schema \
	-target=google_storage_bucket_object.main

apply_gcs:
	terraform apply \
	-target=google_storage_bucket.main \
	-target=google_storage_bucket.schema \
	-target=google_storage_bucket_object.main

# Pub/Sub
plan_pubsub:
	terraform plan \
	-target=google_pubsub_topic.main \
	-target=google_pubsub_subscription.bigquery \
	-target=google_pubsub_subscription.monitor

apply_pubsub:
	terraform apply \
	-target=google_pubsub_topic.main \
	-target=google_pubsub_subscription.bigquery \
	-target=google_pubsub_subscription.monitor

# BigQuery
plan_bigquery:
	terraform plan \
	-target=google_bigquery_dataset.raw \
	-target=google_bigquery_dataset.l1 \
	-target=google_bigquery_dataset.dlq \
	-target=google_bigquery_table.raw_users \
	-target=google_bigquery_table.l1_users \
	-target=google_bigquery_table.l1_dlq_failures \
	-target=google_bigquery_table.l1_dlq_unconfigured \
	-target=google_bigquery_table.raw_dlq_failures

apply_bigquery:
	terraform apply \
	-target=google_bigquery_dataset.raw \
	-target=google_bigquery_dataset.l1 \
	-target=google_bigquery_dataset.dlq \
	-target=google_bigquery_table.raw_users \
	-target=google_bigquery_table.l1_users \
	-target=google_bigquery_table.l1_dlq_failures \
	-target=google_bigquery_table.l1_dlq_unconfigured \
	-target=google_bigquery_table.raw_dlq_failures

# Secret Manager
plan_gsm:
	terraform plan \
	-target=google_secret_manager_secret.main \
	-target=google_secret_manager_secret_version.main

apply_gsm:
	terraform apply \
	-target=google_secret_manager_secret.main \
	-target=google_secret_manager_secret_version.main

# Network
plan_network:
	terraform plan \
	-target=google_compute_network.main \
	-target=google_compute_subnetwork.main

apply_network:
	terraform apply \
	-target=google_compute_network.main \
	-target=google_compute_subnetwork.main
