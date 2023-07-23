resource "google_secret_manager_secret" "main" {
    secret_id = "laminar-service-account-${var.environment}"
    labels = {
        env = "${var.environment}"
        product = "${var.product}"
    }
    replication {
        automatic = true
    }
}

resource "google_secret_manager_secret_version" "main" {
    secret = google_secret_manager_secret.main.id
    secret_data = file("../service_account.${var.environment}.json")
}
