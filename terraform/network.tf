resource "google_compute_network" "main" {
  name = "${var.product}-vpc-general-1-${var.environment}"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "main" {
  name = "${var.product}-subnet-general-1-${var.environment}"
  ip_cidr_range = "172.16.0.0/16"
  region = "${var.gcp_region}"
  network = google_compute_network.main.id
  private_ip_google_access = true
}
