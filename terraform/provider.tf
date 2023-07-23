terraform {
  required_version = ">= 1.5.2"
}

provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
}
