terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 8.4.0"
    }
  }
}

provider "google" {
  project = var.project
  zone    = var.zone
}
