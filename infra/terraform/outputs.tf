output "cluster_name" {
  value = google_container_cluster.this.name
}

output "zone" {
  value = var.zone
}

output "context" {
  value = "gke_${var.project}_${var.zone}_${google_container_cluster.this.name}"
}
