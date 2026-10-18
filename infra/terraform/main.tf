locals {
  cluster_name = "${var.name_prefix}-${var.slot}"
  labels = {
    lifecycle    = "throwaway"
    purpose      = "kubemq-kafka-benchmark"
    broker       = var.broker
    slot         = var.slot
    ttl-deadline = lower(var.ttl_deadline)
  }
  # Everything that is not a broker, driver or monitoring workload is kept off the
  # broker nodes by label selection; the broker pool itself carries no taint because
  # the KubeMQ operator does not expose tolerations, and both sides must be scheduled
  # the same way.
  node_labels_common = {
    "kkbench/slot" = var.slot
  }
}

resource "google_container_cluster" "this" {
  name     = local.cluster_name
  location = var.zone

  min_master_version = var.gke_version
  # GKE no longer lets new projects opt out of release channels. The equivalent of
  # "static version, no upgrades": REGULAR channel, version pinned below, and a
  # NO_UPGRADES maintenance exclusion (GKE's maximum, 30 days) from creation onward.
  release_channel {
    channel = "REGULAR"
  }

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false

  resource_labels = local.labels

  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {}

  # Our own Prometheus on the monitoring pool is the only metrics path. Google's
  # managed agents are turned off so they do not take CPU on broker nodes.
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS"]
  }
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
    managed_prometheus {
      enabled = false
    }
  }

  maintenance_policy {
    # Maintenance is never wanted mid-run: every upgrade is excluded for 30 days.
    daily_maintenance_window {
      start_time = "03:00"
    }
    maintenance_exclusion {
      exclusion_name = "benchmark-no-upgrades"
      start_time     = var.no_upgrade_start
      end_time       = timeadd(var.no_upgrade_start, "720h")
      exclusion_options {
        scope = "NO_UPGRADES"
      }
    }
  }
}

resource "google_container_node_pool" "broker" {
  name       = "broker"
  cluster    = google_container_cluster.this.id
  location   = var.zone
  version    = var.gke_version
  node_count = 3

  management {
    auto_repair  = false
    auto_upgrade = true # required inside a release channel; blocked by the NO_UPGRADES exclusion
  }

  node_config {
    machine_type     = var.broker_machine_type
    min_cpu_platform = var.min_cpu_platform
    image_type       = "COS_CONTAINERD"
    disk_type        = "pd-balanced"
    disk_size_gb     = 100
    spot             = false
    # Raw block, NOT ephemeral-storage mode: a pod delete must never wipe broker data.
    local_nvme_ssd_block_config {
      local_ssd_count = var.local_ssd_count
    }
    labels          = merge(local.node_labels_common, { "kkbench/pool" = "broker" })
    resource_labels = local.labels
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

resource "google_container_node_pool" "driver" {
  name       = "driver"
  cluster    = google_container_cluster.this.id
  location   = var.zone
  version    = var.gke_version
  node_count = var.driver_node_count

  management {
    auto_repair  = false
    auto_upgrade = true # required inside a release channel; blocked by the NO_UPGRADES exclusion
  }

  node_config {
    machine_type     = var.broker_machine_type
    min_cpu_platform = var.min_cpu_platform
    image_type       = "COS_CONTAINERD"
    disk_type        = "pd-balanced"
    disk_size_gb     = 100
    spot             = false
    labels           = merge(local.node_labels_common, { "kkbench/pool" = "driver" })
    resource_labels  = local.labels
    taint {
      key    = "kkbench/pool"
      value  = "driver"
      effect = "NO_SCHEDULE"
    }
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

resource "google_container_node_pool" "monitoring" {
  name       = "monitoring"
  cluster    = google_container_cluster.this.id
  location   = var.zone
  version    = var.gke_version
  node_count = 1

  management {
    auto_repair  = false
    auto_upgrade = true # required inside a release channel; blocked by the NO_UPGRADES exclusion
  }

  node_config {
    machine_type    = "e2-standard-4"
    image_type      = "COS_CONTAINERD"
    disk_type       = "pd-balanced"
    disk_size_gb    = 100
    spot            = false
    labels          = merge(local.node_labels_common, { "kkbench/pool" = "monitoring" })
    resource_labels = local.labels
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}
