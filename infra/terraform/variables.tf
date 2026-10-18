variable "broker" {
  description = "Which broker this cluster runs: kafka or kubemq. The ONLY intended difference between the two clusters."
  type        = string
  validation {
    condition     = contains(["kafka", "kubemq"], var.broker)
    error_message = "broker must be kafka or kubemq."
  }
}

variable "slot" {
  description = "Cluster slot, a or b. Repeat 3 of the matrix swaps brokers between slots (host-bias control)."
  type        = string
  validation {
    condition     = contains(["a", "b"], var.slot)
    error_message = "slot must be a or b."
  }
}

variable "project" {
  type = string
}

variable "zone" {
  type    = string
  default = "us-east1-b"
}

variable "name_prefix" {
  type    = string
  default = "kkbench"
}

# Pinned version, valid in the REGULAR channel. Pick one from
# `gcloud container get-server-config --zone <zone>` (channels[REGULAR].validVersions);
# it is recorded per incarnation.
variable "gke_version" {
  type    = string
  default = "1.36.4-gke.1247000"
}

variable "no_upgrade_start" {
  description = "RFC 3339 start of the 30-day NO_UPGRADES maintenance exclusion. up.sh sets it to the creation time."
  type        = string
  default     = "2026-09-28T00:00:00Z"
}

variable "broker_machine_type" {
  type    = string
  default = "n2-standard-8"
}

variable "driver_node_count" {
  description = "OMB worker nodes. 2 by default; 4 if the driver-ceiling control says the driver was the limit."
  type        = number
  default     = 2
}

variable "min_cpu_platform" {
  type    = string
  default = "Intel Ice Lake"
}

variable "local_ssd_count" {
  description = "375 GB NVMe Local SSDs per broker node, exposed as raw block and assembled into one RAID-0 local PV."
  type        = number
  default     = 2
}

variable "ttl_deadline" {
  description = "UTC deadline (YYYYMMDD-HHMM) after which ttl.sh destroys this cluster. Recorded as a label so any session can see it."
  type        = string
  default     = "unset"
}
