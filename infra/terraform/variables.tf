variable "subscription_id" {
  description = "Identifiant de l'abonnement Azure cible, fourni hors du dépôt."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", var.subscription_id))
    error_message = "Fournir un identifiant d'abonnement Azure au format UUID."
  }
}

variable "environment" {
  description = "Environnement cible : un état distinct doit être utilisé pour chacun."
  type        = string
  default     = "preprod"

  validation {
    condition     = contains(["preprod", "prod"], var.environment)
    error_message = "L'environnement doit être preprod ou prod."
  }
}

variable "location" {
  description = "Région Azure des ressources du projet."
  type        = string
  default     = "francecentral"
}
