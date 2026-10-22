locals {
  # Nom ACR globalement unique, stable pour ce projet/abonnement/environnement.
  registry_suffix = substr(sha256("${var.subscription_id}:fraud-api:${var.environment}"), 0, 10)
  registry_name   = "acrfraud${var.environment}${local.registry_suffix}"
  tags = {
    project     = "fraud-detection-pipeline"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_resource_group" "fraud" {
  name     = "rg-fraud-${var.environment}"
  location = var.location
  tags     = local.tags
}

resource "azurerm_container_registry" "fraud" {
  name                = local.registry_name
  resource_group_name = azurerm_resource_group.fraud.name
  location            = azurerm_resource_group.fraud.location
  sku                 = "Basic"
  admin_enabled       = false
  tags                = local.tags
}
