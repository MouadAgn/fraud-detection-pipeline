provider "azurerm" {
  features {}

  subscription_id = var.subscription_id

  # Aucun enregistrement automatique de services pendant un simple plan.
  # Microsoft.ContainerRegistry sera vérifié séparément avant apply.
  resource_provider_registrations = "none"
}
