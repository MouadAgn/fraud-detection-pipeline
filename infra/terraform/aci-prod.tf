data "azurerm_user_assigned_identity" "aci_prod" {
  count               = var.environment == "prod" ? 1 : 0
  name                = "id-fraud-aci-prod"
  resource_group_name = azurerm_resource_group.fraud.name
}

resource "azurerm_container_group" "fraud_api_prod" {
  count               = var.environment == "prod" ? 1 : 0
  name                = "aci-fraud-prod"
  resource_group_name = azurerm_resource_group.fraud.name
  location            = azurerm_resource_group.fraud.location
  os_type             = "Linux"
  ip_address_type     = "None"
  restart_policy      = "Always"
  tags                = local.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [data.azurerm_user_assigned_identity.aci_prod[0].id]
  }

  image_registry_credential {
    server                    = azurerm_container_registry.fraud.login_server
    user_assigned_identity_id = data.azurerm_user_assigned_identity.aci_prod[0].id
  }

  container {
    name   = "fraud-api"
    image  = "${azurerm_container_registry.fraud.login_server}/fraud-api@${var.fraud_api_prod_digest}"
    cpu    = 1
    memory = 2
  }
}
