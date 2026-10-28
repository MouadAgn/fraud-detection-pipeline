# Le groupe préprod a été créé via Azure CLI pour limiter les droits GitHub
# à ce groupe. Le plan propose son import ; seul un futur apply l'enregistrera.
import {
  for_each = var.environment == "preprod" ? toset(["preprod"]) : toset([])
  to       = azurerm_resource_group.fraud
  id       = "/subscriptions/${var.subscription_id}/resourceGroups/rg-fraud-${each.value}"
}
