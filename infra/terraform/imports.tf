# Les groupes préprod et prod ont été créés via Azure CLI pour limiter les droits GitHub
# au groupe cible. Le plan propose son import ; seul un futur apply l'enregistrera.
import {
  for_each = toset([var.environment])
  to       = azurerm_resource_group.fraud
  id       = "/subscriptions/${var.subscription_id}/resourceGroups/rg-fraud-${each.value}"
}
