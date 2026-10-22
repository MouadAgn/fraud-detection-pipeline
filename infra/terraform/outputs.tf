output "resource_group_name" {
  description = "Groupe de ressources contenant le registre."
  value       = azurerm_resource_group.fraud.name
}

output "acr_name" {
  description = "Nom du registre pour az acr login."
  value       = azurerm_container_registry.fraud.name
}

output "acr_login_server" {
  description = "Adresse du registre, utilisée pour taguer et publier les images."
  value       = azurerm_container_registry.fraud.login_server
}

output "acr_id" {
  description = "Identifiant Azure du registre, pour les permissions futures."
  value       = azurerm_container_registry.fraud.id
}
