# Stockage distant du fichier d'état de préproduction.
# Le Storage Account et le conteneur Blob existent déjà, créés avec Azure CLI.
terraform {
  backend "azurerm" {
    storage_account_name = "stfraudtfmouad221026"
    container_name       = "tfstate"
    key                  = "preprod.tfstate"
    use_azuread_auth     = true
  }
}
