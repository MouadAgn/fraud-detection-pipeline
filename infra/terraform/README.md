# Première infrastructure Azure : groupe de ressources et ACR

Cette étape décrit seulement un groupe de ressources et un registre ACR Basic.
Elle ne déploie pas encore ACI, Key Vault ou un compte de stockage. Aucun secret
ni identifiant personnel d'abonnement n'est enregistré dans la configuration.
ACR reste protégé par l'authentification Azure ; son compte administrateur est
désactivé. Le réseau n'est pas privé : on l'abordera séparément si nécessaire.
Le registre ne sera pas automatiquement rempli : le push de l'image viendra après.

## Comprendre les fichiers

- `backend.tf` : emplacement de l’état préprod dans Azure Blob Storage.
- `versions.tf` : versions Terraform et fournisseur AzureRM acceptées.
- `providers.tf` : connexion à Azure avec la session Azure CLI existante.
- `variables.tf` : abonnement, région et environnement à renseigner.
- `main.tf` : description du groupe de ressources et du registre.
- `outputs.tf` : noms/adresse du registre à récupérer après création.
- `environments/*.tfvars` : réglages pour préprod et prod.
- `.terraform.lock.hcl` : version et empreintes du fournisseur, conservées dans Git.

Le nom du registre est dérivé de l'abonnement et de l'environnement. Il est
stable, respecte le format alphanumérique ACR et réduit les risques de collision.
Azure reste l'autorité pour vérifier sa disponibilité globale.

## Préparer et examiner, sans créer les ressources

Depuis la racine du projet, après `az login` :

```bash
cd infra/terraform
export TF_VAR_subscription_id="$(az account show --query id -o tsv)"
terraform init -reconfigure
terraform fmt -check -recursive
terraform validate
terraform plan -var-file=environments/preprod.tfvars \
  -out=preprod.tfplan
```

`plan` lit Azure et prépare un plan local. Aucun `apply` n'est lancé. Le résultat
attendu sur un abonnement vierge est **2 créations, 0 modification, 0 suppression**.
Vérifier l'abonnement, la région, les noms et le niveau Basic avant de continuer.
ACR est une ressource facturable : vérifier la tarification et l'application de
l'offre gratuite/crédit de l'abonnement avant toute création.

Le fournisseur n'enregistre pas automatiquement les services Azure. Vérifier
l'état de Microsoft.ContainerRegistry avant la première création :

```bash
az provider show --namespace Microsoft.ContainerRegistry \
  --query registrationState -o tsv
```

S'il n'est pas `Registered`, enregistrer explicitement ce fournisseur avant de
régénérer le plan :

```bash
az provider register --namespace Microsoft.ContainerRegistry --wait
```

Cette commande modifie l'abonnement (activation du fournisseur) mais ne crée
pas le registre ACR.

## Créer après examen du plan

À exécuter uniquement lorsque le plan a été relu et que l'étape de création
est décidée :

```bash
terraform apply preprod.tfplan
terraform output
```

Un plan sauvegardé est appliqué tel quel : `apply preprod.tfplan` ne redemande
pas de confirmation interactive. Ne pas le lancer pendant l'étape de préparation.
Si des paramètres changent, régénérer et relire le plan.

## État Terraform et environnements

Le backend Azure conserve l'état dans :

- Storage Account : `stfraudtfmouad221026`
- Conteneur Blob : `tfstate`
- Fichier (clé du blob) : `preprod.tfstate`

Ce stockage a été créé séparément via Azure CLI dans `rg-fraud-tfstate`.
`use_azuread_auth = true` utilise l'identité Azure, sans clé partagée. Ton compte
possède le rôle Storage Blob Data Contributor sur ce stockage.
Le backend Azure permet de partager et verrouiller l'état entre exécutions.
L'état n'est pas un fichier de configuration : il ne doit pas être ajouté à Git.

`terraform init -reconfigure` connecte ce dossier au backend distant. Cette
commande ne crée pas ACR. Elle convient ici parce qu'aucun état de ressources
locales n'a encore été créé. Si un état local contenant des ressources existe,
le sauvegarder et utiliser une migration contrôlée avec `init -migrate-state`
plutôt que de l'ignorer avec `-reconfigure`.

Le nom `preprod.tfstate` est réservé à la préproduction. Ne pas utiliser
`prod.tfvars` avec cette même clé : cela pourrait modifier les ressources de
préprod. La configuration prod utilisera une clé distincte `prod.tfstate` ;
le fichier de variables seul ne sépare pas les états.

Pour GitHub Actions, la connexion OIDC et les permissions de son identité
restent à configurer. Avoir le backend distant ne connecte pas automatiquement
GitHub à Azure. Les commandes plan/apply ci-dessus documentent Terraform ;
le prochain déploiement sera organisé dans le workflow après configuration
de cette identité et validation du plan.

## Références

- https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/container_registry
- https://developer.hashicorp.com/terraform/language/state
