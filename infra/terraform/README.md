# Première infrastructure Azure : groupe de ressources et ACR

Cette étape décrit seulement un groupe de ressources et un registre ACR Basic.
Elle ne déploie pas encore ACI, Key Vault ou un compte de stockage. Aucun secret
ni identifiant personnel d'abonnement n'est enregistré dans la configuration.
ACR reste protégé par l'authentification Azure ; son compte administrateur est
désactivé. Le réseau n'est pas privé : on l'abordera séparément si nécessaire.
Le registre ne sera pas automatiquement rempli : le push de l'image viendra après.

## Comprendre les fichiers

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
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -var-file=environments/preprod.tfvars \
  -state=preprod.tfstate -out=preprod.tfplan
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
terraform output -state=preprod.tfstate
```

Un plan sauvegardé est appliqué tel quel : `apply preprod.tfplan` ne redemande
pas de confirmation interactive. Ne pas le lancer pendant l'étape de préparation.
Si des paramètres changent, régénérer et relire le plan.

## État Terraform et environnements

Pour ce premier exercice local, l'état préprod se trouve dans `preprod.tfstate`.
L'état relie les ressources Azure à leur déclaration dans les fichiers. Il est
exclu de Git, comme le plan et le dossier `.terraform`. Le conserver et ne pas
le partager : un état peut contenir des informations sensibles.

Le fichier `prod.tfvars` prépare les paramètres de prod, mais ne crée aucun
second environnement. Une future exécution locale prod doit impérativement
utiliser **un état différent**, avec `-state=prod.tfstate` et `-out=prod.tfplan`.
Ne jamais changer simplement environment dans un état déjà utilisé : Terraform
pourrait remplacer les ressources de l'autre environnement.

**Avant d'exécuter Terraform dans GitHub Actions**, nous créerons un Storage
Account dédié à l'état et migrerons les états dans des blobs distincts pour
préprod/prod. Cela permettra de partager et verrouiller l'état entre exécutions.
On configurera ensuite l'identité GitHub/Azure sans mot de passe utilisateur.

## Références

- https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/container_registry
- https://developer.hashicorp.com/terraform/language/state
