# GitHub Actions : Terraform préprod avec OIDC

Fichier : `.github/workflows/terraform-preprod.yml`.

## Prérequis configurés

- Environnement GitHub `preprod` avec les variables `AZURE_CLIENT_ID`,
  `AZURE_TENANT_ID` et `AZURE_SUBSCRIPTION_ID` (identifiants, sans client secret).
- Application Entra, principal de service et confiance fédérée GitHub.
- Rôle Contributor sur `rg-fraud-preprod` et Storage Blob Data Contributor
  sur le conteneur `tfstate` pour ce principal.
- Fournisseur Microsoft.ContainerRegistry enregistré dans l'abonnement.

`id-token: write` permet au job de demander une preuve d'identité GitHub.
Azure vérifie la confiance fédérée, puis accorde une session temporaire.
Les rôles Azure déterminent ensuite les opérations autorisées.

## Workflow actuel : plan ou apply

Le fichier s'appelle désormais **Préprod - Terraform plan ou apply**.
Il reste manuel, limité à `main`, avec l'identité OIDC de `preprod`.
Il récupère le code, installe Terraform 1.17.0, vérifie le format, initialise
le backend Azure, valide et sauvegarde un nouveau plan dans `preprod.tfplan`.
Les variables ARM utilisent OIDC pour le backend et le fournisseur AzureRM.
Les exécutions préprod sont sérialisées.

1. Ouvrir **Actions → Préprod - Terraform plan ou apply → Run workflow**.
2. Choisir **main**.
3. Laisser la case de création décochée pour un **plan seul** (valeur par défaut).
4. Cocher la case pour demander **plan puis apply** et créer les ressources.

La case autorise l'application du nouveau plan calculé dans cette exécution,
qui peut différer d'un plan précédent si le code ou Azure a changé. Il n'y a
pas de pause d'approbation supplémentaire entre plan et apply. Pour commencer,
relire le plan seul, puis lancer sur le même code avec la case cochée.
L'étape apply utilise exactement le fichier sauvegardé par l'étape plan du
même job ; elle ne recalcule pas le plan. Un échec d'une étape précédente
empêche l'application. Aucun lancement n'est automatique sur un push.

`infra/terraform/imports.tf` reconnaît le groupe `rg-fraud-preprod` existant.
Le premier apply enregistre son import dans l'état, ajoute les tags et crée
ACR Basic. Il ne déploie pas ACI et ne pousse pas encore l'image Docker.
ACR est facturable. Les noms et l'adresse du registre sont affichés dans
le résumé après apply. L'état est enregistré dans le blob `preprod.tfstate`.
Les plans restent sur la machine temporaire GitHub, sans publication d'artefact.

### Subject OIDC réellement observé

Le test GitHub a révélé un subject incluant les identifiants du propriétaire
et du dépôt. La confiance Azure a été corrigée pour correspondre exactement à :

```text
repo:MouadAgn@103049879/fraud-detection-pipeline@1315509705:environment:preprod
```

Ce subject remplace l'exemple sans identifiants donné plus haut. Toujours
comparer le subject réel dans les logs de connexion avec la règle Entra.
