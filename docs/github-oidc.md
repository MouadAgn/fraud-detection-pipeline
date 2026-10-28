# Première étape GitHub Actions : tester OIDC

Fichier : `.github/workflows/terraform-preprod.yml`.

Ce workflow teste uniquement la connexion à Azure. Il ne construit pas
l'image Docker et n'exécute ni terraform init, ni plan, ni apply.
La réussite confirme l'authentification, pas toutes les permissions sur les
ressources : l'accès au backend et la gestion d'ACR seront testés ensuite.

## Prérequis déjà préparés

- Environnement GitHub `preprod` sur ce dépôt.
- Variables de cet environnement : `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
  `AZURE_SUBSCRIPTION_ID`. Aucun client secret nécessaire.
- Application Entra et principal de service dédiés à GitHub.
- Confiance fédérée avec issuer GitHub Actions, audience
  `api://AzureADTokenExchange` et subject
  `repo:MouadAgn/fraud-detection-pipeline:environment:preprod`.

## Comprendre le fichier

- `workflow_dispatch` : lancement manuel depuis GitHub Actions.
- `if` : seul un lancement depuis `main` exécute le job.
- `runs-on` : GitHub fournit une machine Linux temporaire.
- `environment: preprod` : rend disponibles les variables de cet environnement
  et donne le subject OIDC attendu par Azure.
- `id-token: write` : autorise la demande du jeton d'identité GitHub ; cela
  n'accorde pas de permissions supplémentaires sur les ressources Azure.
- `vars.*` : identifiants configurés par le propriétaire dans GitHub,
  jamais lus depuis le contenu d'une issue ou d'une pull request.
- `azure/login` : échange la preuve GitHub contre une session Azure temporaire.
- Dernière étape : vérifie l'abonnement, le tenant et l'identité du service,
  puis affiche le nom de l'abonnement et un résumé.

Aucun checkout n'est nécessaire : cette étape ne lit pas les fichiers du dépôt.
L'action azure/login effectue aussi son nettoyage de session en fin de job.

## Lancer soi-même

1. Dépôt GitHub → **Actions**.
2. Ouvrir **Préprod - Tester la connexion Azure OIDC**.
3. **Run workflow** → branche **main** → **Run workflow**.
4. Ouvrir l'exécution puis le job **Vérifier la connexion à Azure**.
5. Vérifier les étapes vertes et le message de confirmation.

Si le job est ignoré, vérifier la branche choisie. Si la connexion échoue,
lire l'étape rouge : les noms de variables, issuer, subject et audience
OIDC doivent correspondre exactement. Ne pas partager de jetons d'accès.

La prochaine étape ajoutera Terraform et traitera l'import du groupe
`rg-fraud-preprod`, déjà créé manuellement, avant tout apply.

## Étape suivante : Terraform plan (sans création)

Le même fichier de workflow s'appelle désormais **Préprod - Terraform plan**.
Il reste manuel et limité à `main` et à l'environnement `preprod`.

Après le test de connexion, il récupère le code, installe Terraform 1.17.0,
vérifie le format, initialise le backend distant, valide et prépare un plan.
Les variables ARM indiquent au backend et au fournisseur AzureRM d'utiliser
OIDC ; la session Azure CLI sert au test d'identité, pas de clé partagée.
Les exécutions préprod sont sérialisées pour éviter les opérations concurrentes.

`infra/terraform/imports.tf` déclare l'import du groupe `rg-fraud-preprod`
créé manuellement. Ce bloc ne recrée pas le groupe. Le plan décrit l'import,
mais son enregistrement définitif dans l'état ne se fera qu'au futur apply.
Le plan peut aussi proposer de mettre à jour les tags du groupe pour qu'ils
correspondent au code, et de créer un registre ACR Basic.

Le backend peut écrire un état initial vide et prend un verrou durant le plan ;
cela ne crée pas ACR et ne modifie pas les ressources prévues par le plan.
Aucun `terraform apply` ou `terraform import` CLI n'est exécuté dans ce workflow.
Le plan binaire et sa version texte restent sur la machine temporaire GitHub ;
ils ne sont pas publiés comme artefacts. Le futur apply nécessitera un nouveau
plan revu ou une chaîne distincte avec conservation contrôlée du plan validé.

Pour exécuter : **Actions → Préprod - Terraform plan → Run workflow → main**.
Lire l'étape **Préparer le plan de préproduction sans appliquer** et son résumé.
Le premier résultat attendu est 1 groupe à importer, 1 ACR à créer et, selon
les tags existants, une modification du groupe. Aucune suppression attendue.

### Subject OIDC réellement observé

Le test GitHub a révélé un subject incluant les identifiants du propriétaire
et du dépôt. La confiance Azure a été corrigée pour correspondre exactement à :

```text
repo:MouadAgn@103049879/fraud-detection-pipeline@1315509705:environment:preprod
```

Ce subject remplace l'exemple sans identifiants donné plus haut. Toujours
comparer le subject réel dans les logs de connexion avec la règle Entra.
