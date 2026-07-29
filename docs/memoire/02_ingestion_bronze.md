# 02 — Couche Bronze : ingestion

> **Étape projet** : 1 — Bronze
> **Section mémoire visée** : Partie 2 — chaîne de traitement, ingestion
> **Code** : [`notebooks/01_bronze_ingestion.py`](../../notebooks/01_bronze_ingestion.py) · [`src/fraud/schemas.py`](../../src/fraud/schemas.py)

---

## 1. Le rôle de Bronze, et ce qu'on s'interdit d'y faire

La couche Bronze produit une **copie fidèle de la source**. Aucune transformation métier n'y est appliquée — pas même la correction de défauts pourtant identifiés au profilage :

| Défaut connu (note [01](01_profilage_initial.md)) | Traitement en Bronze |
| --- | --- |
| `source_row_index` — index technique sans valeur métier | Conservé |
| `unix_time` — décalé de 7 ans par rapport à l'horodatage | Conservé tel quel |
| Préfixe `fraud_` sur tous les commerçants | Conservé |
| `first`, `last`, `long` — noms ambigus en SQL | Conservés |

**Justification.** Cette discipline est ce qui rend le pipeline rejouable. Si Bronze corrigeait déjà la donnée, la découverte d'une erreur dans une règle de nettoyage imposerait de retourner chercher les fichiers sources — qui peuvent avoir été écrasés, archivés ou devenus inaccessibles. Avec un Bronze fidèle, il suffit de relancer Silver.

Bronze est le **point de retour en arrière** du pipeline. Le sacrifier pour un gain de propreté immédiat revient à échanger la capacité de correction future contre un confort présent.

## 2. Les six décisions techniques de l'ingestion

### 2.1 Schéma explicite plutôt qu'inférence

`inferSchema=True` est le réglage le plus courant, et le plus mauvais en production. Trois raisons de le refuser :

- **Coût** — l'inférence impose à Spark une lecture complète du fichier pour deviner les types, avant la lecture réelle. Sur 501 Mo, c'est un doublement du temps d'ingestion sans contrepartie.
- **Instabilité** — le type déduit dépend du contenu du lot. Une colonne numérique sur un lot peut devenir textuelle sur le suivant si une seule valeur alphanumérique y apparaît. Tout l'aval casse alors sans qu'aucune règle n'ait changé.
- **Absence de contrat** — un schéma déclaré et versionné dans Git documente ce que le pipeline attend de sa source. C'est une information de conception, pas un artefact d'exécution.

### 2.2 Toutes les colonnes métier en `STRING`

Un fichier CSV ne porte aucun type : tout y est du texte. Déclarer `amt` comme décimal dès la lecture revient déjà à **interpréter** la donnée, ce qui n'est pas le rôle de Bronze.

Le risque est concret. En mode `PERMISSIVE` — le comportement par défaut de Spark — une valeur qui ne se convertit pas au type déclaré est remplacée par `NULL`, **sans erreur ni avertissement**. On perdrait de la donnée de manière invisible, précisément dans la couche censée garantir la fidélité.

Le cas le plus démonstratif est `cc_num`. Lu comme entier, `2703186189652095` passe encore ; mais un numéro de carte est un **identifiant**, pas une quantité. Un zéro de tête serait perdu, un numéro à 19 chiffres déborderait la capacité d'un entier 64 bits, et aucune opération arithmétique n'a de sens sur cette colonne. Elle doit rester du texte.

Le typage devient ainsi une responsabilité explicite de la couche Silver, où il constitue un contrôle mesurable plutôt qu'un effet de bord de la lecture.

### 2.3 Validation de l'en-tête avant lecture

C'est le contrôle le moins visible et le plus important de l'étape.

Un schéma explicite s'applique **par position**, pas par nom. Si la source inversait deux colonnes de même type — `lat` et `long`, par exemple — Spark chargerait les données sans la moindre erreur, mais toutes les latitudes seraient des longitudes. Le défaut ne se manifesterait qu'au niveau des résultats du modèle, des semaines plus tard, et serait pratiquement impossible à rattacher à sa cause.

La fonction `validate_raw_header()` compare l'en-tête réel au contrat déclaré et signale la position exacte de chaque écart. Test de validation du contrôle, sur un en-tête où `lat` et `long` ont été inversés :

```
SchemaValidationError: L'en-tête du fichier source ne respecte pas le contrat de données.
  - position 13 : attendu 'lat', reçu 'long'
  - position 14 : attendu 'long', reçu 'lat'
```

Coût : quelques millisecondes. Risque éliminé : une corruption silencieuse et indétectable en aval.

### 2.4 Traitement des guillemets

Le fichier contient des champs à virgules internes — `"fraud_Rippin, Kub and Mann"`, `"Psychologist, counselling"`. Les options `quote` et `escape` sont donc déclarées explicitement plutôt que laissées au défaut. Sans interprétation correcte des guillemets, chaque virgule interne décalerait toutes les colonnes suivantes de la ligne, avec à la clé des données silencieusement corrompues.

### 2.5 Mode `FAILFAST` plutôt que `PERMISSIVE`

Une ligne structurellement corrompue doit **arrêter** l'ingestion, pas y entrer discrètement avec des valeurs nulles. Comme le schéma ne contient que des chaînes, `FAILFAST` ne peut se déclencher que sur une véritable anomalie de structure — un nombre de champs incorrect — c'est-à-dire exactement le cas où l'ingestion ne doit pas se poursuivre.

*Nuance de production* : sur un flux réel à fort volume, on préférerait rediriger les enregistrements corrompus vers un chemin de rejets (`badRecordsPath`) plutôt que d'interrompre tout un lot pour quelques lignes. Ici, la source est un fichier statique dont la volumétrie a été vérifiée : l'échec immédiat est le comportement le plus sûr.

### 2.6 `overwrite` et idempotence

La source est un **instantané complet** de deux fichiers statiques, non un flux incrémental. Le rechargement intégral est donc la sémantique correcte, et `overwrite` garantit l'**idempotence** : deux exécutions successives produisent un résultat identique. Avec `append`, un second passage dupliquerait les 1,85 M de lignes.

L'historique n'est pas perdu : Delta conserve chaque version de la table et `DESCRIBE HISTORY` en fournit la trace complète.

L'idempotence n'est pas un raffinement théorique. C'est un prérequis à l'orchestration (étape 6) : un job planifié qui échoue en cours d'exécution doit pouvoir être relancé tel quel, sans intervention manuelle ni nettoyage préalable.

## 3. Pourquoi la table n'est pas partitionnée

Le réflexe attendu serait de partitionner par date. Ce serait ici un contresens, et le démontrer chiffres à l'appui vaut mieux que d'appliquer une règle générale.

| Élément | Valeur |
| --- | ---: |
| Période couverte | 731 jours |
| Lignes totales | 1 852 394 |
| Lignes par jour (moyenne) | **~2 534** |
| Taille estimée d'une partition journalière | quelques centaines de Ko |

Partitionner par jour créerait 731 dossiers contenant chacun un fichier minuscule. C'est le **small files problem** : chaque lecture doit ouvrir des centaines de fichiers pour rassembler quelques mégaoctets, et le coût des métadonnées dépasse largement le gain d'élagage.

L'ordre de grandeur communément retenu est qu'une partition doit peser **au moins 1 Go**. Avec environ 200 Mo au total après compression Delta, cette table n'atteint pas la taille d'**une seule** partition. Elle reste donc non partitionnée, Delta assurant lui-même le regroupement des fichiers.

> Le partitionnement est l'optimisation la plus souvent appliquée à tort. La question n'est pas « par quoi partitionner ? » mais « le volume le justifie-t-il ? ». Ici, non.

## 4. Traçabilité d'ingestion

Trois colonnes sont ajoutées, préfixées `_` pour ne jamais être confondues avec les colonnes métier :

| Colonne | Question à laquelle elle répond |
| --- | --- |
| `_ingested_at` | Quand cette ligne est-elle entrée dans la plateforme ? |
| `_source_file` | De quel fichier provient-elle ? |
| `_ingestion_id` | Quel lot d'ingestion l'a produite ? |

**Détail d'implémentation important** : l'horodatage est calculé une seule fois côté Python, puis appliqué comme constante. La fonction `current_timestamp()` de Spark n'est volontairement pas utilisée, car elle est évaluée à l'exécution de chaque tâche : les lignes d'un même lot pourraient alors porter des horodatages différents selon la partition qui les traite. Un lot doit avoir **un** horodatage, faute de quoi la traçabilité devient approximative.

## 5. Contrôles de réconciliation source ↔ cible

Écrire une table ne prouve pas qu'on a écrit *la bonne* table. Trois contrôles comparent le résultat aux volumétries établies indépendamment lors du profilage :

| Contrôle | Valeur attendue |
| --- | ---: |
| Lignes issues de `fraudTrain.csv` | 1 296 675 |
| Lignes issues de `fraudTest.csv` | 555 719 |
| Total | 1 852 394 |
| Fraudes (`is_fraud = '1'`) | 9 651 |

Tout écart lève une exception et interrompt le notebook. Ces contrôles ne sont pas décoratifs : un mauvais traitement des guillemets modifierait simultanément le nombre de lignes et l'alignement des colonnes, et la comparaison des comptages le détecte immédiatement.

## 6. Résultat

| | |
| --- | --- |
| Table | `workspace.fraud_detection.bronze_transactions` |
| Lignes | 1 852 394 |
| Colonnes | 23 métier (toutes `STRING`) + 3 de traçabilité |
| Format | Delta Lake, non partitionné |
| Transformations métier | aucune |
| Idempotence | oui |

## 7. Ce qui évoluera à l'étape 8 (streaming)

Le chargement complet sera remplacé par **Auto Loader**, qui ne traite que les fichiers nouvellement déposés et maintient lui-même la liste de ceux déjà ingérés. Le mode d'écriture passera alors de `overwrite` à `append`, l'idempotence étant assurée par le suivi des fichiers plutôt que par l'écrasement.

Le schéma explicite, la validation d'en-tête et les colonnes de traçabilité restent identiques : c'est précisément l'intérêt de les avoir posés dès le batch.

---

**À retenir pour la soutenance**
Bronze ne corrige rien — c'est ce qui rend le pipeline rejouable. Trois décisions sont défendables en détail : le schéma explicite tout en texte (une valeur non convertible deviendrait `NULL` silencieusement en mode permissif), la validation d'en-tête (le schéma s'applique par position, une inversion `lat`/`long` corromprait les données sans erreur), et l'absence de partitionnement (2 534 lignes par jour rendraient le partitionnement journalier contre-productif).
