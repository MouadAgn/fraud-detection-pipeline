# 04 — Couche Gold : construction des variables explicatives

> **Étape projet** : 3 — Gold
> **Section mémoire visée** : Partie 2 — construction des variables explicatives
> **Code** : [`src/fraud/features.py`](../../src/fraud/features.py) · [`notebooks/03_gold_features.py`](../../notebooks/03_gold_features.py)

---

## 1. L'enjeu

C'est l'étape qui différencie ce projet d'un notebook d'apprentissage. Un modèle appliqué aux colonnes brutes — montant, catégorie, heure — n'apprend que des corrélations de surface. Ce qui distingue réellement une fraude d'un achat légitime n'est pas la transaction prise isolément, mais son **écart au comportement habituel du porteur**.

Cette information n'existe dans aucune colonne. Elle doit être construite.

C'est aussi l'étape où une erreur coûte le plus cher, parce qu'elle est **silencieuse** : une fuite temporelle produit des métriques excellentes. Rien dans l'évaluation ne la signale ; elle ne se révèle qu'en production.

## 2. La fuite temporelle

### Le mécanisme

Une variable comme « montant moyen historique du client » se calcule sur une fenêtre. Si cette fenêtre inclut la transaction courante, la variable explicative contient une information **qui n'existait pas au moment où la décision devait être prise**.

L'effet est particulièrement pervers sur les fraudes. Une transaction frauduleuse de 900 $ chez un client qui dépense habituellement 50 $ tire fortement sa propre moyenne vers le haut. Incluse dans sa propre fenêtre, elle apparaît alors *moins* atypique qu'elle ne l'est — et surtout, le simple fait que la moyenne soit anormalement élevée devient un indice de fraude. Le modèle apprend à détecter sa propre contamination.

### La règle appliquée

Toutes les fenêtres sont bornées à `-1` : strictement les transactions **antérieures** du même porteur, jamais la courante.

### La preuve, plutôt que l'affirmation

Écrire « les fenêtres sont bien bornées » n'a aucune valeur : c'est exactement ce qu'écrirait quelqu'un qui s'est trompé. Trois contrôles vérifient donc une **propriété mathématique du résultat**, indépendamment du code qui l'a produit.

| Contrôle | Propriété vérifiée |
| --- | --- |
| `history_count_mismatch` | Pour la k-ième transaction d'un porteur, `customer_txn_count_hist` vaut **exactement k−1** |
| `first_txn_has_history` | La première transaction d'un porteur n'a pas de montant moyen historique |
| `velocity_not_monotonic` | `txn_count_1h ≤ txn_count_24h ≤ txn_count_7d ≤ historique complet` |

Le premier est le plus fort. Il ne se contente pas de détecter l'inclusion de la ligne courante : il vérifie que la fenêtre contient **ni plus ni moins** que les transactions antérieures. Une violation, et le notebook s'arrête.

## 3. Deux familles de fenêtres, et pourquoi elles diffèrent

Spark propose deux bornages, qui ne répondent pas à la même question.

| | Bornage | Question | Usage |
| --- | --- | --- | --- |
| `rangeBetween(-N, -1)` | sur la **valeur** de la colonne de tri | « combien dans la dernière heure ? » | Vélocité |
| `rowsBetween(unbounded, -1)` | sur le **nombre de lignes** | « quel est son comportement habituel ? » | Historique |

Confondre les deux est une erreur courante. Pour un compteur de vélocité, `rowsBetween(-10, -1)` répondrait à « combien parmi les 10 précédentes ? » — une question sans intérêt métier, puisque ces dix transactions peuvent s'étaler sur une heure comme sur six mois.

**Contrainte technique** : `rangeBetween` n'accepte qu'**une seule** colonne de tri. Les fenêtres de vélocité trient donc sur le seul horodatage, tandis que les fenêtres d'historique ajoutent l'identifiant de transaction comme critère de départage. Sans ce départage, deux transactions du même porteur à la même seconde seraient ordonnées arbitrairement et le résultat varierait d'une exécution à l'autre — le pipeline cesserait d'être reproductible.

Cette contrainte impose aussi la présence de `event_time_unix` en couche Silver : `rangeBetween` ne sait borner que sur une valeur numérique, pas sur un horodatage. C'est la raison d'être de cette colonne, décidée dès l'étape 2.

## 4. Pourquoi les variables sont calculées avant le découpage train/test

La question mérite d'être anticipée, car la réponse est contre-intuitive.

Les variables sont calculées sur **l'historique complet et continu**, périodes d'entraînement et de test confondues. Ce n'est pas une fuite : chaque ligne ne voit que son propre passé, et le passé d'une transaction de test inclut légitimement des transactions de la période d'entraînement — c'est exactement ce qui se produirait en production, où le modèle dispose de tout l'historique du client.

Découper d'abord aurait au contraire créé un **artefact** : les premières transactions du jeu de test auraient perdu leur historique et se seraient présentées comme des clients inconnus, ce qu'elles ne sont pas. On aurait dégradé artificiellement l'évaluation, ce qui n'est pas plus rigoureux que de la gonfler.

L'ordre correct est donc : **variables → découpage → SMOTE**.

## 5. Les variables construites

### 5.1 Vélocité — le scénario de rafale

`txn_count_1h` · `txn_count_24h` · `txn_count_7d` · `amount_sum_1h` · `amount_sum_24h` · `amount_sum_7d` · `seconds_since_previous_txn`

Les largeurs de fenêtre ne sont pas conventionnelles, elles sont **justifiées par la cadence mesurée** (note [01](01_profilage_initial.md), constat 7) :

| Fenêtre | Justification chiffrée |
| --- | --- |
| **1 h** | 43,3 % des fraudes surviennent moins d'une heure après la précédente sur la même carte, contre un rythme normal d'une transaction toutes les 4,6 h. Le contraste est maximal à cette échelle. |
| **24 h** | Couvre un cycle d'activité complet — le 95ᵉ percentile du délai normal est à 31,7 h — et capte l'épisode de fraude entier plutôt qu'une rafale isolée. |
| **7 j** | Référence d'activité récente, sur laquelle les deux fenêtres courtes se détachent. |

### 5.2 Écart au comportement habituel — le cœur du dispositif

`customer_amount_mean_hist` · `customer_amount_std_hist` · `customer_amount_max_hist` · **`amount_zscore_vs_history`** · `amount_ratio_vs_mean`

C'est la famille la plus directement liée à la problématique du mémoire, et l'argument mérite d'être développé.

Un **seuil absolu** — « au-delà de 500 $, c'est suspect » — a deux défauts symétriques :

- il déclenche en permanence chez les porteurs à fort pouvoir d'achat, dont les transactions à 500 $ sont ordinaires : ce sont des **faux positifs par construction** ;
- il ne se déclenche jamais chez les porteurs modestes, chez qui une fraude à 200 $ est pourtant flagrante : ce sont des **faux négatifs par construction**.

Une **mesure relative au porteur** ne souffre d'aucun des deux. Le z-score répond à « à combien d'écarts-types de ses propres habitudes se situe cette transaction ? », c'est-à-dire à la question métier réelle : *inhabituel pour lui*.

C'est la réponse la plus directe apportée par le feature engineering à la minimisation des faux positifs — avant même l'optimisation du seuil de décision (note 06).

Le z-score n'est calculé que si l'écart-type historique est strictement positif. En deçà de deux transactions antérieures, ou pour un porteur aux montants identiques, il n'a pas de sens et reste non renseigné plutôt que de produire une division par zéro. `amount_ratio_vs_mean` prend le relais dans ces cas, avec une lecture plus directe et une robustesse supérieure quand la variance est faible.

### 5.3 Nouveauté

`is_new_merchant_for_customer` · `is_new_category_for_customer` · `customer_category_share_hist`

Une carte compromise est typiquement utilisée chez des commerçants que son porteur n'a jamais fréquentés.

La formulation compte : la variable ne dit pas « ce commerçant est suspect » mais « **ce client n'y était jamais allé** ». La nuance évite de pénaliser systématiquement les petits commerçants peu fréquentés, ce qui serait à la fois injuste et une source de faux positifs.

### 5.4 Temporel

`hour_of_day` · `day_of_week` · `is_weekend` · `is_night`

L'heure est le signal le plus fort du corpus — le taux de fraude est 10 à 30 fois supérieur entre 22 h et 3 h (constat 4). Elle est conservée parce qu'elle correspond à un phénomène métier réel, mais **son poids sera surveillé dans l'analyse SHAP** : si elle écrase toutes les autres variables, c'est le signe que le modèle apprend la règle de génération du jeu simulé plutôt qu'un comportement de fraude. Ce point est repris dans le bilan critique.

## 6. Valeurs manquantes : une absence n'est pas un zéro

La première transaction d'un porteur n'a pas d'historique. Deux traitements coexistent, et la distinction est délibérée.

| Cas | Valeur | Justification |
| --- | --- | --- |
| Compteur sur fenêtre vide | `0` | « aucune transaction dans l'heure précédente » est une **mesure**, pas une absence d'information |
| Moyenne sans historique | `NULL` | le montant habituel d'un porteur sans passé est **indéterminé**, pas nul |

Imputer `0` à la moyenne reviendrait à affirmer au modèle que ce client dépense habituellement 0 $, ce qui est faux — et le conduirait à considérer toute transaction comme infiniment atypique.

XGBoost traite nativement les valeurs manquantes : à chaque nœud, il apprend la direction par défaut qui minimise la perte. Lui transmettre l'absence est donc **plus informatif** que la déguiser. C'est un argument à connaître, car l'imputation systématique est un réflexe répandu que ce choix contredit délibérément.

## 7. Ce qui a été écarté, et pourquoi

| Variable | Motif |
| --- | --- |
| **Distance géographique** porteur/commerçant | Mesurée puis écartée : 76,11 km en moyenne pour les transactions légitimes contre 76,27 km pour les fraudes. Distributions indiscernables (note [01](01_profilage_initial.md), constat 5). C'est une limite du corpus simulé, pas une conclusion générale — sur des données réelles, cette variable serait vraisemblablement l'une des plus fortes. |
| **`merchant_name`** | 693 modalités. L'encoder produirait un modèle qui **mémorise des commerçants** au lieu d'apprendre des comportements, et qui ne saurait rien faire d'un commerçant nouveau. L'information utile est portée par `is_new_merchant_for_customer`. |
| **Identifiants** (`transaction_id`, `customer_id`) | Conservés dans la table pour joindre, auditer et découper, mais exclus des variables explicatives : un modèle qui apprend l'identifiant du client n'apprend rien de généralisable. |

Documenter les variables écartées vaut autant que documenter celles retenues. Un jury qui a lu la méthodologie annoncer une variable de distance demandera pourquoi elle a disparu : « elle a été testée, mesurée, et écartée sur preuve » est une réponse solide.

## 8. Résultats mesurés

### 8.1 Les contrôles anti-fuite passent, et la preuve se lit à la main

Les trois contrôles rapportent **0 violation** sur 1 852 394 lignes. Au-delà du contrôle automatique, la propriété se vérifie manuellement sur les premières transactions d'un porteur :

| Transaction | `amount` | `customer_txn_count_hist` | `customer_amount_mean_hist` | Vérification |
| ---: | ---: | ---: | ---: | --- |
| 1 | 1,57 | 0 | — | Aucun historique |
| 2 | 267,29 | 1 | **1,57** | = montant de la transaction 1 |
| 3 | 229,30 | 2 | **134,43** | (1,57 + 267,29) / 2 |
| 4 | 73,69 | 3 | **166,05** | (1,57 + 267,29 + 229,30) / 3 |

Le z-score se recalcule de même : sur la transaction 3, l'écart-type de {1,57 ; 267,29} vaut 187,89, d'où (229,30 − 134,43) / 187,89 = **0,5049** — la valeur produite par le pipeline.

Cette vérifiabilité manuelle est le meilleur argument disponible en soutenance : la démonstration ne repose ni sur la confiance accordée au code, ni sur un contrôle automatique qu'il faudrait croire sur parole.

### 8.2 Pouvoir discriminant des variables

Moyenne de chaque variable numérique par classe, sur l'ensemble du corpus.

| Variable | Légitime | Fraude | Rapport |
| --- | ---: | ---: | ---: |
| **`amount_zscore_vs_history`** | **0,001** | **3,897** | — *(voir ci-dessous)* |
| `amount_sum_1h` | 12,93 | 413,15 | × 32,0 |
| `amount` | 67,65 | 530,66 | × 7,8 |
| `amount_ratio_vs_mean` | 0,98 | 7,12 | × 7,3 |
| `amount_sum_24h` | 280,80 | 1 937,15 | × 6,9 |
| `txn_count_1h` | 0,198 | 0,677 | × 3,4 |
| `amount_sum_7d` | 1 836,21 | 3 397,89 | × 1,9 |
| `customer_amount_mean_hist` | 70,06 | 114,10 | × 1,6 |
| `customer_amount_std_hist` | 134,85 | 159,82 | × 1,2 |
| `customer_txn_count_hist` | 1 301,37 | 827,53 | × 0,64 |
| `seconds_since_previous_txn` | 30 990 | 21 127 | × 0,68 |
| `txn_count_7d` | 26,53 | 19,28 | × 0,73 |
| `customer_category_share_hist` | 0,082 | 0,090 | × 1,10 |
| `hour_of_day` | 12,80 | 14,05 | × 1,10 |
| `customer_age` | 45,75 | 48,32 | × 1,06 |
| `txn_count_24h` | 4,11 | 4,31 | × 1,05 |

**Le z-score du montant contre l'historique du porteur domine nettement.** Une transaction légitime se situe à **0,001 écart-type** des habitudes de son porteur — ce qui est sa définition même — tandis qu'une fraude se situe à **3,897 écarts-types au-dessus**.

C'est la validation empirique de la démarche : la variable conçue spécifiquement pour répondre à la problématique des faux positifs est aussi la plus discriminante du jeu.

> **Précaution de lecture.** Le rapport des moyennes vaut ici près de 2 900, mais ce chiffre ne doit **pas** être cité : il est mécaniquement gonflé par un dénominateur voisin de zéro. La formulation correcte est l'écart absolu — *0,001 contre 3,897 écarts-types* — qui décrit la même réalité sans exposer à une objection méthodologique.

**La vélocité fonctionne.** `amount_sum_1h` sépare les classes d'un facteur 32 et `txn_count_1h` d'un facteur 3,4, ce qui confirme le scénario de rafale identifié au profilage. Les fenêtres larges sont en revanche peu informatives (`txn_count_24h` à × 1,05) : le signal est bien concentré sur l'heure précédente, exactement comme la cadence mesurée le laissait attendre.

### 8.3 Deux pièges de lecture, et une alerte pour la suite

**Le rapport de moyennes ne convient pas aux variables cycliques.** `hour_of_day` affiche × 1,10, ce qui suggérerait une variable inutile. C'est faux : comparer des moyennes d'heures (12,8 contre 14,0) n'a aucun sens quand 23 h et 1 h sont voisines dans la réalité mais opposées en moyenne. Le profilage a établi un taux de fraude 10 à 30 fois supérieur la nuit ; ce signal existe bien, il est porté par `is_night` et invisible à cet indicateur.

**Deux variables sont potentiellement des indicateurs temporels déguisés.** `customer_txn_count_hist` (× 0,64) et `txn_count_7d` (× 0,73) indiquent que les fraudes touchent des porteurs ayant moins d'historique. Mais ces variables **croissent mécaniquement avec le temps écoulé** depuis le début du corpus, or le taux de fraude y décroît (0,579 % sur la période d'entraînement contre 0,386 % sur celle de test). Elles risquent donc d'encoder « à quelle date on se situe » plutôt qu'un comportement — une information qui ne généraliserait pas au-delà du corpus.

Ce point est à vérifier explicitement dans l'analyse SHAP (note 07) et à reprendre dans le bilan critique.

### 8.4 Valeurs manquantes

| Variable | Manquantes | Taux |
| --- | ---: | ---: |
| `seconds_since_previous_txn` | 999 | 0,054 % |
| `customer_amount_mean_hist` | 999 | 0,054 % |
| `amount_ratio_vs_mean` | 999 | 0,054 % |
| `customer_category_share_hist` | 999 | 0,054 % |
| `customer_amount_std_hist` | 1 998 | 0,108 % |
| `amount_zscore_vs_history` | 1 998 | 0,108 % |

Les chiffres confirment l'exactitude du partitionnement : **999 valeurs manquantes correspondent exactement à une par porteur** — sa première transaction, qui n'a pas d'antécédent. Les 1 998 des variables dépendant de l'écart-type correspondent aux **deux premières transactions** de chaque porteur, puisqu'il en faut au moins deux pour calculer une variance.

Une valeur différente aurait signalé une erreur de partitionnement, par exemple des fenêtres calculées sur l'ensemble du corpus au lieu de chaque porteur.

## 9. Résultat

| | |
| --- | --- |
| Table produite | `workspace.fraud_detection.gold_features` |
| Lignes | 1 852 394 — inchangé depuis Silver |
| Variables comportementales créées | 20 |
| Variables explicatives soumises au modèle | 26 |
| Contrôles anti-fuite | 3, bloquants |
| Fenêtres temporelles | 1 h, 24 h, 7 j, historique complet |

Toutes les colonnes sont documentées en français dans Unity Catalog et dans le [dictionnaire de données](../dictionnaire_donnees.md).

---

**À retenir pour la soutenance**
Trois points portent cette étape. **La fuite temporelle est le piège central du feature engineering en détection de fraude**, et elle est ici écartée par preuve et non par affirmation : le contrôle vérifie que la k-ième transaction d'un porteur voit exactement k−1 antécédents. **Calculer les variables avant le découpage n'est pas une fuite** — chaque ligne ne voit que son passé, et découper d'abord aurait au contraire amputé les premières transactions du test de leur historique. **Le z-score du montant contre l'historique du porteur est la réponse la plus directe du feature engineering à la problématique des faux positifs** : un seuil absolu déclenche par construction chez les gros dépensiers et rate par construction les fraudes chez les petits.
