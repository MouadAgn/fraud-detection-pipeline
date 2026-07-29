# 07 — Explicabilité : analyse SHAP

> **Étape projet** : 5 — Scoring
> **Section mémoire visée** : Partie 3 — explicabilité et interprétation du modèle
> **Code** : [`notebooks/05_scoring_eval.py`](../../notebooks/05_scoring_eval.py), section 6

---

## 1. Pourquoi l'explicabilité n'est pas optionnelle ici

En contexte bancaire réglementé, un refus de transaction doit pouvoir être justifié — devant le client, devant l'analyste qui traite l'alerte, et devant le régulateur. Un modèle incapable d'expliquer ses décisions n'est pas déployable, quelle que soit sa performance.

**Pourquoi SHAP plutôt que l'importance native de XGBoost.** Cette dernière compte les usages d'une variable dans les arbres, ce qui favorise mécaniquement les variables à forte cardinalité et n'explique **aucune décision particulière** : elle décrit le modèle, pas ses prédictions. SHAP attribue à chaque variable sa contribution à l'écart entre la prédiction courante et la prédiction moyenne. Elle est **additive et locale**, donc utilisable pour justifier un blocage précis.

**Regroupement par variable d'origine.** Le prétraitement éclate `customer_state` en cinquante et une colonnes binaires. Comparer ces colonnes une à une aux variables numériques fausserait le classement : une variable catégorielle paraîtrait négligeable parce que son influence est répartie. Les contributions sont donc sommées par variable source, ce que l'additivité de SHAP autorise.

L'analyse porte sur un échantillon de 20 000 transactions du bloc de validation.

## 2. Contribution des variables

| # | Variable | Part | Cumul |
| ---: | --- | ---: | ---: |
| 1 | `amount` | **27,67 %** | 27,7 % |
| 2 | `amount_sum_24h` | **13,54 %** | 41,2 % |
| 3 | `merchant_category` | 11,98 % | 53,2 % |
| 4 | `is_night` | 7,13 % | 60,3 % |
| 5 | `seconds_since_previous_txn` | 3,53 % | 63,9 % |
| 6 | `txn_count_7d` | 3,52 % | 67,4 % |
| 7 | `customer_txn_count_hist` | 3,39 % | 70,8 % |
| 8 | `txn_count_24h` | 3,26 % | 74,0 % |
| 9 | `amount_sum_7d` | 3,03 % | 77,1 % |
| 10 | `amount_ratio_vs_mean` | 2,91 % | 80,0 % |
| 11 | `hour_of_day` | 2,84 % | 82,8 % |
| 12 | `customer_age` | 1,98 % | 84,8 % |
| 13 | `amount_zscore_vs_history` | 1,61 % | 86,4 % |

**Par famille :**

| Famille | Part |
| --- | ---: |
| **Vélocité** (fenêtres glissantes) | **27,90 %** |
| Temporelles (heure, jour) | 14,06 % |
| Écart au comportement habituel | 8,03 % |

## 3. Les deux vérifications ouvertes depuis l'étape 3

### 3.1 L'heure écrase-t-elle les autres variables ? — **Non**

Le profilage avait établi un taux de fraude 10 à 30 fois supérieur entre 22 h et 3 h, en partie par artefact du générateur. Le risque identifié était que le modèle apprenne cette règle de génération plutôt qu'un comportement de fraude.

Les variables temporelles pèsent **14,06 %** de la contribution totale. Le signal horaire est donc bien présent — `is_night` occupe le 4ᵉ rang — mais il n'écrase pas les autres. Le modèle ne se réduit pas à un détecteur d'heure nocturne.

La réserve subsiste néanmoins pour la transposition : sur des données réelles, ce signal serait vraisemblablement plus faible, et 14 % de la capacité de discrimination reposent sur une régularité partiellement artificielle.

### 3.2 `customer_txn_count_hist` est-elle un indicateur temporel déguisé ? — **Risque limité**

Cette variable croît mécaniquement avec le temps écoulé depuis le début du corpus, alors que le taux de fraude y décroît (0,579 % puis 0,386 %). Elle pouvait donc n'encoder que la date, information qui ne généraliserait pas.

Elle se situe au **7ᵉ rang sur 35**, pour **3,39 %** de la contribution. Sa présence est réelle mais marginale : même si son apport était intégralement artificiel, il ne remettrait pas en cause la performance d'ensemble.

Un élément rassure a posteriori : l'écart entre validation et test est faible (note [08](08_evaluation_finale.md)). Si le modèle s'était appuyé sur un indicateur temporel trompeur, la dégradation sur la période suivante aurait été bien plus marquée.

## 4. Le résultat inattendu : le z-score n'est que 13ᵉ

C'est le constat le plus intéressant de l'analyse, et il contredit une anticipation formulée à l'étape 3.

`amount_zscore_vs_history` avait été présentée comme la variable centrale du dispositif, et l'analyse univariée le confirmait spectaculairement : **0,001 écart-type** pour une transaction légitime contre **3,897** pour une fraude. Elle arrive pourtant **13ᵉ**, avec **1,61 %** de la contribution.

### L'explication : information réelle, mais redondante

SHAP mesure une contribution **marginale** — ce qu'une variable apporte *en plus* de toutes les autres. Or le modèle dispose simultanément de `amount` (27,67 %), de `customer_amount_mean_hist` et de `customer_amount_std_hist`. Un arbre de décision peut reconstituer l'information du z-score en combinant ces trois entrées par des découpages successifs.

La variable n'est donc pas inutile : son information est **présente dans le modèle**, mais elle y transite par ses ingrédients plutôt que par la variable construite.

### Ce que cela enseigne sur le feature engineering

Deux enseignements méritent d'être formulés, car ils dépassent le cas particulier.

**Le feature engineering explicite apporte moins aux modèles à arbres qu'aux modèles linéaires.** Un arbre approxime naturellement les rapports et les écarts par des découpages successifs. Une régression logistique, elle, ne le peut pas : elle ne voit que des combinaisons linéaires des entrées, et ne peut pas former le rapport `amount / mean` toute seule. C'est cohérent avec sa contre-performance mesurée à l'étape 4 — AUC-PR de 0,5479 contre 0,9805.

**L'analyse univariée et l'analyse SHAP ne répondent pas à la même question.** La première mesure le pouvoir discriminant d'une variable *prise seule* ; la seconde mesure son apport *en présence des autres*. Une variable peut être excellente au premier test et marginale au second sans qu'il y ait contradiction. Confondre les deux conduirait à surestimer l'apport du feature engineering.

## 5. Ce que l'analyse valide malgré tout

La famille **vélocité** est la première contributrice avec **27,90 %**, dont `amount_sum_24h` en deuxième position individuelle (13,54 %).

Ces variables n'existent pas dans la source : elles ont été construites à l'étape 3, et leurs largeurs de fenêtre ont été choisies sur la cadence réellement mesurée au profilage. **Le feature engineering comportemental est donc bien validé — simplement pas par la variable qui avait été annoncée comme centrale.**

C'est aussi la famille la plus transposable : contrairement au signal horaire, elle décrit un mécanisme de fraude réel — la carte compromise exploitée en série avant blocage — et non une régularité du générateur.

## 6. Explication individuelle d'une alerte

SHAP produit, pour chaque transaction, la liste des variables qui l'ont poussée vers la fraude ou vers la légitimité, avec leur contribution chiffrée. C'est sous cette forme qu'un analyste recevrait la justification d'un blocage, et sous cette forme qu'une décision serait défendue devant un régulateur.

L'existence de cette capacité est le point à retenir : le dispositif ne se contente pas de classer, il **motive** chacune de ses décisions.

---

**À retenir pour la soutenance**

**Le modèle n'est pas un détecteur d'heure nocturne.** Les variables temporelles pèsent 14 % — le signal artificiel du générateur est présent sans dominer.

**La variable que j'annonçais comme centrale n'est que 13ᵉ.** Le z-score du montant, spectaculaire en analyse univariée, ne contribue qu'à 1,61 % en SHAP : son information est reconstituée par le modèle à partir du montant et des statistiques d'historique. Un arbre approxime naturellement les rapports, là où une régression logistique ne le peut pas — ce qui explique l'écart considérable entre les deux à l'étape 4.

**Le feature engineering est validé par la vélocité, première famille contributrice à 27,9 %.** C'est aussi la plus transposable, puisqu'elle décrit un mécanisme de fraude réel et non une régularité du corpus simulé.
