# 08 — Évaluation expérimentale finale

> **Étape projet** : 5 — Scoring
> **Section mémoire visée** : Partie 3 — évaluation expérimentale (AUC, précision, rappel, taux de faux positifs)
> **Code** : [`notebooks/05_scoring_eval.py`](../../notebooks/05_scoring_eval.py), section 7

---

## 1. Le protocole, et pourquoi il donne sa valeur aux chiffres

Le bloc de test couvre le **21 juin au 31 décembre 2020**, soit 555 719 transactions dont 2 145 fraudes (0,386 %). Il n'a été consulté qu'**une seule fois**, après fixation définitive :

- du modèle — XGBoost sans rééquilibrage, retenu sur le bloc de validation (note [05](05_modelisation.md)) ;
- du seuil de décision — 0,0380, retenu sur le bloc de validation par minimisation du coût métier (note [06](06_seuil_faux_positifs.md)).

Aucun réglage n'a été effectué après consultation du test. C'est cette discipline, et elle seule, qui autorise à présenter les résultats ci-dessous comme une estimation honnête de la performance sur une période inconnue.

Le test porte en outre sur **six mois postérieurs** à toute donnée d'entraînement, avec un taux de fraude en baisse (0,579 % sur l'entraînement contre 0,386 % sur le test) : c'est bien la capacité à tenir sur une population qui a dérivé qui est mesurée.

## 2. Généralisation : validation contre test

| Métrique | Validation | **Test** | Écart |
| --- | ---: | ---: | ---: |
| AUC-PR | 0,98053 | **0,96710** | −0,01342 |
| AUC-ROC | 0,99960 | **0,99911** | −0,00049 |
| Précision | 0,81497 | **0,77108** | −0,04389 |
| Rappel | 0,97334 | **0,95478** | −0,01857 |
| F1 | 0,88715 | **0,85316** | −0,03399 |
| **Taux de faux positifs** | 0,00130 | **0,00110** | −0,00020 |
| Alertes pour 1 000 transactions | 6,98 | **4,78** | −2,20 |

**La dégradation est faible.** L'AUC-PR ne perd que 1,3 point sur une période de six mois jamais vue, et le rappel 1,9 point. Un modèle qui aurait appris des régularités non généralisables — un indicateur temporel déguisé, par exemple — se serait effondré bien davantage.

**Le taux de faux positifs s'améliore légèrement** sur le test (0,110 % contre 0,130 %), de même que le nombre d'alertes pour mille transactions. Ce n'est pas paradoxal : le taux de fraude étant plus faible sur cette période, le modèle alerte moins souvent en valeur relative. La précision baisse en revanche de 4,4 points, ce qui est la conséquence attendue d'une classe positive plus rare — à qualité de détection égale, une base moins fraudée produit mécaniquement une proportion de fausses alertes plus élevée **parmi les alertes**.

C'est un point à savoir expliquer : précision et taux de faux positifs ne varient pas dans le même sens parce qu'ils n'ont pas le même dénominateur.

## 3. Matrice de confusion sur le bloc de test

| | Prédit légitime | Prédit fraude |
| --- | ---: | ---: |
| **Réellement légitime** | 552 966 | **608** |
| **Réellement fraude** | **97** | 2 048 |

### Lecture opérationnelle

| | |
| --- | ---: |
| Fraudes détectées | **2 048 sur 2 145** — 95,5 % |
| Fraudes manquées | 97 |
| Fausses alertes | **608** sur 553 574 transactions légitimes |
| **Taux de faux positifs** | **0,110 %** |
| Alertes à traiter | 2 656 |
| Précision des alertes | 77,1 % |
| **Alertes examinées par fraude trouvée** | **1,3** |

## 4. Le résultat, formulé pour le métier

> Sur les 555 719 transactions du dernier semestre 2020, le dispositif détecte **2 048 fraudes sur 2 145 (95,5 %)** en générant **608 fausses alertes**, soit **une transaction légitime bloquée pour mille** environ.
>
> Un analyste examinant les alertes trouve une fraude **dans plus de trois cas sur quatre**, et n'a besoin d'examiner que **1,3 alerte** pour en identifier une.

Ce dernier chiffre est le plus parlant en contexte opérationnel. Un dispositif produisant vingt alertes par fraude serait rejeté par les équipes qui doivent les traiter, quelle que soit sa performance statistique. C'est la traduction concrète de la problématique : **détecter davantage sans noyer les analystes**.

## 5. Confrontation aux objectifs du projet

| Objectif annoncé | Résultat |
| --- | --- |
| Pipeline produisant des tables analytiques propres | Bronze → Silver → Gold, contrôlées et documentées |
| Modèle scorant chaque transaction | 1 852 394 transactions scorées, table `gold_scored_transactions` |
| Métriques réelles — AUC, précision, rappel, taux de faux positifs | AUC-PR 0,967 · précision 0,771 · rappel 0,955 · TFP 0,110 % |
| Minimisation des faux positifs | 608 sur 553 574 transactions légitimes |

## 6. Réserves — ce que ces chiffres ne disent pas

Trois limites doivent accompagner toute citation de ces résultats. Les énoncer soi-même vaut mieux que de les subir en soutenance.

### 6.1 Le corpus est simulé, ces valeurs sont une borne supérieure

Une AUC-PR de 0,967 est **hors de portée d'un système de détection de fraude réel**, qui se situe plutôt entre 0,4 et 0,7. L'écart s'explique par la régularité des règles de génération du jeu de données, dont trois artefacts ont été identifiés et mesurés dès le profilage :

- la concentration horaire de la fraude, trop nette pour être réaliste ;
- le plafonnement des montants frauduleux à 1 376 $, alors que les transactions légitimes atteignent 28 949 $ ;
- l'absence totale de pouvoir discriminant de la distance géographique, qui serait vraisemblablement l'une des variables les plus fortes sur des données réelles.

Ces métriques servent à **comparer les configurations entre elles** — usage pour lequel elles sont valides — et non à annoncer une performance transposable en production.

### 6.2 Le coût d'une fausse alerte est une hypothèse

Le seuil retenu dépend de l'estimation à 5 $ du coût d'un faux positif. L'analyse de sensibilité (note [06](06_seuil_faux_positifs.md), § 5) montre que porter cette valeur à 25 $ divise les fausses alertes par près de quatre, au prix de 1,8 point de rappel. Le point de fonctionnement présenté n'est donc pas *le* point optimal dans l'absolu, mais celui qui correspond à une hypothèse économique explicite.

### 6.3 Aucun retour métier n'a pu être intégré

Le corpus ne contient aucune information sur les alertes réellement traitées par un analyste, ni sur le coût effectif d'un blocage abusif. Toute la modélisation du coût repose donc sur des hypothèses, non sur des mesures.

## 7. Table scorée

L'ensemble du corpus — 1 852 394 transactions — est scoré et écrit dans `gold_scored_transactions`, avec pour chaque ligne le score de fraude, la décision, et le seuil qui l'a produite. Cette table alimente le tableau de bord de l'étape 7.

Le seuil et l'URI du modèle sont enregistrés comme **propriétés de la table** : une décision `is_flagged` n'est interprétable que si l'on sait à quel seuil et par quel modèle elle a été prise.

---

**À retenir pour la soutenance**

**Le chiffre central** : 2 048 fraudes détectées sur 2 145 (95,5 %) pour 608 fausses alertes sur 553 574 transactions légitimes, soit un taux de faux positifs de **0,110 %** et **1,3 alerte examinée par fraude trouvée**.

**Ces chiffres sont honnêtes parce que le protocole l'était** : le bloc de test n'a été consulté qu'une fois, après fixation du modèle et du seuil sur la validation. Aucun réglage n'a suivi.

**La généralisation tient** : 1,3 point d'AUC-PR perdu sur six mois jamais vus, alors que le taux de fraude a baissé d'un tiers sur la période. Un modèle appuyé sur des régularités non généralisables se serait effondré davantage.

**Mais le corpus est simulé.** Une AUC-PR de 0,967 est hors de portée d'un système réel. Ces valeurs comparent des configurations entre elles ; elles ne promettent pas une performance en production.
