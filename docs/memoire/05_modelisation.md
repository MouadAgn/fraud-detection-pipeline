# 05 — Modélisation : protocole, rééquilibrage et comparaison

> **Étape projet** : 4 — Modèle
> **Sections mémoire visées** : Partie 2 — choix de l'algorithme et correction du déséquilibre · Partie 2 — justification du modèle retenu
> **Code** : [`src/fraud/modeling.py`](../../src/fraud/modeling.py) · [`src/fraud/metrics.py`](../../src/fraud/metrics.py) · [`notebooks/04_model_training.py`](../../notebooks/04_model_training.py)

---

## 1. Le protocole compte plus que l'algorithme

Un XGBoost évalué sur un découpage aléatoire produit des chiffres plus flatteurs — et beaucoup moins fiables — qu'une régression logistique évaluée correctement. C'est pourquoi cette note consacre l'essentiel de son propos au protocole, et non au réglage des modèles.

Quatre décisions déterminent si les métriques obtenues signifient quelque chose : le découpage, le rééquilibrage, le traitement des valeurs manquantes, et le choix des métriques elles-mêmes.

## 2. Découpage chronologique à trois blocs

### Pourquoi chronologique

En détection de fraude, l'évaluation doit répondre à : *le modèle entraîné sur le passé détecte-t-il la fraude de demain ?* Un tirage aléatoire entraînerait le modèle sur des transactions postérieures à celles du test — situation impossible en production, qui surestime les performances.

### Pourquoi trois blocs et non deux

| Bloc | Période | Rôle |
| --- | --- | --- |
| **Entraînement** | 01/01/2019 → 31/03/2020 | Ajustement des modèles |
| **Validation** | 01/04/2020 → 21/06/2020 | Comparaison des modèles et choix du seuil |
| **Test** | 21/06/2020 → 31/12/2020 | Évaluation finale, jamais consultée avant la fin |

Le bloc de validation n'est pas un raffinement académique.

Le seuil de décision est un **paramètre**, au même titre qu'un hyperparamètre. Le choisir directement sur le jeu de test reviendrait à l'optimiser sur les données censées mesurer la performance : le taux de faux positifs annoncé serait alors optimiste, et c'est précisément le chiffre central du mémoire. La sélection du modèle souffrirait du même biais.

La frontière validation/test coïncide avec la coupure native du corpus Kaggle (21/06/2020), ce qui préserve l'intégrité du jeu de test tel qu'il a été distribué. Le bloc de validation est prélevé sur la période d'entraînement.

Trois contrôles bloquants vérifient le découpage : absence de chevauchement entre blocs consécutifs, conservation du volume total, et présence de fraudes dans chacun des trois blocs — un bloc sans positif serait inévaluable.

## 3. Le rééquilibrage n'est pas un réglage neutre

### Pourquoi ne pas rééquilibrer à 50/50

Le réflexe, face à 0,5 % de positifs, est de rééquilibrer complètement. C'est **contraire à la problématique du mémoire**.

Générer autant de fraudes synthétiques que de transactions légitimes apprend au modèle que la fraude est un événement sur deux. Il devient alors hypersensible : le rappel monte, mais chaque signal faible déclenche une alerte, et les faux positifs explosent. On optimise exactement ce que le mémoire cherche à réduire.

Plusieurs taux de rééquilibrage sont donc comparés — 2 %, 5 %, 20 % de minorité — plutôt qu'un réglage adopté par convention.

### SMOTE ou pondération ?

SMOTE crée des exemples synthétiques par interpolation entre fraudes voisines. `scale_pos_weight`, propre aux modèles à boosting, atteint un objectif comparable en pondérant les erreurs sur la classe minoritaire, **sans fabriquer de données**.

La distinction mérite d'être posée devant un jury : SMOTE suppose que l'interpolation entre deux fraudes produit une fraude plausible. C'est une hypothèse forte, contestable sur des variables comportementales — la moyenne de deux scénarios de fraude n'est pas nécessairement un scénario de fraude. Les deux approches sont donc comparées empiriquement plutôt que départagées par principe.

### SMOTE dans le pipeline, jamais à la main

SMOTE est intégré à un pipeline `imblearn`, ce qui garantit **structurellement** qu'il ne s'applique qu'à l'entraînement et jamais lors d'une prédiction.

Appliqué manuellement avant le découpage, ou par inadvertance à l'ensemble des données, il contamine la validation : le modèle est alors évalué sur des fraudes qu'il a en partie contribué à générer. C'est l'erreur la plus fréquente sur les jeux déséquilibrés, et elle est **invisible dans les métriques** — elle les améliore.

## 4. Traitement des valeurs manquantes

XGBoost et LightGBM traitent nativement les valeurs manquantes. La régression logistique, la forêt aléatoire et SMOTE en sont incapables.

Pour que la comparaison porte sur les **algorithmes** et non sur leur tolérance aux trous, tous reçoivent la même préparation : imputation par la médiane, **assortie d'une colonne indicatrice**.

L'indicateur est essentiel et concilie ce choix avec le principe posé en couche Gold (note [04](04_features_gold.md), § 6). Imputer seul effacerait l'information « ce porteur n'avait pas d'historique » ; l'indicateur la conserve sous forme explicite. On ne déguise donc pas l'absence, **on l'encode**.

## 5. Encodage cyclique de l'heure

`hour_of_day` est un entier de 0 à 23. Pour un modèle linéaire, 23 h et 0 h sont distantes de 23 unités alors qu'elles sont voisines dans la réalité — ce qui rend la plage nocturne, pourtant le signal le plus fort du corpus, littéralement inapprenable.

L'heure est donc projetée sur un cercle (sinus et cosinus). Les modèles à arbres n'en ont pas besoin : ils découpent l'axe par seuils et retrouveraient la plage nocturne d'eux-mêmes.

Mais la comparaison doit être **équitable**. Sans cet encodage, la régression logistique de référence serait handicapée par la représentation des données plutôt que par ses capacités propres, et sa contre-performance ne prouverait rien sur l'intérêt du boosting.

## 6. Le choix des métriques

### Pourquoi l'AUC-ROC ne suffit pas

À 0,4 % de positifs, l'AUC-ROC est dominée par la capacité du modèle à reconnaître la classe majoritaire. Elle reste élevée même quand la précision est catastrophique : un modèle affichant 0,98 d'AUC-ROC peut produire vingt fausses alertes par fraude détectée.

L'**AUC-PR** (précision moyenne) ne considère que la classe positive et s'effondre dès que les faux positifs augmentent. C'est elle qui sert de **critère de sélection**, parce qu'elle mesure ce que le mémoire cherche à optimiser.

### Deux indicateurs métier

| Métrique | Définition | Pourquoi |
| --- | --- | --- |
| `false_positive_rate` | FP / (FP + VN) | Part des transactions **légitimes** bloquées à tort. Le dénominateur est le trafic légitime, non le total : rapportée au total, la valeur serait diluée par la classe majoritaire et paraîtrait systématiquement meilleure qu'elle ne l'est. |
| `alerts_per_1000` | Alertes pour mille transactions | Traduit le taux de faux positifs en **charge de travail réelle** pour une équipe d'analystes. Aucun pourcentage ne rend cette réalité intuitive. |

### L'arbitrage que ces métriques révèlent

Sur un jeu de contrôle synthétique reproduisant le déséquilibre réel, le dispositif de mesure produit l'arbitrage suivant :

| Rappel visé | Faux positifs |
| ---: | ---: |
| 60 % | 729 |
| 70 % | 1 885 |
| 80 % | 4 836 |
| 90 % | 13 308 |

Passer de 60 % à 90 % de fraudes détectées multiplie les fausses alertes par **dix-huit**. C'est l'arbitrage central du mémoire, et il se joue au niveau du **seuil de décision** — pas du modèle. Le sujet est traité à l'étape 5.

## 7. Les modèles comparés

| Modèle | Rôle dans la comparaison |
| --- | --- |
| **Régression logistique** | Référence. Si un modèle complexe ne la bat pas nettement, sa complexité n'est pas justifiée. |
| **Forêt aléatoire** | Non linéaire, sans boosting. Isole l'apport propre du boosting. |
| **LightGBM** | Boosting concurrent, plus rapide sur gros volumes. |
| **XGBoost** | Le modèle annoncé dans la méthodologie. Il doit être **choisi**, pas subi. |

Tous reçoivent exactement les mêmes données, le même prétraitement et le même rééquilibrage. Seul l'algorithme change — c'est la condition pour que la comparaison signifie quelque chose.

L'inclusion d'une régression logistique n'est pas une formalité. Elle protège d'un travers courant : adopter le modèle à la mode sans démontrer qu'il apporte quoi que ce soit.

## 8. Suivi et registre

Chaque entraînement est journalisé dans **MLflow** : paramètres, métriques de validation, durée d'ajustement et modèle sérialisé. Le modèle retenu est enregistré dans le **Model Registry d'Unity Catalog**, avec sa signature d'entrée.

L'enregistrement automatique de MLflow est explicitement désactivé : il capturerait chaque appel scikit-learn, y compris les étapes internes du pipeline. On journalise ce qui a un sens pour le mémoire plutôt que tout ce qui est techniquement capturable.

## 9. Tests unitaires du module de métriques

`src/fraud/metrics.py` est volontairement dépourvu de dépendance à Spark, ce qui permet de l'exécuter et de le tester hors de tout environnement Databricks.

Ce choix répond à un risque précis. Une définition fausse du taux de faux positifs — un dénominateur incorrect, par exemple — **ne provoque aucune exception**, ne se voit pas à la lecture d'un tableau de résultats, et invaliderait silencieusement toutes les conclusions du mémoire.

La stratégie retenue est la **vérification croisée** : chaque métrique est comparée à un calcul indépendant réalisé directement depuis la matrice de confusion de scikit-learn. On ne teste pas que le code s'exécute, on teste qu'il calcule la bonne chose.

Dix-sept tests couvrent : la concordance de chaque compte et de chaque taux avec la matrice de confusion à cinq seuils différents, le dénominateur du taux de faux positifs, la reconstruction des faux positifs dans le balayage de seuils, la monotonie de l'arbitrage rappel/faux positifs, et deux cas limites — classifieur parfait et seuil supérieur à tous les scores.

```
py -3.13 -m pip install -r requirements-dev.txt
py -3.13 -m pytest tests/ -v
```

## 10. Résultats

> ⏳ *À compléter après exécution du notebook : tableau comparatif des quatre modèles, comparatif des stratégies de rééquilibrage, modèle retenu et gain sur la référence logistique.*

---

**À retenir pour la soutenance**
**Trois blocs et non deux** : le seuil de décision est un paramètre, le choisir sur le test rendrait optimiste le chiffre même que le mémoire prétend mesurer. **L'AUC-PR et non l'AUC-ROC** : à 0,4 % de positifs, l'AUC-ROC reste élevée alors que la précision s'effondre. **Pas de rééquilibrage à 50/50** : apprendre au modèle que la fraude est un événement sur deux le rend hypersensible et multiplie les fausses alertes — l'inverse exact de l'objectif poursuivi.
