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

Toutes les valeurs sont mesurées sur le **bloc de validation**, au seuil arbitraire de 0,5. Le bloc de test n'a pas été consulté.

### 10.1 Comparaison des algorithmes

À rééquilibrage constant (SMOTE 5 %).

| Modèle | AUC-PR | AUC-ROC | Précision | Rappel | F1 | TFP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| LightGBM | **0,9834** | 0,9995 | 0,9809 | 0,9295 | 0,9545 | 0,0001 |
| XGBoost | 0,9761 | 0,9994 | 0,9795 | 0,9037 | 0,9401 | 0,0001 |
| Forêt aléatoire | 0,9060 | 0,9936 | 0,9517 | 0,7455 | 0,8361 | 0,0002 |
| Régression logistique | 0,5479 | 0,9791 | 0,6206 | 0,6793 | 0,6486 | 0,0024 |

**La hiérarchie est nette** : boosting ≫ forêt ≫ linéaire. Le gain du meilleur modèle sur la référence logistique atteint **+79,5 % d'AUC-PR**, ce qui justifie le recours à un modèle complexe. La forêt aléatoire, non linéaire mais sans boosting, reste en retrait de huit points : c'est bien le boosting, et non la seule non-linéarité, qui fait la différence.

### 10.2 La régression logistique démontre pourquoi l'AUC-ROC est trompeuse

C'est le résultat le plus instructif du tableau, et il illustre en conditions réelles l'argument développé au § 6.

| | Régression logistique |
| --- | ---: |
| AUC-ROC | **0,9791** — laisse croire à un excellent modèle |
| AUC-PR | **0,5479** — révèle un modèle médiocre |
| Taux de faux positifs | **0,0024**, soit **24 fois** celui des modèles à boosting |

Les deux métriques portent sur les **mêmes prédictions**. Un projet qui se serait contenté de l'AUC-ROC aurait conclu que la régression logistique convenait — et aurait déployé un modèle produisant vingt-quatre fois plus de fausses alertes.

### 10.3 Plan complet : deux algorithmes × quatre stratégies de rééquilibrage

AUC-PR mesurée en validation.

| Stratégie | XGBoost | LightGBM | Écart |
| --- | ---: | ---: | ---: |
| Aucune | **0,9805** | 0,9767 | −0,0038 |
| `scale_pos_weight` | 0,9788 | 0,9776 | −0,0012 |
| SMOTE 2 % | 0,9761 | **0,5658** | **−0,4103** |
| SMOTE 20 % | 0,9738 | **0,9842** | +0,0104 |

**Le rééquilibrage n'améliore rien chez XGBoost — il dégrade.** La décroissance est monotone : aucun rééquilibrage (0,9805) > `scale_pos_weight` (0,9788) > SMOTE 2 % (0,9761) > SMOTE 20 % (0,9738).

Le résultat mérite d'être formulé précisément, car il est plus profond qu'il n'y paraît : l'AUC-PR est **indépendante du seuil**. Sa dégradation signifie que le rééquilibrage n'améliore pas la capacité du modèle à ordonner les transactions du plus au moins suspect. Il **déplace simplement le point de fonctionnement** — ce que le réglage du seuil de décision accomplit sans fabriquer de données synthétiques ni distordre la distribution d'apprentissage.

SMOTE, annoncé dans la méthodologie initiale, a donc été testé sur quatre configurations puis **écarté sur preuve**. C'est un résultat, non un renoncement : l'appliquer par réflexe aurait dégradé la performance sans que rien ne le signale.

### 10.4 L'arbitrage rendu visible

La ligne `scale_pos_weight` de XGBoost isole parfaitement le compromis au cœur du mémoire.

| | Sans rééquilibrage | Avec `scale_pos_weight` |
| --- | ---: | ---: |
| Rappel | 0,9037 | **0,9751** |
| Précision | 0,9813 | **0,7735** |
| Taux de faux positifs | 0,0001 | **0,0017** — **× 17** |

Sept points de fraudes détectées en plus, au prix de **dix-sept fois plus de fausses alertes**.

### 10.5 Le plan à un facteur à la fois avait un angle mort

Les deux premières comparaisons procédaient à un facteur constant : les algorithmes à rééquilibrage fixe, puis les rééquilibrages à algorithme fixe. Les tendances observées — LightGBM devant XGBoost, absence de rééquilibrage devant SMOTE — désignaient *LightGBM sans rééquilibrage* comme combinaison la plus prometteuse. Elle ne figurait dans aucun des deux tableaux.

Le plan a donc été complété. **L'hypothèse s'est révélée fausse** : LightGBM sans rééquilibrage arrive cinquième (0,9767), derrière XGBoost sans rééquilibrage (0,9805).

L'épisode a valeur méthodologique : un plan à un facteur à la fois est lisible mais incomplet, et rien ne garantit que la combinaison retenue soit la meilleure si on ne balaie pas la grille.

### 10.6 Stabilité : le critère que le classement par score ignore

Le plan complet fait apparaître une anomalie — `lgbm_smote_02` voit son AUC-PR s'effondrer à 0,57 alors que son AUC-ROC reste à 0,978. Une seule modification du taux de rééquilibrage suffit à faire basculer le modèle.

| Algorithme | Min | Max | Amplitude | Écart-type |
| --- | ---: | ---: | ---: | ---: |
| **XGBoost** | 0,9738 | 0,9805 | **0,0068** | 0,0026 |
| LightGBM | 0,5658 | 0,9842 | **0,4184** | 0,1792 |

**L'amplitude de LightGBM est soixante et une fois supérieure à celle de XGBoost.**

Un second constat s'y ajoute, découvert en réexécutant la grille : la configuration `lgbm_smote_02` a produit **0,6026 au premier passage et 0,5658 au second**, à graine, code et données identiques. LightGBM n'est pas déterministe en multithread par défaut — la construction des histogrammes dépend de l'ordonnancement des fils d'exécution, et `deterministic=True` est nécessaire pour l'y contraindre.

Ce n'est plus seulement de l'instabilité entre configurations, c'est un défaut de **reproductibilité**, rédhibitoire pour un travail dont chaque chiffre doit être rejouable. XGBoost a restitué exactement 0,9805 aux deux passages.

### 10.7 Modèle retenu

Retenir mécaniquement le meilleur AUC-PR conduirait à `lgbm_smote_20` (0,9842). Ce serait une erreur : ce point est le maximum d'une famille dont l'amplitude dépasse 0,41 et dont les résultats ne se reproduisent pas à l'identique.

**Modèle retenu : XGBoost sans rééquilibrage.**

| Critère | Valeur |
| --- | --- |
| AUC-PR (validation) | 0,9805 — à **0,37 %** du meilleur score absolu |
| AUC-ROC (validation) | 0,9996 |
| Précision | **0,9813 — la meilleure des douze combinaisons** |
| Taux de faux positifs | **0,0001 — le plus bas du plan** |
| Amplitude sur les configurations | 0,0068 |
| Reproductibilité | exacte entre deux exécutions |

Le renoncement à 0,37 % de performance achète la robustesse, la reproductibilité, la meilleure précision et le plus bas taux de faux positifs du plan. Il supprime en outre la génération de données synthétiques : moins d'hypothèses, entraînement plus rapide, comportement plus simple à expliquer devant un régulateur.

### 10.8 Réserve sur le niveau absolu des performances

Une AUC-PR de 0,98 est **hors de portée d'un système de détection de fraude réel**, qui se situe plutôt entre 0,4 et 0,7. Ces valeurs constituent une **borne supérieure optimiste**, imputable à la régularité des règles de génération du corpus simulé — trois artefacts en ont été identifiés et mesurés dès le profilage (note [01](01_profilage_initial.md)) : concentration horaire trop nette, plafonnement des montants frauduleux, et un z-score de montant très séparateur.

Ces métriques servent donc à **comparer les modèles entre eux**, ce pour quoi elles sont valides, et non à annoncer une performance transposable en production.

---

**À retenir pour la soutenance**

**L'AUC-ROC est trompeuse, et vos propres chiffres le prouvent.** La régression logistique affiche 0,9791 d'AUC-ROC et 0,5479 d'AUC-PR sur les mêmes prédictions, avec vingt-quatre fois plus de fausses alertes que le modèle retenu. Un projet qui s'en serait tenu à l'AUC-ROC aurait conclu qu'elle convenait.

**SMOTE a été testé puis écarté sur preuve.** Sur quatre configurations, le rééquilibrage dégrade l'AUC-PR de façon monotone. Comme cette métrique est indépendante du seuil, sa dégradation démontre que le rééquilibrage n'améliore pas la capacité du modèle à ordonner les transactions : il déplace le point de fonctionnement, ce que le réglage du seuil accomplit sans fabriquer de données synthétiques.

**XGBoost est retenu sur sa robustesse, pas sur son score.** LightGBM le devance de 0,37 %, mais son amplitude sur les quatre configurations atteint 0,4184 contre 0,0068 — soit soixante et une fois plus — et il ne reproduit pas ses résultats à graine constante. Un modèle qui ne tient que sur un réglage précis est un risque en production, où les données dérivent sans que personne ne réajuste.
