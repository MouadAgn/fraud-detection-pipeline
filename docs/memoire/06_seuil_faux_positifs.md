# 06 — Le seuil de décision et l'arbitrage des faux positifs

> **Étape projet** : 5 — Scoring
> **Section mémoire visée** : Partie 3 — arbitrage précision/rappel, minimisation des faux positifs
> **Code** : [`notebooks/05_scoring_eval.py`](../../notebooks/05_scoring_eval.py) · [`src/fraud/metrics.py`](../../src/fraud/metrics.py)

---

## 1. Ce n'est pas le modèle qui produit les faux positifs, c'est le seuil

Toutes les métriques des étapes précédentes ont été calculées au seuil de 0,5 — une valeur arbitraire, héritée du fait qu'un classifieur binaire renvoie une probabilité entre 0 et 1.

Or le même modèle, selon l'endroit où l'on coupe, peut bloquer une transaction légitime sur mille ou une sur dix. Le modèle produit un **ordonnancement** ; le seuil produit une **décision**. Confondre les deux revient à laisser un réglage par défaut décider du KPI central du mémoire.

## 2. Pourquoi ne pas maximiser le F1

Le réflexe serait de retenir le seuil maximisant le F1. Ce serait poser, sans le dire, qu'**un faux positif et un faux négatif se valent**.

C'est faux dans tous les métiers, et particulièrement en détection de fraude :

| Erreur | Nature du coût | Mesurable ? |
| --- | --- | --- |
| **Faux négatif** — fraude non détectée | Le montant de la transaction | **Oui** — il figure dans les données |
| **Faux positif** — transaction légitime bloquée | Temps d'analyste, contact client, friction commerciale | **Non** — il doit être posé en hypothèse |

L'asymétrie est considérable sur ce corpus : une fraude coûte en moyenne **531 $**, tandis qu'une fausse alerte est estimée à **5 $**. Le rapport est de l'ordre de **100 pour 1**. Un critère qui les traite à égalité ne peut pas produire une décision économiquement sensée.

## 3. Le seuil retenu minimise le coût métier total

Pour chaque seuil candidat, deux coûts s'opposent :

- **le montant des fraudes non détectées** — il décroît quand le seuil s'abaisse ;
- **le coût des fausses alertes** — il croît quand le seuil s'abaisse.

Leur somme passe par un minimum. La courbe inclut aussi le point « aucune alerte », qui constitue la référence *sans dispositif* : sans elle, on ne saurait pas si le système fait gagner ou perdre de l'argent.

### Résultats sur le bloc de validation

Hypothèse : une fausse alerte coûte 5 $.

| | Sans dispositif | Au seuil optimal |
| --- | ---: | ---: |
| Seuil | — | **0,0380** |
| Alertes émises | 0 | 1 389 |
| Fraudes détectées | 0 | 1 132 (**97,3 %**) |
| Fausses alertes | 0 | 257 |
| Fraude subie | **619 630 $** | 2 401 $ |
| Coût des alertes | 0 $ | 1 285 $ |
| **Coût total** | **619 630 $** | **3 686 $** |

**Économie : 615 944 $, soit 99,4 %.**

### Le seuil optimal est très éloigné de 0,5

**0,0380**, contre 0,5 par défaut. Le modèle alerte donc dès qu'il attribue à une transaction 3,8 % de probabilité d'être frauduleuse.

C'est économiquement rationnel : une fraude coûtant environ cent fois une fausse alerte, il vaut la peine d'accepter une centaine de vérifications inutiles pour éviter une fraude supplémentaire. Conserver le seuil par défaut aurait laissé passer des fraudes qu'un simple réglage permettait d'attraper.

## 4. Le point le plus important : « minimiser les faux positifs » ne peut pas être un objectif absolu

C'est la nuance qui doit être exposée en soutenance, car elle reformule la problématique du mémoire.

Minimiser les faux positifs *sans autre contrainte* a une solution triviale et absurde : porter le seuil à 1, n'émettre aucune alerte, obtenir **zéro faux positif** — et subir l'intégralité des 619 630 $ de fraude. Le taux de faux positifs serait parfait et le dispositif inutile.

L'optimum économique va d'ailleurs dans le sens opposé à l'intuition : il accepte **plus** de fausses alertes que le seuil par défaut, parce que chacune coûte cent fois moins qu'une fraude manquée.

La formulation rigoureuse de la problématique est donc :

> **À niveau de détection donné, minimiser les faux positifs.**

Ce qui revient à déplacer la frontière précision/rappel elle-même, et non à se déplacer le long d'une frontière fixée. C'est le travail accompli en amont — feature engineering comportemental (étape 3) et sélection du modèle (étape 4) — tandis que le seuil ne fait que choisir un point sur cette frontière.

Le résultat obtenu se lit dans ce cadre : **95,5 % des fraudes détectées pour 0,110 % de transactions légitimes bloquées** sur le bloc de test. C'est la frontière qui a été améliorée, pas le seuil qui a été forcé.

## 5. Analyse de sensibilité au coût d'une fausse alerte

Le coût d'un faux positif est une **hypothèse**, pas une mesure. Annoncer un seuil unique masquerait que la conclusion en dépend entièrement.

| Coût d'une fausse alerte | Seuil | Rappel | Précision | Fausses alertes | Taux de faux positifs | Coût total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 $ | 0,0302 | 97,7 % | 79,3 % | 297 | 0,00150 | 2 824 $ |
| **5 $** | **0,0380** | **97,3 %** | **81,5 %** | **257** | **0,00130** | **3 686 $** |
| 10 $ | 0,0380 | 97,3 % | 81,5 % | 257 | 0,00130 | 4 971 $ |
| 25 $ | 0,1758 | 95,9 % | 94,1 % | 70 | 0,00035 | 7 426 $ |

Trois lectures :

1. **La relation attendue est vérifiée.** Plus une fausse alerte coûte cher, plus le seuil devient sélectif : de 0,0302 à 0,1758. Le taux de faux positifs est divisé par plus de quatre, au prix de 1,8 point de rappel.
2. **Le seuil est stable entre 5 $ et 10 $.** Doubler l'hypothèse ne change pas la décision, ce qui indique un optimum peu sensible dans cette plage — une information rassurante, puisque l'hypothèse n'est pas connue avec précision.
3. **La décision appartient au métier.** Ce n'est pas au data scientist de fixer ce que coûte un client dérangé à tort. Le rôle du dispositif est de montrer comment le point de fonctionnement se déplace selon cette valeur, et de laisser l'arbitrage à ceux dont c'est la responsabilité.

## 6. Le seuil est choisi sur la validation, jamais sur le test

Le seuil est un **paramètre**, au même titre qu'un hyperparamètre. L'ajuster sur le jeu de test reviendrait à l'optimiser sur les données censées mesurer la performance : le taux de faux positifs annoncé serait alors optimiste, et c'est précisément le chiffre central du mémoire.

Le bloc de test n'a été utilisé qu'une seule fois, après fixation définitive du modèle et du seuil (note [08](08_evaluation_finale.md)).

## 7. Traçabilité de la décision

Le seuil retenu est enregistré comme **propriété de la table scorée**, aux côtés de l'hypothèse de coût et de l'URI du modèle :

```sql
ALTER TABLE gold_scored_transactions SET TBLPROPERTIES (
    'fraud.decision_threshold' = '0.0380...',
    'fraud.false_alert_cost'   = '5.0',
    'fraud.model_uri'          = 'models:/...@champion'
)
```

La colonne `is_flagged` n'est interprétable que si l'on sait à quel seuil elle a été calculée, et par quel modèle. Cette information doit voyager avec les données, faute de quoi la table devient inauditable dès que le seuil évolue.

## 8. Une correction issue des tests

La courbe de coût raisonnait initialement par sommes cumulées sur les scores triés, ce qui décrit « alerter sur les *k* transactions les mieux classées ». Or la règle réellement appliquée est « alerter si score ≥ seuil », qui déclenche **aussi sur les ex æquo situés au-delà de *k*** — une coupure au milieu d'un groupe de scores identiques décrivait donc un point de fonctionnement inatteignable.

Le défaut a été détecté par un test de vérification croisée, non à la lecture du code. Seules les coupures séparant deux scores distincts sont désormais conservées, ce qui rend chaque ligne de la courbe exactement reproductible par application du seuil correspondant.

Sans ce test, l'erreur n'aurait levé aucune exception et aurait faussé le nombre de fausses alertes annoncé.

---

**À retenir pour la soutenance**

**Le seuil, et non le modèle, décide du taux de faux positifs.** Le retenir à 0,5 par défaut revient à laisser un réglage arbitraire fixer le KPI central du mémoire.

**Le F1 est un mauvais critère ici**, car il postule qu'un faux positif et un faux négatif se valent. Sur ce corpus, une fraude coûte cent fois une fausse alerte : le seuil optimal en tient compte, et se situe à 0,038 et non à 0,5.

**« Minimiser les faux positifs » ne peut pas être un objectif absolu** — la solution triviale serait de n'alerter jamais. La formulation rigoureuse est : *à niveau de détection donné, minimiser les faux positifs*, ce qui revient à déplacer la frontière précision/rappel. C'est le feature engineering et le choix du modèle qui la déplacent ; le seuil ne fait qu'y choisir un point.
