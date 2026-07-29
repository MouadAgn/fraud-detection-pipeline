# 10 — Tableau de bord et suivi du KPI

> **Étape projet** : 7 — Dashboard
> **Section mémoire visée** : Partie 3 — analyse du taux de faux positifs
> **Code** : [`dashboards/queries.sql`](../../dashboards/queries.sql) · [`dashboards/README.md`](../../dashboards/README.md)

---

## 1. À quoi sert vraiment un tableau de bord ici

Un tableau de bord n'est pas une vitrine. Sa fonction est de **rendre une dérive visible avant qu'elle ne coûte cher**.

Les métriques de l'étape 5 sont un instantané : elles décrivent la performance sur une période close. En exploitation, la question n'est plus « le modèle est-il bon ? » mais « **l'est-il encore ?** ». Un dispositif de fraude se dégrade continûment — les fraudeurs s'adaptent, la clientèle évolue, les usages changent. La dégradation est lente et n'émet aucun signal d'erreur.

C'est pourquoi la pièce maîtresse du tableau de bord n'est pas la valeur du taux de faux positifs, mais **sa courbe**.

## 2. Ce que le tableau de bord affiche

| Visualisation | Question à laquelle elle répond |
| --- | --- |
| **Indicateurs clés** | Combien de fraudes attrapées, combien de clients dérangés à tort ? |
| **Évolution hebdomadaire du taux de faux positifs** | Le dispositif se dégrade-t-il ? |
| **File d'alertes** | Que doit examiner un analyste ce matin, et pourquoi ? |
| **Concentration des faux positifs** | Où agir pour les réduire ? |
| **Impact financier** | Combien le dispositif rapporte-t-il ? |
| **Distribution des scores** | Où le seuil vient-il couper ? |
| **Comparaison des périodes** | Ces chiffres sont-ils honnêtes ? |

## 3. Le choix méthodologique qui conditionne tout : `WHERE split = 'test'`

Toutes les requêtes filtrent sur le bloc de test.

**Le modèle a vu les transactions d'entraînement.** Les métriques calculées dessus mesurent sa capacité à mémoriser, non à détecter : elles sont optimistes par construction.

Un tableau de bord agrégeant les trois périodes afficherait donc un taux de faux positifs flatteur et **faux** — sans produire le moindre message d'erreur. C'est le type d'erreur le plus dangereux d'un tableau de bord : il ne se trompe pas, il **présente autre chose que ce qu'on croit lire**, et personne ne le remarque parce que le chiffre paraît crédible.

Cette contrainte a imposé de reprendre la couche Gold : la table scorée ne conservait pas la période d'appartenance de chaque transaction. Une colonne `split` y a été ajoutée. Le tableau de bord a ainsi révélé une insuffisance du modèle de données — c'est une situation fréquente, et c'est un bon exemple de la façon dont la restitution rétroagit sur l'amont.

Une requête de contrôle (n° 7) est conservée dans le tableau de bord et affiche les trois périodes côte à côte, avec pour chacune son interprétation. Le choix du filtre devient ainsi **visible par le lecteur** au lieu d'être un paramètre caché dans le code.

## 4. Pourquoi la courbe compte plus que la moyenne

Un taux moyen de 0,110 % ne dit rien de sa stabilité.

Une valeur qui passerait de 0,11 % à 0,40 % en trois semaines signalerait que la population a changé et que le modèle doit être réentraîné. Cette information est **invisible sur un indicateur agrégé**, et c'est pourtant la seule qui déclenche une action.

C'est aussi ce qui relie le tableau de bord au job de réentraînement mensuel défini à l'étape 6 : la courbe est le déclencheur, le job est la réponse. L'un sans l'autre ne sert à rien — surveiller sans pouvoir corriger produit de l'anxiété, corriger sans surveiller produit du gaspillage.

## 5. Le taux de faux positifs se rapporte au trafic légitime

```sql
faux_positifs / transactions_légitimes
```

et non `faux_positifs / total`.

C'est la définition métier : *quelle proportion des clients honnêtes bloque-t-on à tort ?* Rapporté au total, l'indicateur serait dilué par la classe majoritaire et paraîtrait systématiquement meilleur qu'il ne l'est.

Sur ce corpus l'écart est faible — 0,1098 % contre 0,1094 % — parce que les fraudes ne représentent que 0,4 % du volume. Il deviendrait significatif sur un portefeuille plus fraudé, et le principe reste juste indépendamment du corpus.

## 6. La charge de travail, seul indicateur que l'exploitation lit spontanément

```
alertes_par_fraude = alertes émises / fraudes détectées
```

**1,3 alerte examinée par fraude trouvée.**

Ce chiffre traduit le taux de faux positifs en réalité opérationnelle. Un dispositif exigeant vingt vérifications par fraude serait rejeté par les équipes qui doivent les traiter, quelle que soit sa performance statistique — et un dispositif rejeté ne détecte rien.

Un rappel de 95 % ne signifie rien pour un responsable d'exploitation. « Une alerte sur quatre est une fausse alerte » se comprend immédiatement.

## 7. De la mesure à l'action

Le taux global indique **qu'il y a** des faux positifs ; il ne dit pas **d'où ils viennent**.

La ventilation par catégorie de commerçant (requête n° 4) ouvre une piste concrète : si une catégorie concentre les fausses alertes, elle mérite soit une variable dédiée, soit un seuil qui lui soit propre. C'est le passage de « le taux est de 0,11 % » à « voilà où agir » — et c'est la seule chose qui justifie de mesurer.

## 8. La file d'alertes n'affiche pas qu'un score

Chaque alerte est présentée avec son contexte : montant, écart aux habitudes du porteur, nombre de transactions dans l'heure écoulée, caractère nocturne.

Un analyste qui ne verrait qu'un score de 0,87 ne pourrait **ni décider, ni contester**. En contexte bancaire réglementé, il doit pouvoir justifier un blocage — auprès du client, et le cas échéant du régulateur. Le tableau de bord prolonge donc l'exigence d'explicabilité posée à l'étape 5 : SHAP explique le modèle, la file d'alertes explique chaque décision au moment où elle doit être traitée.

## 9. Limite : le tableau de bord ne mesure pas ce qu'il devrait

En production, le taux de faux positifs se mesurerait sur le **retour des analystes** — alerte confirmée ou infirmée après enquête. Ici, il est calculé sur l'étiquette `is_fraud` du corpus, connue à l'avance.

La différence n'est pas anodine : la vérité terrain réelle arrive avec plusieurs jours de décalage, elle est incomplète — toutes les alertes ne sont pas investiguées — et elle est elle-même faillible. Un tableau de bord de production devrait afficher un taux **provisoire**, révisé au fil des retours.

Cette limite est structurelle au corpus, non au dispositif, et rejoint celle déjà énoncée à propos de la modélisation du coût : le projet ne dispose d'aucune donnée de traitement d'alertes.

---

**À retenir pour la soutenance**

**La courbe compte plus que la valeur.** Un taux de faux positifs moyen ne dit rien de sa stabilité, et c'est la dérive qui déclenche l'action. La courbe hebdomadaire est le déclencheur du réentraînement mensuel défini à l'étape 6.

**Toutes les requêtes filtrent sur le bloc de test**, et une requête de contrôle rend ce choix visible dans le tableau de bord lui-même. Agréger les trois périodes afficherait un chiffre flatteur et faux, sans aucun message d'erreur — l'erreur la plus dangereuse qu'un tableau de bord puisse commettre.

**1,3 alerte examinée par fraude trouvée.** C'est la traduction opérationnelle du KPI, et le seul indicateur qu'une équipe d'exploitation lit spontanément.
