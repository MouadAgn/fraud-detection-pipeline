# Tableau de bord

Les requêtes sont dans [`queries.sql`](queries.sql), numérotées et commentées.

## Ce que le tableau de bord doit montrer

| # | Visualisation | Type | Rôle |
| --- | --- | --- | --- |
| 1 | Indicateurs clés | Vignettes | Taux de faux positifs, rappel, alertes par fraude |
| 2 | **Évolution hebdomadaire du taux de faux positifs** | Courbe | **Le graphique de la problématique** |
| 3 | File d'alertes à traiter | Tableau | La vue qu'ouvrirait un analyste |
| 4 | Concentration des faux positifs | Barres | Où agir pour les réduire |
| 5 | Impact financier | Vignettes | La performance traduite en dollars |
| 6 | Distribution des scores | Histogramme empilé | Où le seuil vient couper |
| 7 | Comparaison des périodes | Tableau | Contrôle méthodologique |

## Construction dans Databricks SQL

1. Menu de gauche → **Dashboards** → **Create dashboard**
2. Onglet **Data** → **Create from SQL** → coller une requête de `queries.sql`
3. Onglet **Canvas** → **Add a visualization** → choisir le jeu de données et le type
4. Répéter pour les sept requêtes

Les vignettes se construisent avec le type **Counter**, en sélectionnant la colonne à afficher.

## La règle qui gouverne toutes les requêtes

```sql
WHERE split = 'test'
```

**Le modèle a vu les transactions d'entraînement.** Les métriques calculées dessus mesurent sa capacité à mémoriser, pas à détecter : elles sont optimistes par construction.

Un tableau de bord agrégeant les trois périodes afficherait un taux de faux positifs flatteur et **faux**, sans produire le moindre message d'erreur. C'est précisément le genre d'erreur qu'un tableau de bord rend invisible — d'où la requête n° 7, conservée dans le tableau de bord pour rendre ce choix explicite au lecteur.

Seul le bloc de test — 555 719 transactions du dernier semestre 2020, jamais vues — mesure une performance honnête.

## Le seuil de décision est une propriété de la table

```sql
SHOW TBLPROPERTIES workspace.fraud_detection.gold_scored_transactions;
```

Renvoie `fraud.decision_threshold`, `fraud.false_alert_cost` et `fraud.model_uri`.

La colonne `is_flagged` n'est interprétable que si l'on sait **à quel seuil et par quel modèle** elle a été calculée. Cette information voyage donc avec les données : sans elle, comparer deux exports pris à deux dates n'aurait aucun sens.

## Deux choix de conception

**Le taux de faux positifs se rapporte aux transactions légitimes, jamais au total.** Rapporté au total, il serait dilué par la classe majoritaire et paraîtrait systématiquement meilleur qu'il ne l'est — 0,11 % deviendrait 0,109 %, écart négligeable ici mais trompeur en principe.

**La courbe hebdomadaire compte plus que la valeur moyenne.** Un taux moyen ne dit rien de sa stabilité. C'est la dérive qui alerte : un taux de 0,11 % qui devient 0,4 % en trois semaines signale que la population a changé et que le modèle doit être réentraîné. Cette information est invisible sur un indicateur agrégé, et c'est pourtant la seule qui déclenche une action.
