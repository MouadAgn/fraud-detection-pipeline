# 09 — Orchestration du pipeline

> **Étape projet** : 6 — Orchestration
> **Section mémoire visée** : Partie 2 — industrialisation de la chaîne de traitement
> **Code** : [`databricks.yml`](../../databricks.yml) · [`workflows/README.md`](../../workflows/README.md)

---

## 1. Ce que l'orchestration change

Jusqu'à cette étape, le pipeline était une **succession de notebooks exécutés à la main**, dans le bon ordre, par quelqu'un qui connaissait cet ordre. C'est suffisant pour démontrer une méthode ; ce n'est pas un système.

L'orchestration transforme cette suite en un traitement qui s'exécute **seul, à heure fixe, avec des dépendances explicites, des reprises sur erreur et des alertes**. C'est la différence entre savoir coder un traitement et savoir en exploiter un.

## 2. Un fichier versionné plutôt que l'interface graphique

Le pipeline est décrit dans `databricks.yml`, au format **Databricks Asset Bundle**.

Un enchaînement construit dans l'interface n'existe que dans le workspace : il ne se relit pas en revue de code, ne se compare pas d'une version à l'autre, ne se redéploie pas sur un autre environnement, et disparaît avec le workspace. Décrit dans un fichier versionné, le pipeline devient un **artefact du projet** au même titre que le code — avec son historique, sa traçabilité et sa reproductibilité.

C'est l'application au pipeline du principe déjà retenu pour le reste : ce qui n'est pas versionné n'est pas défendable.

## 3. La décision structurante : deux jobs et non un seul

| Job | Contenu | Fréquence |
| --- | --- | --- |
| **`fraud_detection_pipeline`** | setup → bronze → silver → gold → scoring | Quotidienne |
| **`fraud_model_training`** | Entraînement, comparaison, enregistrement | Mensuelle ou manuelle |

**Le pipeline de données et le cycle de vie du modèle n'ont pas la même fréquence.** Les données arrivent tous les jours ; un modèle se réentraîne quand la population a suffisamment dérivé — typiquement une fois par mois.

Les enchaîner dans un même job aurait trois conséquences, toutes indésirables :

1. **Un modèle différent chaque jour**, donc des décisions non reproductibles : une même transaction pourrait être bloquée un jour et acceptée le lendemain sans que rien n'ait changé dans les données.
2. **Des métriques incomparables** d'un jour à l'autre, puisqu'elles porteraient à chaque fois sur un modèle différent.
3. **Vingt minutes d'entraînement quotidien** pour un résultat qui n'évolue pas significativement à cette échelle de temps.

Le pipeline quotidien **consomme** la version portant l'alias `champion` ; le job d'entraînement est le seul habilité à déplacer cet alias. C'est cette séparation qui rend le système prévisible.

La confusion entre pipeline de données et cycle de vie du modèle est fréquente, y compris dans des projets professionnels. La distinguer explicitement est un marqueur de maturité.

## 4. Le graphe de dépendances

```
setup → bronze_ingestion → silver_cleaning → gold_features → scoring
```

Le champ `depends_on` de chaque tâche est ce qui constitue le **graphe orienté acyclique** (DAG). Sans lui, Databricks lancerait les cinq tâches en parallèle : la couche Silver démarrerait avant que Bronze n'ait écrit sa table, et le pipeline échouerait pour une raison difficile à diagnostiquer.

La tâche `setup` est incluse bien qu'elle ne fasse que créer le schéma et le volume : elle rend le job exécutable sur un workspace vierge, sans préparation manuelle préalable. C'est ce qui distingue un pipeline déployable d'un pipeline qui suppose un environnement déjà en place.

## 5. Résultats d'exécution

Exécution du 29 juillet 2026, compute serverless en mode *performance optimized*.

| Tâche | Durée | Statut |
| --- | ---: | --- |
| `setup` | 30 s | ✅ |
| `bronze_ingestion` | 42 s | ✅ |
| `silver_cleaning` | 1 min 12 | ✅ |
| `gold_features` | 1 min 05 | ✅ |
| `scoring` | ~1 min 46 | ✅ |
| **Total** | **5 min 15** | **Succeeded** |

**L'ensemble de la chaîne — ingestion, contrôles qualité, pseudonymisation, construction de vingt variables comportementales par fenêtres glissantes, puis scoring de 1 852 394 transactions — s'exécute en un peu plus de cinq minutes, sans intervention humaine.**

### Lecture de la répartition

`silver_cleaning` (1 min 12) est plus lente que `bronze_ingestion` (42 s), alors qu'elle traite le même volume déjà chargé en Delta. L'écart s'explique : Bronze ne fait que lire et écrire, tandis que Silver exécute onze règles de qualité, applique un hachage SHA-256 sur 1,85 M de lignes et reconstruit les codes postaux.

C'est cohérent avec la nature des couches — Bronze déplace la donnée, Silver la transforme.

### Ce que ces chiffres démontrent

Ils matérialisent l'apport de Spark. Le même traitement en pandas sur un poste unique prendrait un ordre de grandeur de plus, et surtout ne tiendrait pas en mémoire au-delà de quelques millions de lignes — alors que le pipeline resterait valable à cent millions.

Le champ *Lineage* de Databricks affiche par ailleurs « 3 tables amont, 4 tables aval » : la traçabilité entre les tables est **détectée automatiquement** à partir des requêtes exécutées, sans avoir été déclarée. C'est un apport direct d'Unity Catalog, appréciable en contexte audité.

## 6. Configuration, et pourquoi

| Réglage | Valeur | Justification |
| --- | --- | --- |
| `max_concurrent_runs` | 1 | Deux exécutions simultanées écriraient concurremment sur les mêmes tables Delta |
| `max_retries` (pipeline) | 1 | Les échecs sont des erreurs de données, pas des incidents transitoires. Réessayer dix fois masquerait le problème au lieu de le signaler |
| `max_retries` (entraînement) | 0 | Un entraînement dure une vingtaine de minutes et échoue pour des raisons qu'une reprise ne corrigera pas |
| `pause_status` | `PAUSED` | Un job déployé ne doit pas se déclencher avant d'avoir été exécuté manuellement au moins une fois |
| Notification | à l'échec uniquement | Notifier chaque succès produit un bruit que plus personne ne lit — et l'échec passe alors inaperçu |

Le mécanisme de file d'attente s'est d'ailleurs vérifié en conditions réelles : trois exécutions lancées coup sur coup ont été **sérialisées** par Databricks au lieu d'être menées en parallèle, ce qui a effectivement protégé les tables Delta d'écritures concurrentes.

## 7. Ce qui rend cette orchestration possible

Deux propriétés, respectées **dès l'étape 1**, sont ici indispensables.

**Chaque notebook est exécutable indépendamment.** Il lit ses entrées depuis Unity Catalog et n'hérite d'aucune variable du précédent. Dans un Workflow, chaque tâche s'exécute dans sa propre session : un notebook dépendant de l'état laissé par un autre ne pourrait tout simplement pas être orchestré.

**Chaque notebook est idempotent.** Toutes les écritures se font en `overwrite` sur des sources complètes. Une tâche qui échoue se relance telle quelle, sans nettoyage préalable ni risque de doublon.

Ces contraintes n'ont pas été ajoutées à cette étape : elles ont été posées dès la conception de la couche Bronze, précisément parce qu'elles conditionnaient l'industrialisation.

Le point a été vérifié malgré nous : la session serverless d'un notebook s'est interrompue pour inactivité en cours de projet, effaçant tout l'état Python. Le notebook de scoring a pu être relancé seul, sans reprendre les précédents — démonstration involontaire mais probante de cette indépendance.

## 8. Limite identifiée : le seuil de décision est recalculé à chaque exécution

Le notebook `05_scoring_eval` redérive le seuil optimal depuis le bloc de validation à chaque passage.

En exécution manuelle, c'était cohérent : le seuil était l'objet même de l'étude. **Dans un pipeline planifié, c'est un défaut de conception.** La règle de décision changerait silencieusement d'un jour à l'autre, et les taux de faux positifs ne seraient plus comparables dans le temps — or c'est précisément l'indicateur que le tableau de bord doit suivre.

Un seuil de décision doit être **fixé, versionné, et modifié explicitement**, jamais redérivé automatiquement. La correction consiste à le passer en paramètre du job, le recalcul n'intervenant que sur demande explicite lors d'une révision du modèle.

Cette limite est documentée plutôt que corrigée dans l'urgence : elle illustre une différence réelle entre un traitement exploratoire et un traitement industrialisé, et c'est à ce titre qu'elle a sa place dans le bilan critique.

---

**À retenir pour la soutenance**

**Deux jobs et non un seul** : le pipeline de données tourne tous les jours, le réentraînement une fois par mois. Les confondre produirait un modèle différent chaque jour, donc des décisions non reproductibles et des métriques incomparables.

**Le pipeline est décrit dans un fichier versionné**, pas construit dans une interface. Ce qui n'est pas versionné ne se relit pas, ne se redéploie pas, et disparaît avec le workspace.

**Toute la chaîne s'exécute en 5 min 15 sur 1,85 M de transactions**, sans intervention. Et elle n'est orchestrable que parce que chaque notebook est indépendant et idempotent — deux contraintes posées dès l'étape 1, pas ajoutées à la fin.
