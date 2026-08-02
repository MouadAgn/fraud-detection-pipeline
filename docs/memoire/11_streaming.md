# 11 — Traitement en flux

> **Étape projet** : 8 — Streaming (bonus)
> **Section mémoire visée** : Partie 2 — dimension vélocité du Big Data · Partie 3 — bilan critique
> **Code** : [`notebooks/06_streaming_replay.py`](../../notebooks/06_streaming_replay.py) · [`notebooks/07_streaming_pipeline.py`](../../notebooks/07_streaming_pipeline.py)

---

## 1. Pourquoi cette étape

Le pipeline batch répond aux dimensions **volume** et **variété** du Big Data. Il ne répond pas à la **vélocité** : un traitement quotidien détecte une fraude jusqu'à vingt-quatre heures après qu'elle a eu lieu, quand la carte a déjà été vidée.

Cette étape transforme la chaîne en dispositif capable de traiter les transactions **au fil de leur arrivée**. Elle est traitée en dernier, et c'est volontaire : le streaming ajoute une difficulté considérable pour un bénéfice nul tant que la logique métier n'est pas stabilisée.

## 2. Fichiers déposés plutôt que Kafka

| Approche | Ce qu'elle démontre | Coût |
| --- | --- | --- |
| **Fichiers + Auto Loader** *(retenue)* | Ingestion incrémentale, reprise sur point de contrôle | Aucun service externe |
| Kafka | Idem, plus l'exploitation d'un courtier de messages | Un cluster à administrer |

Le dépôt de fichiers correspond au cas le plus fréquent en banque — un système transactionnel qui exporte périodiquement — et il isole ce qui est réellement en jeu, le **traitement incrémental**, sans le mêler à l'exploitation d'un courtier.

Ajouter Kafka aurait produit une brique de plus à présenter, sans rien démontrer que le dépôt de fichiers ne démontre déjà.

## 3. Ce que le streaming change vraiment

L'API est presque identique : `readStream` au lieu de `read`, `writeStream` au lieu de `write`. C'est l'argument commercial de Spark, et il est en partie trompeur — **la ressemblance du code masque une différence de nature**.

En batch, chaque traitement voit **toutes** les données. En flux, il ne voit qu'un micro-lot et doit se souvenir du reste.

| | Batch | Flux |
| --- | --- | --- |
| Vision des données | Complète | Un micro-lot à la fois |
| Classement global | Possible | **Impossible** — le prochain lot pourrait contenir une ligne antérieure |
| Historique d'un client | Recalculable à volonté | Doit être **conservé en état** |
| Reprise après panne | Relancer le traitement | Rejouer depuis le **point de contrôle** |

## 4. Auto Loader n'est pas un `readStream` sur répertoire

Un `readStream` classique sur un dossier doit **relister tous les fichiers** à chaque micro-lot pour repérer les nouveaux. Sur un répertoire qui accumule des millions de fichiers, ce listage devient le goulot d'étranglement — le traitement passe plus de temps à chercher qu'à calculer.

Auto Loader tient un index des fichiers déjà vus dans son point de contrôle. Le coût de détection devient proportionnel au nombre de **nouveaux** fichiers, non au total accumulé.

C'est une différence de complexité algorithmique, pas une commodité.

## 5. La déduplication ne se transpose pas — et c'est instructif

C'est la difficulté la plus révélatrice de cette étape.

En batch, les doublons sont écartés par `row_number()` sur une fenêtre ordonnée (note [03](03_qualite_silver.md)). Cette opération suppose un **classement global** de toutes les lignes partageant un identifiant.

En flux, cette information n'existe pas. Le prochain micro-lot pourrait contenir un exemplaire qui aurait dû être classé premier. Spark **refuse** donc l'opération sur un DataFrame en flux — et il a raison de refuser : l'exécuter donnerait un résultat dépendant de l'ordre d'arrivée.

### L'équivalent en flux, et son compromis

`dropDuplicatesWithinWatermark` déduplique sur une **fenêtre temporelle bornée** au lieu de l'historique complet.

Le filigrane est nécessaire pour une raison précise : sans lui, Spark devrait mémoriser **tous** les identifiants déjà vus, pour toujours. L'état croîtrait indéfiniment jusqu'à saturation mémoire. Le filigrane fixe la limite — ici, une transaction ne peut arriver avec plus d'un jour de retard ; au-delà, elle est ignorée et son identifiant peut être oublié.

**C'est un compromis assumé, pas une équivalence.** Un doublon séparé de plus d'un jour passerait en flux alors qu'il serait détecté en batch. La largeur de fenêtre se choisit d'après le retard réellement observé sur les données amont, non par convention.

### Ce que le reste du code démontre

Les huit autres règles de qualité s'appliquent **sans aucune modification**. Elles portent sur une seule ligne à la fois, donc ne dépendent d'aucun contexte. Le module `silver.py` sert les deux chaînes, avec un unique paramètre pour désactiver la déduplication.

C'est le bénéfice réel de l'API unifiée : non pas que tout se transpose, mais que **ce qui se transpose le fait sans réécriture**, et que ce qui ne se transpose pas se signale explicitement au lieu de produire un résultat faux.

## 6. Le vrai mur : les variables comportementales

Les variables de la couche Gold reposent sur des fenêtres glissantes par porteur — « combien de transactions dans la dernière heure », « quel est son montant moyen historique ».

En batch, Spark relit tout l'historique. En flux, il faudrait conserver **un état par client**, indéfiniment. C'est ici que le streaming cesse d'être une variante d'écriture pour devenir un problème d'architecture.

### Deux architectures possibles

| Approche | Principe | Limite |
| --- | --- | --- |
| **État Spark** (`flatMapGroupsWithState`) | Spark garde l'historique de chaque client en mémoire entre les micro-lots | État proportionnel au nombre de clients ; complexité et coût de reprise élevés |
| **Profil précalculé + jointure flux/statique** *(retenue)* | Le batch calcule périodiquement un profil par client ; le flux le consulte | Le profil a jusqu'à 24 h de retard |

### Pourquoi la seconde

C'est l'architecture la plus répandue en production, et son principe est solide : **elle sépare ce qui doit être temps réel de ce qui peut ne pas l'être.**

La *décision* doit être immédiate — bloquer ou laisser passer, en quelques centaines de millisecondes. La *connaissance du comportement habituel* d'un client, elle, n'évolue pas d'une minute à l'autre : un montant moyen calculé sur mille transactions ne bouge pas significativement parce qu'une transaction s'ajoute.

Recalculer ce profil en temps réel coûterait cher pour un gain nul.

C'est le principe d'un **feature store** : les variables lentes sont précalculées et servies à faible latence, les variables rapides sont calculées à la volée.

### La limite de la solution retenue

La jointure flux/statique n'est possible ici que parce que le côté statique est **petit** — 999 porteurs, diffusables à tous les exécuteurs. Sur un portefeuille de plusieurs millions de clients, une table Delta ne suffirait plus : il faudrait un magasin clé-valeur à faible latence (Redis, DynamoDB, ou le Feature Serving de Databricks).

Le principe resterait identique ; seul le support du profil changerait.

## 7. Le déclencheur : `availableNow` plutôt qu'un flux continu

Le compute serverless n'autorise que les déclencheurs `availableNow` et `once`. Un flux continu exigerait un cluster dédié tournant en permanence.

Cette contrainte n'est pas une limitation subie : `availableNow` traite tout ce qui est arrivé depuis le dernier passage, puis s'arrête. C'est le mode dit **batch incrémental**, et c'est celui que retiennent la plupart des pipelines de production.

| | Flux continu | `availableNow` |
| --- | --- | --- |
| Latence | Secondes | Celle de la planification |
| Coût | Cluster allumé en permanence | Uniquement pendant le traitement |
| Orchestration | Impossible dans un job qui se termine | Naturelle |
| Reprise | Point de contrôle | Point de contrôle — identique |

**La sémantique incrémentale est la même** : seuls les nouveaux fichiers sont traités, et le point de contrôle garantit qu'aucun ne l'est deux fois. Ce qui change est la fréquence de réveil, pas la logique.

Pour une détection réellement temps réel — bloquer une transaction en quelques centaines de millisecondes — il faudrait un cluster dédié et un déclencheur continu. C'est un arbitrage de coût, non une limite technique : la même chaîne y fonctionnerait sans modification.

## 8. Ce qui n'a pas été fait

**Le scoring n'est pas branché sur le flux.** La chaîne en flux s'arrête aux variables enrichies. Appliquer le modèle nécessiterait une fonction Python vectorisée chargeant le pipeline scikit-learn sur chaque exécuteur — techniquement accessible, mais qui n'aurait rien démontré de plus sur la vélocité.

**Les compteurs de vélocité (1 h / 24 h) ne sont pas produits en flux.** Ils exigent un état par client, c'est-à-dire la première des deux architectures ci-dessus. Les produire aurait doublé la complexité du notebook pour illustrer un mécanisme déjà expliqué.

Ces deux limites sont énoncées plutôt que masquées. Un jury distingue sans peine une limite assumée d'un oubli.

## 9. Le piège pratique du streaming

Un détail mérite d'être mentionné, car il coûte du temps à tout le monde une fois.

Rejouer une démonstration suppose de vider **l'atterrissage et les points de contrôle**. Vider seulement l'atterrissage donne un flux qui ne traite rien et **n'émet aucune erreur** : Auto Loader considère les fichiers réécrits comme déjà vus.

C'est le symptôme le plus déroutant du streaming — « rien ne se passe, et rien ne le signale » — et la première chose à vérifier.

---

**À retenir pour la soutenance**

**Le code batch ne se transpose pas tel quel.** `row_number()` suppose un classement global que le flux ne peut pas fournir. La ressemblance des API masque une différence de nature : en batch on voit tout, en flux on voit un micro-lot et l'on doit se souvenir du reste.

**Le filigrane borne l'état, au prix d'un compromis explicite.** Sans lui, Spark mémoriserait tous les identifiants vus, pour toujours. Avec lui, un doublon séparé de plus d'un jour échappe à la détection.

**Les variables comportementales ne passent pas en flux pur.** L'architecture retenue — profil précalculé en batch, consulté par jointure flux/statique — sépare ce qui doit être temps réel, la décision, de ce qui peut ne pas l'être, la connaissance du comportement habituel. C'est le principe d'un feature store, et c'est la réponse standard de l'industrie à ce problème.
