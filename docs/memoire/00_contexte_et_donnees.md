# 00 — Contexte et choix des données

> **Étape projet** : 0 — Mise en place
> **Section mémoire visée** : Partie 2 — constitution du corpus, conformité RGPD

---

## 1. Pourquoi un jeu de données public plutôt que des données bancaires réelles

Les données de transactions bancaires sont des **données à caractère personnel** au sens de l'article 4 du RGPD : rattachées à un porteur de carte identifié, elles révèlent ses habitudes de consommation, ses déplacements et son niveau de vie. Elles sont en outre couvertes par le secret bancaire.

Trois obstacles rendent leur usage impossible dans le cadre de ce mémoire :

1. **Absence de base légale (art. 6 RGPD)** — un travail académique mené à titre individuel ne dispose ni du consentement des personnes concernées, ni d'un intérêt légitime opposable à un établissement bancaire.
2. **Principe de minimisation (art. 5.1.c)** — même avec un accord, la transmission d'un historique complet de transactions à un tiers externe serait disproportionnée au regard de la finalité.
3. **Impossibilité pratique** — aucun établissement ne diffuse ses données de fraude, celles-ci constituant à la fois un actif concurrentiel et une information exploitable par les fraudeurs.

Le recours à un **jeu de données public simulé** n'est donc pas un pis-aller : c'est la seule voie conforme. Il est présenté dans le mémoire comme un *jeu représentatif*, et ses limites sont explicitement assumées (§ 5 et note [01](01_profilage_initial.md)).

## 2. Choix du jeu de données : comparaison de trois candidats

| Critère | **Sparkov** *(retenu)* | PaySim | Credit Card Fraud (ULB) |
| --- | --- | --- | --- |
| Volume | 1,85 M transactions | 6,3 M transactions | 285 k transactions |
| Variables métier interprétables | **Oui** — client, commerçant, catégorie, montant, géolocalisation, horodatage | Partiellement — type, montant, soldes | **Non** — V1 à V28 anonymisées par ACP |
| Feature engineering comportemental | **Possible** | Limité | **Impossible** |
| Horodatage exploitable en flux | **Oui** — timestamp à la seconde | Pas de date, seulement un pas horaire | Secondes écoulées depuis la 1ʳᵉ transaction |
| Récit métier défendable devant un jury | **Oui** | Moyen | Non |
| Déséquilibre de classes réaliste | 0,52 % | 0,13 % | 0,17 % |

**Décision : Sparkov** — c'est le seul des trois qui permette de construire les variables comportementales au cœur de la problématique (fréquence d'achat sur courte période, écart au comportement habituel du client, contexte géographique) *et* d'expliquer une prédiction en langage métier.

**ULB écarté** : ses variables étant déjà transformées par analyse en composantes principales, aucune ne porte de signification interprétable. Il aurait été impossible d'y faire du feature engineering, et l'explicabilité SHAP y produirait des conclusions du type « la composante V14 contribue négativement » — sans valeur en contexte bancaire réglementé.

**PaySim écarté** : son volume supérieur aurait été un atout pour la dimension *vélocité*, mais ses colonnes trop pauvres auraient vidé de sa substance l'étape de feature engineering, qui est le cœur du projet.

**Source** : <https://www.kaggle.com/datasets/kartik2112/fraud-detection>
Le jeu est produit par l'outil *Sparkov Data Generation* (B. Harris), qui simule des parcours d'achat à partir de profils de consommation et injecte des scénarios de fraude. Les identités (noms, adresses, dates de naissance, numéros de carte) sont générées synthétiquement.

## 3. Description du corpus

Chiffres établis par `scripts/profile_dataset.py` (rejouable) :

| | `fraudTrain.csv` | `fraudTest.csv` | **Total** |
| --- | ---: | ---: | ---: |
| Transactions | 1 296 675 | 555 719 | **1 852 394** |
| Fraudes | 7 506 | 2 145 | **9 651** |
| Taux de fraude | 0,579 % | 0,386 % | **0,521 %** |
| Ratio légitime : fraude | 172 : 1 | 258 : 1 | **191 : 1** |
| Début de période | 01/01/2019 00:00:18 | 21/06/2020 12:14:25 | |
| Fin de période | 21/06/2020 12:13:37 | 31/12/2020 23:59:34 | **24 mois** |
| Volume disque | 351 Mo | 150 Mo | **501 Mo** |

**Cardinalités métier** : 999 porteurs de carte distincts (983 dans le train, 924 dans le test, 908 communs aux deux), 693 commerçants, 14 catégories d'achat, 51 États américains.

**Structure** : 23 colonnes, dont 22 exploitables — la première (`Unnamed: 0`) est un index technique issu de l'export CSV, sans valeur métier.

| Groupe | Colonnes |
| --- | --- |
| Identification transaction | `trans_num`, `trans_date_trans_time`, `unix_time` |
| Porteur de carte | `cc_num`, `first`, `last`, `gender`, `dob`, `job` |
| Adresse du porteur | `street`, `city`, `state`, `zip`, `lat`, `long`, `city_pop` |
| Commerçant | `merchant`, `category`, `merch_lat`, `merch_long` |
| Transaction | `amt` |
| Cible | `is_fraud` |

> ⚠️ **Deux écarts avec la fiche de cadrage initiale**, relevés au profilage : le corpus compte **693 commerçants** (et non 800) et **1,85 M transactions au total** (et non 1,3 M — ce chiffre ne correspond qu'au fichier d'entraînement). Les valeurs ci-dessus sont celles mesurées sur les fichiers.

## 4. Position au regard du RGPD

Le jeu de données contient des colonnes qui, dans un système réel, seraient des **données à caractère personnel directement identifiantes** : nom, prénom, adresse postale, date de naissance, numéro de carte, profession.

Ces valeurs étant ici **entièrement synthétiques**, elles ne se rapportent à aucune personne physique identifiée ou identifiable. Le traitement réalisé dans ce projet **ne relève donc pas du RGPD**.

Le pipeline applique néanmoins une **pseudonymisation en couche Silver** (hachage du numéro de carte, suppression des identifiants directs). Ce choix est délibéré et poursuit trois objectifs :

1. **Démontrer la maîtrise de la contrainte** — un pipeline destiné à un contexte bancaire réel doit intégrer la protection des données par conception (art. 25 RGPD, *privacy by design*). Un projet de portfolio qui l'ignore ne serait pas transposable.
2. **Vérifier que le modèle n'en dépend pas** — si les performances chutaient sans les identifiants directs, cela signalerait un modèle qui mémorise des individus au lieu d'apprendre des comportements.
3. **Rendre le pipeline directement transposable** — la même chaîne peut être branchée sur des données réelles sans modification de la logique de conformité.

Le détail des règles appliquées figure dans la note 03 (couche Silver).

## 5. Limites assumées du corpus

Ces limites sont énoncées ici et reprises dans le bilan critique (Partie 3). Les documenter en amont vaut mieux que de les subir en soutenance.

| Limite | Conséquence | Traitement dans le projet |
| --- | --- | --- |
| **Données simulées** | Les scénarios de fraude suivent des règles de génération plus régulières que la fraude réelle. Les performances obtenues constituent une **borne supérieure optimiste**. | Énoncé explicitement ; les métriques sont présentées comme relatives (comparaison entre modèles) et non comme une promesse de performance en production. |
| **Séparabilité artificielle** | Certaines variables sont anormalement discriminantes du fait du générateur (cf. note [01](01_profilage_initial.md), § 3). | Identifié par le profilage, quantifié, et discuté dans l'analyse SHAP. |
| **Périmètre géographique unique** | Uniquement des États-Unis, sur 51 États. Pas de transaction transfrontalière ni de devise étrangère. | Aucune variable de change ou de pays n'est construite : elle serait constante. |
| **Absence de dérive de population** | 908 des porteurs de carte sont présents dans les deux périodes ; aucun client réellement nouveau à grande échelle. | Le protocole d'évaluation reste un split temporel, qui teste malgré tout la dérive du comportement de fraude dans le temps (le taux de fraude passe de 0,579 % à 0,386 %). |
| **Absence de retour métier** | Aucune information sur les alertes réellement traitées par un analyste, ni sur le coût d'un faux positif. | Le coût métier est modélisé par hypothèse explicite lors du choix du seuil (note 06). |

---

**À retenir pour la soutenance**
Si un membre du jury demande « pourquoi ne pas avoir utilisé de vraies données ? », la réponse tient en trois points : absence de base légale RGPD, impossibilité pratique d'obtenir un jeu bancaire réel, et choix d'un corpus simulé assez riche pour permettre le feature engineering comportemental — avec ses limites explicitement documentées.
