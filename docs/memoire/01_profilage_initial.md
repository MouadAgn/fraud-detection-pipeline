# 01 — Profilage initial des données

> **Étape projet** : 0 — Mise en place
> **Sections mémoire visées** : Partie 2 — analyse exploratoire et spécification des traitements · Partie 3 — bilan critique
> **Reproductible via** : `scripts/profile_dataset.py`

---

## 1. Objectif et méthode

Avant d'écrire la moindre ligne de pipeline, on profile la source. L'objectif n'est pas de « regarder les données » mais de **répondre à des questions dont dépend le code des étapes suivantes** :

- Quelles règles de qualité la couche Silver doit-elle réellement appliquer ?
- Quelles variables comportementales ont une chance d'être discriminantes ?
- Quelles largeurs de fenêtre temporelle sont pertinentes au vu de la cadence réelle des transactions ?
- Quelles particularités du corpus fausseraient l'interprétation des résultats ?

Écrire des règles de nettoyage sans avoir mesuré ce qu'elles corrigent, ou construire des variables sans avoir vérifié qu'elles portent un signal, revient à produire du code décoratif. Les sept constats ci-dessous sont donc chacun assortis de la décision qu'ils entraînent.

---

## 2. Constat 1 — La qualité brute est excellente, et cela change la nature de l'étape Silver

| Contrôle | Résultat sur 1 852 394 lignes |
| --- | ---: |
| Valeurs manquantes (toutes colonnes) | **0** |
| `trans_num` dupliqués | **0** |
| Lignes strictement identiques | **0** |
| Montants négatifs ou nuls (`amt <= 0`) | **0** |
| Horodatages invalides | **0** |

**Interprétation.** C'est la signature d'un jeu généré : aucune saisie manuelle, aucun système amont défaillant, aucune reprise d'historique. Un flux bancaire réel présenterait des taux de valeurs manquantes de plusieurs pourcents sur les champs de géolocalisation et des doublons issus des rejeux d'interface.

**Décision.** L'étape Silver ne peut pas être présentée comme une étape de *réparation* — il n'y a rien à réparer. Elle est donc redéfinie autour de trois responsabilités réelles :

1. **Validation défensive** — les contrôles sont écrits et exécutés, et leur résultat (0 rejet) est *publié comme métrique*. Un contrôle qui ne rejette rien n'est pas un contrôle inutile : c'est un contrôle qui prouve la conformité, et qui protégera le pipeline le jour où la source changera.
2. **Typage et normalisation** — le CSV ne porte aucun type ; c'est la couche Silver qui établit le contrat de données.
3. **Conformité et pseudonymisation** — voir note [00](00_contexte_et_donnees.md), § 4.

C'est une différence de posture importante en soutenance : présenter des contrôles qui passent tous, en le disant, est plus solide que d'inventer un nettoyage fictif.

---

## 3. Constat 2 — `unix_time` est incohérent avec `trans_date_trans_time` (décalage de 7 ans)

Les deux colonnes sont censées représenter le même instant. Elles ne le font pas.

| Mesure | Valeur |
| --- | --- |
| Écart minimal | 220 838 400 s |
| Écart maximal | 220 924 800 s |
| Nombre de valeurs d'écart distinctes | **2** |
| Écart moyen converti | **6,9989 an** |

Exemple : la première transaction porte `trans_date_trans_time = 2019-01-01 00:00:18` et `unix_time = 1325376018`, qui correspond à **2012-01-01 00:00:18**.

**Interprétation.** L'écart n'est ni aléatoire ni dérivant : il vaut exactement 7 années calendaires, et ne prend que **deux valeurs distinctes** — 2 556 et 2 557 jours, l'écart d'un jour s'expliquant par les années bissextiles traversées. Il s'agit donc d'un **décalage systématique introduit lors de la génération**, et non d'une corruption de données.

**Décision.**
- `trans_date_trans_time` est déclaré **horodatage de référence** du pipeline. Toute la logique temporelle (fenêtres, split train/test, agrégations du dashboard) s'appuie sur lui.
- `unix_time` n'est pas conservé tel quel : il est **recalculé** à partir de l'horodatage de référence en couche Silver. Le conserver aurait exposé le projet à un bug silencieux — un développeur utilisant `unix_time` pour une fenêtre temporelle aurait obtenu un résultat cohérent en interne mais décalé de 7 ans par rapport au reste du pipeline.

> C'est le seul véritable défaut de qualité du corpus, et il est **invisible à l'inspection superficielle** : les deux colonnes semblent plausibles prises isolément. Il n'apparaît qu'en testant leur *cohérence croisée* — un contrôle qu'on n'écrit que si on y pense. C'est un bon exemple, exploitable en Partie 2, de ce que doit être un contrôle qualité.

---

## 4. Constat 3 — Le préfixe `fraud_` des commerçants n'est pas une fuite de la cible

Tous les libellés de commerçants commencent par `fraud_` (`fraud_Rippin, Kub and Mann`, `fraud_Kirlin and Sons`, …).

| Vérification | Résultat |
| --- | ---: |
| Transactions dont le commerçant est préfixé `fraud_` | 1 296 675 / 1 296 675 — **100 %** |

**Interprétation.** Le préfixe est appliqué à **tous** les commerçants, frauduleux ou non. Il ne porte donc aucune information sur la cible : c'est un artefact de nommage du générateur.

**Décision.** Le préfixe est retiré en couche Silver, pour lisibilité. Il fallait néanmoins le vérifier : un préfixe corrélé à la cible aurait constitué une **fuite massive** (le modèle aurait « prédit » la fraude en lisant le nom du commerçant), invalidant tous les résultats. Le contrôle est documenté ici pour prouver qu'il a été fait.

---

## 5. Constat 4 — L'heure de la transaction est le signal dominant, et c'est un artefact

Taux de fraude par heure de la journée (jeu d'entraînement) :

| Plage horaire | Taux de fraude |
| --- | ---: |
| 22 h – 23 h | **2,84 % – 2,88 %** |
| 00 h – 03 h | **1,42 % – 1,54 %** |
| 04 h – 21 h | 0,095 % – 0,142 % |

L'écart entre la plage nocturne et la plage diurne représente un rapport de **10 à 30 selon les heures comparées**.

**Interprétation.** Une concentration nocturne de la fraude est plausible dans la réalité — les dispositifs de surveillance humaine sont réduits la nuit. Mais l'amplitude et la **netteté de la rupture** (passage de 0,113 % à 21 h à 2,883 % à 22 h, sans transition) trahissent une **règle de génération** : le simulateur tire les fraudes dans des créneaux horaires prédéfinis.

**Décision et précaution.**
- L'heure est conservée comme variable explicative : la retirer serait arbitraire, et elle correspond à un phénomène métier réel.
- Mais **son poids sera surveillé dans l'analyse SHAP** (note 07). Si elle domine outrageusement les autres variables, c'est le signe que le modèle apprend surtout la règle du générateur, ce qui doit être dit — c'est précisément ce genre de lucidité qu'attend un jury.
- Cette dépendance est le principal argument expliquant pourquoi les performances obtenues ne sont pas transposables telles quelles en production (note [00](00_contexte_et_donnees.md), § 5).

---

## 6. Constat 5 — La distance géographique n'est PAS discriminante (résultat négatif)

La fiche de cadrage préconisait une variable « distance/localisation inhabituelle ». Elle a été testée avant d'être construite : distance de Haversine entre les coordonnées du porteur (`lat`, `long`) et celles du commerçant (`merch_lat`, `merch_long`).

| | Transactions légitimes | Transactions frauduleuses |
| --- | ---: | ---: |
| Distance moyenne | 76,11 km | **76,27 km** |
| Distance médiane | 78,23 km | **77,93 km** |
| Distance maximale | 152,12 km | 144,52 km |

**Interprétation.** Les deux distributions sont **indiscernables**. Le générateur tire la position du commerçant uniformément dans un rayon fixe autour du domicile du porteur, indépendamment du caractère frauduleux — d'où une distance quasi constante et un maximum plafonné à ~152 km dans les deux classes.

**Décision.** La variable de distance brute est **écartée du jeu de features**. Elle est néanmoins calculée et documentée, pour trois raisons :

1. Un jury demandera pourquoi une variable annoncée dans la méthodologie n'apparaît pas dans le modèle final. La réponse « elle a été testée, mesurée, et écartée sur preuve » est bien meilleure qu'un silence.
2. C'est un **résultat négatif documenté** — un marqueur de rigueur méthodologique, pas un échec.
3. Sur des données réelles, cette variable serait vraisemblablement l'une des plus fortes. Son inefficacité ici est une **limite du corpus**, pas une conclusion générale sur la détection de fraude. La distinction doit être faite explicitement en Partie 3.

---

## 7. Constat 6 — Le montant est fortement discriminant, mais plafonné artificiellement

| | Légitimes | Frauduleuses | Rapport |
| --- | ---: | ---: | ---: |
| Montant moyen | 67,67 $ | **531,32 $** | × 7,9 |
| Montant médian | 47,28 $ | **396,51 $** | × 8,4 |
| Montant maximal | **28 948,90 $** | 1 376,04 $ | × 0,05 |

**Interprétation.** Le signal est net dans la zone centrale : une fraude coûte en médiane huit fois plus qu'une transaction normale. Mais la dernière ligne révèle un second artefact : **aucune fraude ne dépasse 1 376 $**, alors que les transactions légitimes montent à près de 29 000 $.

Conséquence contre-intuitive : dans ce corpus, **un montant très élevé est un indicateur de transaction légitime**, l'inverse de l'intuition métier. Un modèle entraîné ici apprendra cette règle.

**Décision.** Le montant brut est conservé, mais l'effort de feature engineering porte sur sa forme **relative** — écart au montant habituel du client, exprimé en écarts-types de son propre historique. Une variable relative est plus robuste qu'un seuil absolu : elle reste valide même si l'échelle des montants change, et elle capte la notion métier réelle, qui est « inhabituel pour *ce* client » et non « élevé dans l'absolu ».

---

## 8. Constat 7 — Les fraudes surviennent en rafales : la vélocité est le bon axe de features

C'est le constat qui **valide empiriquement** l'axe principal de feature engineering annoncé dans la fiche.

**Cadence de référence d'un client normal :**

| Mesure | Valeur |
| --- | ---: |
| Transactions par client (moyenne) | 1 319 |
| Délai médian entre deux transactions | **4,60 h** |
| 5ᵉ percentile du délai | 16,4 min |
| 95ᵉ percentile du délai | 31,7 h |

**Cadence lors d'un épisode de fraude, sur la même carte :**

| Mesure | Valeur |
| --- | ---: |
| Délai médian entre deux fraudes consécutives | **64,9 min** |
| Part des fraudes survenant moins d'1 h après la fraude précédente | **43,3 %** |

**Interprétation.** Le délai médian entre deux fraudes sur une même carte est **4,3 fois plus court** qu'entre deux transactions normales, et près d'une fraude sur deux suit la précédente en moins d'une heure. Le scénario de fraude est donc un **épisode dense** : la carte est compromise puis exploitée en série jusqu'au blocage. C'est exactement le comportement que des compteurs glissants savent capter.

**Décision — spécification des fenêtres de l'étape 3.** Les largeurs sont choisies sur la base de ces mesures, pas par convention :

| Fenêtre | Justification chiffrée |
| --- | --- |
| **1 h** | Capte 43,3 % des fraudes en rafale, alors qu'un client normal n'a qu'une transaction toutes les 4,6 h en médiane. Le contraste est maximal à cette échelle. |
| **24 h** | Couvre un cycle d'activité complet (le 95ᵉ percentile du délai normal est à 31,7 h), et capte l'épisode de fraude entier plutôt qu'une rafale isolée. |
| **7 j** | Sert de référence de comportement habituel du client, sur laquelle les deux fenêtres courtes sont comparées. |

---

## 9. Synthèse — ce que le profilage impose aux étapes suivantes

| Étape | Décision issue du profilage |
| --- | --- |
| **1 — Bronze** | Ingérer les 23 colonnes sans transformation, en conservant `Unnamed: 0` et `unix_time` malgré leurs défauts : Bronze doit rester fidèle à la source pour permettre le rejeu. |
| **2 — Silver** | Supprimer `Unnamed: 0` ; recalculer `unix_time` depuis l'horodatage de référence ; retirer le préfixe `fraud_` ; pseudonymiser `cc_num` et supprimer les identifiants directs ; exécuter et **publier** les contrôles qualité même s'ils ne rejettent rien ; signaler (sans rejeter) les 15 568 transactions dont le porteur a moins de 18 ans. |
| **3 — Gold** | Construire les compteurs de vélocité sur 1 h / 24 h / 7 j ; construire l'écart de montant en unités d'écart-type de l'historique du client ; conserver l'heure de la journée ; **ne pas** retenir la distance géographique brute. |
| **4 — Modèle** | Surveiller le poids de l'heure de la journée dans l'analyse SHAP ; conserver la coupure temporelle native du 21/06/2020 comme frontière train/test. |
| **Partie 3** | Trois artefacts du générateur à discuter dans le bilan critique : concentration horaire, plafonnement des montants frauduleux, non-discrimination de la distance. |

---

**À retenir pour la soutenance**
Le profilage n'a pas servi à « découvrir les données » mais à **spécifier le pipeline sur preuves**. Trois résultats sont défendables tels quels devant un jury : une incohérence de données détectée par contrôle croisé (`unix_time`), une variable préconisée puis écartée sur mesure (la distance), et des largeurs de fenêtre justifiées par la cadence réelle observée plutôt que par convention.
