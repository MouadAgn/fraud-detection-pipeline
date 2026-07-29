# 03 — Couche Silver : qualité, typage et conformité

> **Étape projet** : 2 — Silver
> **Section mémoire visée** : Partie 2 — data cleaning et conformité · Partie 3 — bilan critique
> **Code** : [`notebooks/02_silver_cleaning.py`](../../notebooks/02_silver_cleaning.py) · [`src/fraud/silver.py`](../../src/fraud/silver.py) · [`src/fraud/quality.py`](../../src/fraud/quality.py)

---

## 1. Une redéfinition nécessaire : il n'y a rien à nettoyer

La méthodologie annonçait une étape de *data cleaning*. Le profilage (note [01](01_profilage_initial.md)) a établi que le corpus ne contient **aucune valeur manquante, aucun doublon, aucun montant négatif, aucun horodatage invalide**.

Deux attitudes étaient possibles. Écrire des règles de nettoyage décoratives qui n'auraient jamais rien corrigé, et laisser croire qu'elles servaient à quelque chose. Ou reconnaître le fait et redéfinir l'étape autour de ce qu'elle fait réellement. C'est la seconde qui a été retenue : une règle qui ne corrige rien mais qu'on présente comme corrective est un mensonge par omission, et un jury technique le repère.

La couche Silver a donc **trois responsabilités réelles** :

| | Responsabilité | Nature |
| --- | --- | --- |
| 1 | Établir le contrat de données (typage) | Transformation |
| 2 | Contrôler et publier la qualité | Mesure |
| 3 | Assurer la conformité réglementaire | Protection |

Une réparation authentique s'y ajoute néanmoins — la restauration des codes postaux (§ 5), qui n'avait pas été détectée au profilage initial et qui concerne 6,28 % des lignes.

## 2. Le typage comme contrat de données

Bronze ne contient que du texte, par construction (note [02](02_ingestion_bronze.md), § 2.2). Silver est l'endroit où chaque colonne reçoit son type définitif.

Ce n'est pas une formalité technique. Une conversion est le premier contrôle de qualité réel : une valeur qui refuse de se convertir est une valeur qui n'est pas ce qu'elle prétend être. Le pipeline exploite cette propriété, en séparant deux cas que Spark confondrait :

- une valeur **absente à la source** — légitime, à traiter comme telle ;
- une valeur **présente mais non convertible** — une anomalie qui doit être détectée.

Techniquement, la conversion est appliquée dans une colonne temporaire, puis la règle teste la conjonction « source renseignée **et** résultat nul ». Sans cette précaution, les deux cas produiraient le même `NULL` et deviendraient indiscernables.

## 3. Les règles de qualité

Onze règles, réparties en deux sévérités.

### 3.1 Règles de rejet — la ligne part en quarantaine

Elles portent toutes sur l'**exploitabilité structurelle** de la ligne. Aucune ne porte de jugement métier.

| Règle | Ce qu'elle détecte |
| --- | --- |
| `transaction_id_missing` | Identifiant de transaction absent ou vide — ligne non traçable |
| `transaction_id_duplicated` | Exemplaire surnuméraire d'une transaction déjà présente |
| `event_time_unparseable` | Horodatage illisible — sans date, ni fenêtre temporelle ni split possible |
| `amount_unparseable` | Montant non numérique |
| `amount_not_positive` | Montant négatif ou nul |
| `customer_key_missing` | Numéro de carte absent — ligne non rattachable à un porteur |
| `target_out_of_domain` | Variable cible hors du domaine {0, 1} — étiquette inexploitable |
| `customer_coordinates_invalid` | Coordonnées du porteur hors bornes géographiques |
| `merchant_coordinates_invalid` | Coordonnées du commerçant hors bornes géographiques |

**Ce qu'on s'interdit d'y mettre.** Aucune règle n'écarte une transaction pour son caractère atypique — montant élevé, heure inhabituelle, commerçant rare. Ce serait une faute méthodologique majeure : en détection de fraude, **la valeur aberrante est précisément le signal à apprendre**. Nettoyer les valeurs extrêmes reviendrait à supprimer les fraudes du jeu d'entraînement.

### 3.2 Règles de signalement — la ligne est conservée et marquée

| Règle | Ce qu'elle signale | Volume |
| --- | --- | ---: |
| `flag_minor_cardholder` | Porteur de moins de 18 ans au moment de la transaction | 21 847 lignes (1,18 %) |
| `flag_zip_code_repaired` | Code postal reçu sur 4 caractères, zéro de tête restauré | 116 389 lignes (6,28 %) |

> **Une divergence instructive sur le calcul de l'âge.** Le profilage initial annonçait 21 860 porteurs mineurs, le pipeline en a compté 21 847. L'écart de 13 lignes vient de la méthode : le profilage approximait l'âge par `(date − naissance) / 365,25`, là où la couche Silver utilise `months_between`, exact au calendrier. Les 13 lignes concernées sont des transactions survenues à quelques jours d'un dix-huitième anniversaire.
>
> L'écart est négligeable en volume, mais pas dans son principe : **un profilage dont les chiffres ne coïncident pas avec ceux du pipeline cesse de faire référence.** Le script de profilage a donc été aligné sur la sémantique exacte du pipeline, et c'est la valeur produite par l'exécution — 21 847 — qui fait foi dans ce mémoire. C'est aussi la raison pour laquelle chaque chiffre cité ici est réconcilié avec une sortie d'exécution, jamais avec une estimation.

**Pourquoi signaler plutôt que rejeter.** Ces anomalies sont **métier**, pas techniques. La ligne reste parfaitement exploitable ; c'est à l'analyse, et non à la couche technique, de décider quoi en faire. Un porteur de quinze ans est invraisemblable comme titulaire principal, mais parfaitement légitime comme porteur secondaire d'une carte familiale. Rejeter ces 21 847 lignes reviendrait à faire trancher par une couche technique une question qui relève du métier — et à perdre 1,18 % du corpus sur une supposition.

## 4. Le principe de la quarantaine

Un filtre du type `WHERE amount > 0` est **invisible**. Personne ne saura jamais combien de transactions ont disparu, ni pourquoi, ni à quel moment le taux de rejet s'est mis à dériver.

Les lignes non conformes sont donc redirigées vers une table `silver_quarantine`, accompagnées :

- du **motif exact** de leur rejet — sous forme de tableau, car une ligne peut enfreindre plusieurs règles à la fois, et s'arrêter à la première priverait le diagnostic de la moitié de l'information ;
- de leurs **valeurs sources non transformées** — pour diagnostiquer un rejet, il faut voir la valeur telle qu'elle est arrivée, pas le `NULL` produit par la conversion qui a échoué.

Le taux de rejet devient ainsi une **métrique mesurable et historisée**, et non un effet de bord silencieux.

### Le taux de rejet mesuré est de 0 %, et c'est un résultat

Les neuf règles de rejet n'écartent aucune ligne. L'exécution du pipeline sur les 1 852 394 transactions produit **0 ligne en quarantaine**, conformément à la prévision établie par le profilage.

Il faut savoir défendre ce point, car il sera interrogé. Des contrôles qui ne rejettent rien ne sont pas des contrôles inutiles :

1. **Ils prouvent la conformité au lieu de la supposer.** Sans eux, l'affirmation « les données sont propres » repose sur une inspection ponctuelle. Avec eux, elle repose sur un contrôle exécuté à chaque passage du pipeline.
2. **Ils protègent l'avenir.** Le jour où la source change — nouveau lot, nouveau fournisseur, changement de format — ils se déclenchent. C'est précisément le moment où l'on n'aurait pas pensé à les écrire.
3. **Ils rendent la qualité observable.** Un taux de rejet de 0 % qui devient 3 % la semaine suivante est une alerte. Sans mesure, la dégradation passe inaperçue jusqu'à ce qu'elle atteigne le modèle.

## 5. La réparation des codes postaux — un défaut réel, détecté tardivement

C'est le seul défaut de données authentique du corpus, et il n'était pas apparu au profilage initial.

### Le constat

Un code postal américain comporte **toujours cinq chiffres**. Le fichier source en contient qui n'en ont que quatre.

| Mesure | Valeur |
| --- | ---: |
| Lignes avec un code postal à 4 caractères | **116 389** (6,28 %) |
| Codes postaux distincts concernés | 58 |
| Codes postaux commençant par `0` dans la source | **0** |

### La preuve que c'est une corruption, et non une donnée manquante

Les états concernés sont exactement : **CT, MA, ME, NH, NJ, RI, VT**.

Ce sont précisément les états dont les codes postaux commencent par `0` — la Nouvelle-Angleterre et le New Jersey occupent les plages `010xx` à `089xx` du plan de numérotation américain. Aucun autre état n'est touché.

La conclusion est sans ambiguïté : un traitement en amont a interprété ces codes postaux comme des **nombres**, ce qui a détruit leur zéro de tête. `07208` est devenu `7208`.

### Ce que cela démontre

C'est la validation empirique de la règle posée en Bronze : **un identifiant ne doit jamais être typé comme un nombre**. L'argument avait été formulé de manière théorique à l'étape 1, à propos de `cc_num`. Le corpus en fournit la démonstration factuelle sur une autre colonne, et sur 6,28 % de ses lignes.

Corollaire décisif pour l'architecture : **si Bronze avait chargé `zip` en entier, la réparation serait ici impossible.** L'information aurait été détruite à l'ingestion, définitivement. C'est la justification la plus concrète du choix « tout en texte en Bronze » — et elle n'est pas théorique, elle est mesurée.

### La règle appliquée

Complétion à gauche par des zéros jusqu'à cinq caractères, et marquage de chaque ligne réparée par `flag_zip_code_repaired`. La réparation est tracée, donc auditable : on peut à tout moment savoir quelles lignes ont été modifiées par le pipeline et lesquelles sont d'origine.

## 6. Conformité : pseudonymisation et minimisation

Le jeu de données contient des colonnes qui, dans un système réel, seraient des données à caractère personnel directement identifiantes. Bien qu'elles soient ici entièrement synthétiques (note [00](00_contexte_et_donnees.md), § 4), le pipeline les traite comme si elles ne l'étaient pas.

### Colonnes supprimées

| Colonne | Motif |
| --- | --- |
| `first`, `last` | Identifiants directs — nom et prénom du porteur |
| `street` | Identifiant direct — adresse postale précise |
| `cc_num` | Remplacée par `customer_id`, pseudonymisé |
| `dob` | Donnée personnelle — remplacée par `customer_age`, non identifiant |
| `source_row_index` | Index technique de l'export, sans valeur métier |
| `unix_time` | Incohérente avec l'horodatage (décalage de 7 ans) — recalculée |

Le remplacement de la date de naissance par l'âge est une application directe du **principe de minimisation** (art. 5.1.c RGPD) : l'âge suffit à l'analyse, la date exacte de naissance ne serait qu'un identifiant supplémentaire.

### Pourquoi le hachage doit être salé

`customer_id` est l'empreinte **SHA-256 salée** du numéro de carte.

Sans sel, le haché d'un numéro de carte est reconstituable par force brute. L'espace des numéros valides est fortement contraint — préfixes émetteurs connus, longueur fixe, clé de Luhn — si bien que quiconque disposerait de la table pourrait retrouver les numéros d'origine en testant les candidats. **Le hachage seul ne constitue donc pas une pseudonymisation** au sens de l'article 4(5) du RGPD ; avec un sel tenu séparément, il en devient une.

*Limite assumée* : le sel figure en clair dans le dépôt. C'est acceptable ici — le corpus est intégralement synthétique, aucun numéro de carte réel n'est concerné. Sur des données de production, il devrait provenir d'un secret scope Databricks, faute de quoi la pseudonymisation serait réversible par toute personne ayant accès au code. Le pipeline prévoit cette substitution par variable d'environnement, sans modification de code.

### Pourquoi un hachage plutôt qu'un identifiant séquentiel

Le haché est **déterministe** : une même carte produit toujours le même identifiant, entre deux exécutions comme entre les périodes d'entraînement et de test. Cette stabilité est une contrainte fonctionnelle, pas une préférence esthétique — sans elle, aucune variable comportementale ne pourrait être calculée sur l'historique d'un porteur, et toute l'étape 3 s'effondrerait.

Un contrôle de sortie vérifie que la pseudonymisation préserve la cardinalité : **999 porteurs distincts** avant et après. Un nombre inférieur révélerait une collision de hachage, un nombre supérieur une instabilité du calcul.

## 7. Le dictionnaire de données

Les noms de colonnes sont en anglais — standard professionnel, et cohérent avec les noms de la source. La documentation fonctionnelle est en français.

Les deux sont réconciliés par un **commentaire posé sur chaque colonne dans Unity Catalog**, visible directement dans le Catalog Explorer, au plus près de la donnée. Ces mêmes descriptions alimentent [`docs/dictionnaire_donnees.md`](../dictionnaire_donnees.md).

Le document est **généré** depuis le code, jamais rédigé à la main : documenter les colonnes à deux endroits garantirait qu'ils divergeraient à la première modification. La source unique de vérité est `SILVER_DATA_DICTIONARY` dans `src/fraud/silver.py`.

Au-delà de la question linguistique, un dictionnaire de données est un **livrable attendu en contexte bancaire réglementé** : il documente la signification et la provenance de chaque champ, pour les auditeurs comme pour les analystes.

## 8. Contrôles de sortie

Trois vérifications font échouer le notebook en cas d'écart.

| Contrôle | Ce qu'il détecte |
| --- | --- |
| **Conservation du volume** — `conformes + quarantaine = lignes Bronze` | Une ligne perdue en route, le pire défaut possible : ni traitée, ni rejetée, simplement disparue |
| **Absence d'identifiant direct** en Silver | Une régression de conformité introduite par une modification ultérieure du code |
| **Cardinalité des porteurs** — 999 attendus | Une collision de hachage ou une instabilité de la pseudonymisation |

## 9. Résultat

| | |
| --- | --- |
| Table produite | `workspace.fraud_detection.silver_transactions` |
| Table de quarantaine | `workspace.fraud_detection.silver_quarantine` |
| Colonnes | 26, toutes typées et documentées en français |
| Règles de rejet | 9 |
| Règles de signalement | 2 |
| Lignes conformes | 1 852 394 |
| Taux de rejet mesuré | **0 %** — 0 ligne en quarantaine |
| Réparations appliquées | 116 389 codes postaux, 0 non conforme après réparation |
| Porteurs distincts après pseudonymisation | 999 (identique à la source) |
| Identifiants directs supprimés | 5 |

---

**À retenir pour la soutenance**
Trois points se défendent seuls. **Le taux de rejet de 0 % est un résultat, pas une absence de contrôle** — les règles prouvent la conformité et protégeront le pipeline quand la source changera. **La distinction rejet/signalement** évite qu'une couche technique tranche une question métier : les 21 847 transactions de porteurs mineurs sont marquées, pas supprimées. **La réparation des codes postaux** valide empiriquement la décision d'architecture prise en Bronze : 6,28 % des lignes avaient perdu leur zéro de tête, les états concernés sont exactement ceux dont les ZIP commencent par 0, et si Bronze avait typé cette colonne en nombre l'information serait détruite sans retour.
