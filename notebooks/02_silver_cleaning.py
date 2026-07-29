# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Couche Silver : typage, qualité et conformité
# MAGIC
# MAGIC **Objectif** : transformer la copie brute en une table exploitable et
# MAGIC conforme, en traçant chaque décision.
# MAGIC
# MAGIC ## Ce que Silver fait réellement ici
# MAGIC
# MAGIC Le profilage (note 01) a montré que le corpus ne contient **aucune valeur
# MAGIC manquante, aucun doublon, aucun montant négatif**. Il n'y a donc rien à
# MAGIC « réparer » au sens habituel du data cleaning, et prétendre le contraire
# MAGIC serait malhonnête.
# MAGIC
# MAGIC Silver a donc trois responsabilités réelles, et une seule correspond à ce
# MAGIC qu'on appelle d'ordinaire du nettoyage :
# MAGIC
# MAGIC | | Responsabilité | Ce que ça produit |
# MAGIC |---|---|---|
# MAGIC | 1 | **Établir le contrat de données** | Bronze ne contient que du texte. Chaque colonne reçoit ici son type définitif. |
# MAGIC | 2 | **Contrôler et publier la qualité** | Les règles sont exécutées, comptées, et leur résultat publié — y compris quand elles ne rejettent rien. |
# MAGIC | 3 | **Assurer la conformité réglementaire** | Pseudonymisation du porteur, suppression des identifiants directs. |
# MAGIC
# MAGIC Une **réparation réelle** est malgré tout appliquée : la restauration des
# MAGIC zéros de tête des codes postaux, détruits en amont par un traitement
# MAGIC numérique. Elle concerne 6,28 % des lignes et sera mesurée plus bas.
# MAGIC
# MAGIC ## Le principe de la quarantaine
# MAGIC
# MAGIC Aucune ligne n'est supprimée silencieusement. Un `WHERE amount > 0` est
# MAGIC invisible : personne ne saura jamais combien de transactions ont disparu ni
# MAGIC pourquoi. Les lignes non conformes sont redirigées vers une table dédiée avec
# MAGIC le **motif exact** de leur rejet, ce qui transforme le taux de rejet en une
# MAGIC métrique mesurable et historisée.
# MAGIC
# MAGIC Deux sévérités sont distinguées :
# MAGIC
# MAGIC - **`REJECT`** — ligne structurellement inexploitable → quarantaine.
# MAGIC - **`FLAG`** — anomalie métier → ligne **conservée** et marquée.
# MAGIC
# MAGIC La distinction se défend : rejeter une ligne pour une anomalie métier
# MAGIC reviendrait à décider techniquement qu'une donnée métier n'existe pas. Un
# MAGIC porteur de 15 ans est peut-être un porteur secondaire légitime — ce n'est pas
# MAGIC à la couche technique de trancher.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.getcwd())
SRC_PATH = os.path.join(REPO_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from pyspark.sql import functions as F

from fraud import config, quality, silver

print(config.describe())

# COMMAND ----------

bronze = spark.table(config.BRONZE_TABLE)
bronze_count = bronze.count()

print(f"Lignes lues depuis Bronze : {bronze_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mesure préalable des réparations à effectuer
# MAGIC
# MAGIC Un code postal américain comporte **toujours 5 chiffres**. Le fichier source
# MAGIC en contient qui n'en ont que 4 : leur zéro de tête a été détruit en amont par
# MAGIC un traitement qui les a interprétés comme des nombres.
# MAGIC
# MAGIC La preuve est dans les états concernés — ce sont exactement ceux dont les
# MAGIC codes postaux commencent par `0` (Nouvelle-Angleterre et New Jersey). Ce n'est
# MAGIC donc pas une donnée manquante, c'est une **corruption systématique**.
# MAGIC
# MAGIC C'est aussi la confirmation empirique de la règle posée en Bronze : un
# MAGIC identifiant ne doit jamais être typé comme un nombre. Si Bronze avait chargé
# MAGIC `zip` en entier, la réparation serait ici impossible.

# COMMAND ----------

zip_diagnosis = (
    bronze.withColumn("zip_length", F.length(F.trim(F.col("zip"))))
    .groupBy("zip_length")
    .agg(
        F.count("*").alias("lignes"),
        F.countDistinct("zip").alias("codes_distincts"),
        F.sort_array(F.collect_set("state")).alias("etats"),
    )
    .orderBy("zip_length")
)
display(zip_diagnosis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Colonnes de validation
# MAGIC
# MAGIC Les conversions sont calculées **avant** l'application des règles, car les
# MAGIC règles portent précisément sur leur résultat : une valeur d'origine non vide
# MAGIC qui produit un `NULL` après conversion est un échec de parsing, et c'est
# MAGIC exactement ce qu'il faut détecter.
# MAGIC
# MAGIC Le rang de déduplication est calculé sur un tri **totalement déterministe**
# MAGIC (fichier source, puis index de ligne). Sans tri stable, deux exécutions
# MAGIC pourraient conserver des exemplaires différents d'un même doublon, et le
# MAGIC pipeline cesserait d'être reproductible.

# COMMAND ----------

validated = silver.add_validation_helpers(bronze)

reject_rules = silver.reject_rules()
flag_rules = silver.flag_rules()

print(f"Règles de rejet       : {len(reject_rules)}")
print(f"Règles de signalement : {len(flag_rules)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Rapport de qualité
# MAGIC
# MAGIC Les infractions sont comptées **en une seule passe** sur les 1,85 M de
# MAGIC lignes, et non une passe par règle.
# MAGIC
# MAGIC > **Ce rapport est un livrable, pas une trace de débogage.** Des contrôles
# MAGIC > qui ne rejettent rien ne sont pas des contrôles inutiles : ils *prouvent*
# MAGIC > la conformité au lieu de la supposer, et ils protégeront le pipeline le
# MAGIC > jour où la source changera. C'est ce tableau qui documente la section
# MAGIC > « qualité des données » du mémoire.

# COMMAND ----------

report = quality.violation_counts(validated, reject_rules + flag_rules)
print(quality.format_report(report, bronze_count))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Séparation conformes / quarantaine
# MAGIC
# MAGIC Une ligne peut enfreindre plusieurs règles à la fois. Toutes ses infractions
# MAGIC sont conservées dans `_rejection_reasons` : s'arrêter à la première priverait
# MAGIC l'analyse du diagnostic complet.

# COMMAND ----------

clean, quarantined = quality.split_on_quality(validated, reject_rules)
flagged = quality.apply_flags(clean, flag_rules)

clean_count = flagged.count()
quarantined_count = quarantined.count()

print(f"Lignes conformes   : {clean_count:,}")
print(f"Lignes en quarantaine : {quarantined_count:,}")
print(f"Taux de rejet      : {100 * quarantined_count / bronze_count:.4f} %")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Projection Silver : typage, renommage et pseudonymisation
# MAGIC
# MAGIC ### Colonnes supprimées, et pourquoi
# MAGIC
# MAGIC | Colonne | Motif |
# MAGIC |---|---|
# MAGIC | `source_row_index` | Index technique de l'export CSV, sans valeur métier |
# MAGIC | `first`, `last`, `street` | **Identifiants directs** — prénom, nom, adresse postale |
# MAGIC | `cc_num` | Remplacé par `customer_id`, pseudonymisé |
# MAGIC | `dob` | Donnée personnelle — remplacée par `customer_age`, non identifiant |
# MAGIC | `unix_time` | Incohérent avec l'horodatage (décalage de 7 ans) — recalculé |
# MAGIC
# MAGIC ### Pourquoi un hachage **salé**
# MAGIC
# MAGIC Sans sel, le haché d'un numéro de carte est reconstituable par force brute :
# MAGIC l'espace des numéros valides est contraint (préfixes émetteurs connus,
# MAGIC longueur fixe, clé de Luhn). Quiconque disposerait de la table pourrait
# MAGIC retrouver les numéros d'origine en testant les candidats. **Le hachage seul
# MAGIC n'est donc pas une pseudonymisation** au sens de l'article 4(5) du RGPD ;
# MAGIC avec un sel tenu séparément, il en devient une.
# MAGIC
# MAGIC ### Pourquoi un hachage plutôt qu'un identifiant séquentiel
# MAGIC
# MAGIC Le haché est **déterministe** : une même carte produit toujours le même
# MAGIC identifiant, entre deux exécutions comme entre les périodes d'entraînement et
# MAGIC de test. Cette stabilité est indispensable — sans elle, aucune variable
# MAGIC comportementale ne pourrait être calculée sur l'historique d'un porteur.

# COMMAND ----------

silver_df = silver.build_silver_projection(flagged, config.PSEUDONYM_SALT)

print(f"Colonnes Silver : {len(silver_df.columns)}")
silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Écriture des tables

# COMMAND ----------

(
    silver_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.SILVER_TABLE)
)

spark.sql(f"""
    COMMENT ON TABLE {config.SILVER_TABLE} IS
    'Couche Silver — transactions typées, contrôlées et pseudonymisées.
     Les identifiants directs du porteur (nom, prénom, adresse, numéro de carte,
     date de naissance) ont été supprimés ou remplacés par une empreinte salée.
     Une ligne présente ici a passé l''intégralité des règles de qualité ;
     les lignes rejetées sont dans silver_quarantine avec leur motif.'
""")

print(f"Table écrite : {config.SILVER_TABLE}  ({clean_count:,} lignes)")

# COMMAND ----------

# La quarantaine conserve les colonnes SOURCES, pas les colonnes typées : pour
# diagnostiquer un rejet, il faut voir la valeur telle qu'elle est arrivée, pas
# le NULL produit par une conversion qui a échoué.
quarantine_df = quarantined.select(
    *bronze.columns,
    quality.REJECTION_REASONS_COL,
    quality.QUARANTINED_AT_COL,
)

(
    quarantine_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.QUARANTINE_TABLE)
)

spark.sql(f"""
    COMMENT ON TABLE {config.QUARANTINE_TABLE} IS
    'Journal des lignes rejetées par les contrôles qualité de la couche Silver,
     avec le motif de rejet et les valeurs sources non transformées.
     Une table vide signifie que tous les contrôles sont passés — ce qui est
     une information, pas une absence d''information.'
""")

print(f"Table écrite : {config.QUARANTINE_TABLE}  ({quarantined_count:,} lignes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Dictionnaire de données — commentaires métier en français
# MAGIC
# MAGIC Les noms de colonnes sont en anglais (standard professionnel, cohérent avec
# MAGIC les noms de la source) mais **chaque colonne porte une description métier en
# MAGIC français**, posée dans Unity Catalog.
# MAGIC
# MAGIC Ces descriptions s'affichent directement dans le Catalog Explorer. Au-delà de
# MAGIC la question de la langue, un dictionnaire de données est un **livrable
# MAGIC attendu en contexte bancaire réglementé** : il documente la signification et
# MAGIC la provenance de chaque champ pour les auditeurs comme pour les analystes.

# COMMAND ----------

for statement in silver.column_comment_statements(
    config.SILVER_TABLE, silver.SILVER_COLUMN_COMMENTS
):
    spark.sql(statement)

print(f"{len(silver.SILVER_COLUMN_COMMENTS)} colonnes documentées en français.")
display(
    spark.sql(f"DESCRIBE TABLE {config.SILVER_TABLE}")
    .filter(F.col("col_name").isNotNull() & (F.col("col_name") != ""))
    .select("col_name", "data_type", "comment")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Contrôles de sortie
# MAGIC
# MAGIC Trois vérifications, dont chacune peut faire échouer le notebook.

# COMMAND ----------

errors = []

# --- Contrôle 1 : conservation du volume ---------------------------------
# Toute ligne entrée doit ressortir quelque part : conforme ou en quarantaine.
# Un écart signalerait une ligne perdue en route, le pire défaut possible.
if clean_count + quarantined_count != bronze_count:
    errors.append(
        f"conservation : {clean_count:,} + {quarantined_count:,} "
        f"!= {bronze_count:,} lignes Bronze"
    )
print(f"Conservation du volume : {clean_count:,} + {quarantined_count:,} = {bronze_count:,}")

# --- Contrôle 2 : aucun identifiant direct résiduel ----------------------
FORBIDDEN = {"cc_num", "first", "last", "street", "dob", "source_row_index", "unix_time"}
leaked = FORBIDDEN & set(spark.table(config.SILVER_TABLE).columns)
if leaked:
    errors.append(f"identifiants directs présents en Silver : {sorted(leaked)}")
print(f"Identifiants directs résiduels : {sorted(leaked) if leaked else 'aucun'}")

# --- Contrôle 3 : la pseudonymisation préserve la cardinalité ------------
# 999 porteurs distincts dans la source (profilage). Un nombre inférieur
# révélerait une collision de hachage, un nombre supérieur une instabilité.
silver_table = spark.table(config.SILVER_TABLE)
distinct_customers = silver_table.select("customer_id").distinct().count()
EXPECTED_CUSTOMERS = 999
if distinct_customers != EXPECTED_CUSTOMERS:
    errors.append(
        f"cardinalité des porteurs : {distinct_customers} au lieu de {EXPECTED_CUSTOMERS}"
    )
print(f"Porteurs distincts après pseudonymisation : {distinct_customers} (attendu {EXPECTED_CUSTOMERS})")

if errors:
    raise ValueError("Contrôles de sortie en échec :\n" + "\n".join(f"  - {e}" for e in errors))

print()
print("Tous les contrôles de sortie sont passés.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Vérification de la réparation des codes postaux

# COMMAND ----------

repaired = silver_table.filter(F.col("flag_zip_code_repaired")).count()
still_short = silver_table.filter(F.length("customer_zip") != 5).count()

print(f"Codes postaux réparés    : {repaired:,}  ({100 * repaired / clean_count:.2f} % des lignes)")
print(f"Codes postaux non conformes après réparation : {still_short}")

display(
    silver_table.filter(F.col("flag_zip_code_repaired"))
    .select("customer_zip", "customer_state", "customer_city")
    .distinct()
    .orderBy("customer_zip")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Aperçu de la table Silver

# COMMAND ----------

display(
    silver_table.select(
        "transaction_id", "event_time", "customer_id", "customer_age",
        "merchant_name", "merchant_category", "amount", "is_fraud",
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Table produite | `workspace.fraud_detection.silver_transactions` |
# MAGIC | Table de quarantaine | `workspace.fraud_detection.silver_quarantine` |
# MAGIC | Règles de rejet | 9 |
# MAGIC | Règles de signalement | 2 |
# MAGIC | Colonnes documentées en français | 26 |
# MAGIC | Identifiants directs supprimés | `first`, `last`, `street`, `cc_num`, `dob` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Points à savoir expliquer en soutenance
# MAGIC
# MAGIC 1. **« Vos contrôles ne rejettent rien, à quoi servent-ils ? »** À prouver la
# MAGIC    conformité plutôt qu'à la supposer, et à protéger le pipeline quand la
# MAGIC    source changera. Un taux de rejet de 0 % est un **résultat mesuré**, pas
# MAGIC    une absence de contrôle.
# MAGIC 2. **Pourquoi une quarantaine et pas un filtre ?** Un filtre est invisible.
# MAGIC    La quarantaine transforme le rejet en métrique historisée et diagnosticable.
# MAGIC 3. **Pourquoi signaler les porteurs mineurs au lieu de les rejeter ?** Parce
# MAGIC    que c'est une anomalie métier, pas technique. La ligne reste exploitable,
# MAGIC    et un mineur peut légitimement être porteur secondaire. Écarter les
# MAGIC    valeurs atypiques au nom du nettoyage reviendrait à supprimer le signal
# MAGIC    que le modèle doit apprendre.
# MAGIC 4. **Pourquoi saler le hachage ?** Sans sel, le haché d'un numéro de carte
# MAGIC    est reconstituable par force brute — l'espace des numéros valides est trop
# MAGIC    contraint. Le hachage nu ne suffit pas à constituer une pseudonymisation.
# MAGIC 5. **La réparation des codes postaux** est la démonstration empirique de la
# MAGIC    règle posée en Bronze : 6,28 % des lignes avaient perdu le zéro de tête de
# MAGIC    leur code postal, et les états concernés sont exactement ceux dont les ZIP
# MAGIC    commencent par 0. Si Bronze avait typé `zip` en entier, la réparation
# MAGIC    aurait été impossible.
# MAGIC
# MAGIC **Étape suivante** : `03_gold_features` — variables comportementales par
# MAGIC fenêtres glissantes, sous contrainte stricte d'absence de fuite temporelle.
