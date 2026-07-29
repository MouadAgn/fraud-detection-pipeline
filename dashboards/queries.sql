-- ===========================================================================
-- Tableau de bord — Détection de fraude
-- ===========================================================================
-- Source : workspace.fraud_detection.gold_scored_transactions
--
-- RÈGLE APPLIQUÉE À TOUTES LES REQUÊTES : `WHERE split = 'test'`
--
-- Le modèle a vu les transactions d'entraînement. Les métriques calculées
-- dessus mesurent sa capacité à mémoriser, pas à détecter — elles sont
-- optimistes par construction. Un tableau de bord agrégeant les trois périodes
-- afficherait un taux de faux positifs flatteur et faux, sans produire le
-- moindre message d'erreur.
--
-- Seul le bloc de test — 555 719 transactions du dernier semestre 2020, jamais
-- vues par le modèle — mesure une performance honnête.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. INDICATEURS CLÉS
-- ---------------------------------------------------------------------------
-- Destinés aux vignettes en haut du tableau de bord.
--
-- Le taux de faux positifs se rapporte aux transactions LÉGITIMES, non au
-- total : rapporté au total, il serait dilué par la classe majoritaire et
-- paraîtrait systématiquement meilleur qu'il ne l'est.
--
-- `alertes_par_fraude` traduit ce taux en charge de travail réelle. C'est le
-- seul indicateur que les équipes d'exploitation lisent spontanément : un
-- dispositif exigeant vingt vérifications par fraude trouvée est rejeté,
-- quelle que soit sa performance statistique.
SELECT
    COUNT(*)                                                       AS transactions,
    SUM(is_fraud)                                                  AS fraudes_reelles,
    SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)                    AS alertes_emises,
    SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)   AS fraudes_detectees,
    SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END)   AS faux_positifs,
    SUM(CASE WHEN NOT is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END) AS fraudes_manquees,

    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)
          / NULLIF(SUM(is_fraud), 0), 2)                           AS rappel_pct,
    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END), 0), 2) AS precision_pct,

    -- Le KPI central du mémoire.
    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END), 0), 4) AS taux_faux_positifs_pct,

    ROUND(1.0 * SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END), 0), 2)
                                                                   AS alertes_par_fraude
FROM workspace.fraud_detection.gold_scored_transactions
WHERE split = 'test';


-- ---------------------------------------------------------------------------
-- 2. ÉVOLUTION HEBDOMADAIRE DU TAUX DE FAUX POSITIFS
-- ---------------------------------------------------------------------------
-- LE graphique de la problématique. À représenter en courbe.
--
-- Un taux moyen ne dit rien de sa stabilité. C'est la dérive qui compte : un
-- taux de 0,1 % qui devient 0,4 % en trois semaines signale que la population
-- a changé et que le modèle doit être réentraîné — information invisible sur
-- un indicateur agrégé.
SELECT
    DATE_TRUNC('WEEK', event_date)                                 AS semaine,
    COUNT(*)                                                       AS transactions,
    SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END)   AS faux_positifs,
    SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)   AS fraudes_detectees,
    SUM(CASE WHEN NOT is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END) AS fraudes_manquees,

    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END), 0), 4) AS taux_faux_positifs_pct,
    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)
          / NULLIF(SUM(is_fraud), 0), 2)                           AS rappel_pct
FROM workspace.fraud_detection.gold_scored_transactions
WHERE split = 'test'
GROUP BY DATE_TRUNC('WEEK', event_date)
ORDER BY semaine;


-- ---------------------------------------------------------------------------
-- 3. FILE D'ALERTES À TRAITER
-- ---------------------------------------------------------------------------
-- La vue qu'un analyste ouvrirait le matin. Triée par score décroissant : le
-- plus suspect d'abord, pour que le temps disponible aille aux cas qui le
-- méritent.
--
-- Les colonnes de contexte ne sont pas décoratives : elles répondent à
-- « pourquoi cette transaction est-elle remontée ? ». Un analyste qui ne voit
-- qu'un score ne peut ni décider ni contester.
--
-- `is_fraud` n'existerait évidemment pas en production — la vérité n'est connue
-- qu'après enquête. Elle figure ici pour évaluer la qualité de la file.
SELECT
    event_time                                                     AS horodatage,
    ROUND(amount, 2)                                               AS montant,
    merchant_category                                              AS categorie,
    merchant_name                                                  AS commercant,
    ROUND(amount_zscore_vs_history, 2)                             AS ecart_habitudes_client,
    txn_count_1h                                                   AS txn_derniere_heure,
    is_night                                                       AS transaction_nocturne,
    ROUND(fraud_score, 4)                                          AS score,
    CASE WHEN is_fraud = 1 THEN 'Fraude confirmée'
         ELSE 'Fausse alerte' END                                  AS issue
FROM workspace.fraud_detection.gold_scored_transactions
WHERE split = 'test'
  AND is_flagged
ORDER BY fraud_score DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- 4. OÙ SE CONCENTRENT LES FAUX POSITIFS
-- ---------------------------------------------------------------------------
-- Le taux global ne dit pas d'où viennent les fausses alertes. Cette
-- ventilation par catégorie ouvre une piste d'amélioration concrète : si une
-- catégorie concentre les faux positifs, elle mérite soit une variable
-- dédiée, soit un seuil qui lui soit propre.
--
-- C'est le passage de « le taux est de 0,11 % » à « voilà où agir ».
SELECT
    merchant_category                                              AS categorie,
    COUNT(*)                                                       AS transactions,
    SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END)   AS faux_positifs,
    SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)   AS fraudes_detectees,

    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END), 0), 4) AS taux_faux_positifs_pct,
    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END), 0), 2) AS precision_pct,

    ROUND(AVG(CASE WHEN is_flagged AND is_fraud = 0 THEN amount END), 2) AS montant_moyen_faux_positif
FROM workspace.fraud_detection.gold_scored_transactions
WHERE split = 'test'
GROUP BY merchant_category
ORDER BY faux_positifs DESC;


-- ---------------------------------------------------------------------------
-- 5. IMPACT FINANCIER
-- ---------------------------------------------------------------------------
-- Traduit la performance en euros, seule unité que comprend un comité de
-- direction. Un rappel de 95 % ne signifie rien pour lui ; « 1,2 M$ de fraude
-- interceptée » si.
--
-- Les deux montants ne se compensent pas : le montant intercepté est un gain,
-- le montant manqué une perte subie. Leur rapport mesure l'efficacité
-- économique du dispositif.
SELECT
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 2)   AS fraude_totale_tentee,
    ROUND(SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN amount ELSE 0 END), 2) AS fraude_interceptee,
    ROUND(SUM(CASE WHEN NOT is_flagged AND is_fraud = 1 THEN amount ELSE 0 END), 2) AS fraude_subie,

    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN amount ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 0), 2)
                                                                   AS montant_intercepte_pct,

    -- Coût des vérifications inutiles, à l'hypothèse retenue de 5 $ par alerte.
    ROUND(5.0 * SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END), 2) AS cout_fausses_alertes
FROM workspace.fraud_detection.gold_scored_transactions
WHERE split = 'test';


-- ---------------------------------------------------------------------------
-- 6. DISTRIBUTION DES SCORES ET POSITION DU SEUIL
-- ---------------------------------------------------------------------------
-- À représenter en histogramme empilé (fraudes / légitimes par tranche).
--
-- Rend visible ce qu'aucune métrique ne montre : la séparation des deux
-- populations, et l'endroit exact où le seuil vient couper. C'est le
-- graphique qui explique le mieux, devant un public non technique, ce que
-- « choisir un seuil » veut dire.
SELECT
    CASE
        WHEN fraud_score < 0.01  THEN '0,00 – 0,01'
        WHEN fraud_score < 0.038 THEN '0,01 – 0,038  (sous le seuil)'
        WHEN fraud_score < 0.10  THEN '0,038 – 0,10  (alerte)'
        WHEN fraud_score < 0.50  THEN '0,10 – 0,50'
        WHEN fraud_score < 0.90  THEN '0,50 – 0,90'
        ELSE                          '0,90 – 1,00'
    END                                                            AS tranche_de_score,
    SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END)                  AS transactions_legitimes,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)                  AS fraudes,
    MIN(fraud_score)                                               AS borne_min
FROM workspace.fraud_detection.gold_scored_transactions
WHERE split = 'test'
GROUP BY tranche_de_score
ORDER BY borne_min;


-- ---------------------------------------------------------------------------
-- 7. POURQUOI SEUL LE BLOC DE TEST COMPTE
-- ---------------------------------------------------------------------------
-- Requête de contrôle méthodologique, à conserver dans le tableau de bord.
--
-- Elle rend visible l'écart entre les périodes vues et non vues par le modèle,
-- et justifie le filtre appliqué partout ailleurs. Un lecteur qui verrait des
-- métriques sans savoir sur quoi elles portent pourrait légitimement les
-- soupçonner d'être choisies pour flatter.
SELECT
    split                                                          AS periode,
    MIN(event_date)                                                AS debut,
    MAX(event_date)                                                AS fin,
    COUNT(*)                                                       AS transactions,
    SUM(is_fraud)                                                  AS fraudes,
    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)
          / NULLIF(SUM(is_fraud), 0), 2)                           AS rappel_pct,
    ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END), 0), 4) AS taux_faux_positifs_pct,
    CASE split
        WHEN 'train'      THEN 'Vue par le modèle — métriques optimistes'
        WHEN 'validation' THEN 'Ayant servi au choix du modèle et du seuil'
        WHEN 'test'       THEN 'Jamais vue — seule mesure honnête'
    END                                                            AS interpretation
FROM workspace.fraud_detection.gold_scored_transactions
GROUP BY split
ORDER BY debut;
