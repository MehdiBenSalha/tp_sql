WITH data_clean AS (
    SELECT *,
           -- Traitement de la colonne "Consommation (MW)" : remplacer les vides et 'ND' par 0
           CASE
               WHEN "Consommation (MW)" = '' OR "Consommation (MW)" = 'ND' OR "Consommation (MW)" IS NULL THEN 0
               ELSE TRY_CAST("Consommation (MW)" AS INT)
           END AS "Consommation (MW)",
           
           -- Traitement de la colonne "Eolien (MW)" : remplacer 'ND' par 0
           CASE
               WHEN "Eolien (MW)" = 'ND' OR "Eolien (MW)" IS NULL THEN 0
               ELSE TRY_CAST("Eolien (MW)" AS INT)
           END AS "Eolien (MW)",
           
           -- Traitement de la colonne "Thermique (MW)" : remplacer les vides et 'ND' par 0
           CASE
               WHEN "Thermique (MW)" = '' OR "Thermique (MW)" = 'ND' OR "Thermique (MW)" IS NULL THEN 0
               ELSE TRY_CAST("Thermique (MW)" AS INT)
           END AS "Thermique (MW)",

           -- Traitement de la colonne "Bioénergies (MW)" : remplacer les vides et 'ND' par 0
           CASE
               WHEN "Bioénergies (MW)" = '' OR "Bioénergies (MW)" = 'ND' OR "Bioénergies (MW)" IS NULL THEN 0
               ELSE TRY_CAST("Bioénergies (MW)" AS INT)
           END AS "Bioénergies (MW)",

           -- Traitement des autres colonnes : remplacer les vides par 0
           COALESCE(TRY_CAST("Nucléaire (MW)" AS INT), 0) AS "Nucléaire (MW)",
           COALESCE(TRY_CAST("Solaire (MW)" AS INT), 0) AS "Solaire (MW)",
           COALESCE(TRY_CAST("Hydraulique (MW)" AS INT), 0) AS "Hydraulique (MW)",
           COALESCE(TRY_CAST("Pompage (MW)" AS INT), 0) AS "Pompage (MW)",
           COALESCE(TRY_CAST("Ech. physiques (MW)" AS INT), 0) AS "Ech. physiques (MW)"
    FROM eco2
    WHERE "Région" IS NOT NULL AND "Date" IS NOT NULL
),
clean AS (
    SELECT *, 
           -- Calcul de la production totale en sommant les valeurs traitées (assurant que toutes les valeurs sont numériques)
           COALESCE("Nucléaire (MW)", 0) + 
           COALESCE("Eolien (MW)", 0) +
           COALESCE("Solaire (MW)", 0) +
           COALESCE("Hydraulique (MW)", 0) +
           COALESCE("Thermique (MW)", 0) AS production_totale
    FROM data_clean
)
SELECT 
    "Code INSEE région",
    "Région", 
    "Date", 
    "Heure", 
    "Consommation (MW)",
    "Thermique (MW)",
    "Nucléaire (MW)",
    "Eolien (MW)",
    "Solaire (MW)",
    "Hydraulique (MW)",
    "Pompage (MW)",
    "Bioénergies (MW)",
    "Ech. physiques (MW)",
    production_totale
FROM clean