
  
  create view "dev"."main"."3_6__dbt_tmp" as (
    WITH sequenced_consumption AS (
    -- Identifie les séquences d'augmentation de consommation
    SELECT
        CONCAT("Date", ' ', "Heure") AS "Date_Heure",  -- Combinaison de la date et de l'heure
        "Région",
        "consommation",
        ROW_NUMBER() OVER (PARTITION BY "Région" ORDER BY "Date", "Heure") AS row_num,
        CASE 
            WHEN "consommation" > LAG("consommation") OVER (PARTITION BY "Région" ORDER BY "Date", "Heure") THEN 0 
            ELSE 1 
        END AS is_new_sequence
    FROM "dev"."main"."clean"
    WHERE "consommation" IS NOT NULL
),
sequence_grouped AS (
    -- Crée les groupes de séquences continues
    SELECT 
        "Date_Heure",
        "Région",
        "consommation",
        row_num,
        SUM(is_new_sequence) OVER (PARTITION BY "Région" ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sequence_group
    FROM sequenced_consumption
),
sequence_details AS (
    -- Agrège les séquences par groupe et calcule la durée
    SELECT 
        MIN(CAST("Date_Heure" AS TIMESTAMP)) AS start_datetime,
        MAX(CAST("Date_Heure" AS TIMESTAMP)) AS end_datetime,
        "Région",
        ARRAY_AGG("consommation" ORDER BY "Date_Heure") AS sequence,
        COUNT(*) AS sequence_length
    FROM sequence_grouped
    GROUP BY "Région", sequence_group
    HAVING COUNT(*) > 1  -- Filtre les séquences de plus d'une valeur (augmentation continue)
),
ranked_sequences AS (
    -- Classe les séquences par longueur décroissante et par date de début pour gérer les égalités
    SELECT 
        start_datetime,
        end_datetime,
        "Région",
        sequence,
        sequence_length,
        ROW_NUMBER() OVER (PARTITION BY "Région" ORDER BY sequence_length DESC, start_datetime) AS rank
    FROM sequence_details
)
-- Sélectionne les trois plus longues séquences
SELECT 
    start_datetime AS "Date - Heure",
    -- Calcul de la durée manuellement (en heures, minutes, secondes)
    EXTRACT(HOUR FROM end_datetime - start_datetime) || ':' || 
    LPAD(CAST(EXTRACT(MINUTE FROM end_datetime - start_datetime) AS VARCHAR), 2, '0') || ':' || 
    LPAD(CAST(EXTRACT(SECOND FROM end_datetime - start_datetime) AS VARCHAR), 2, '0') AS "Durée (hh:mm:ss)",
    "Région",
    sequence AS "Séquence (MW*)",
    rank AS "Rang"
FROM ranked_sequences
WHERE rank <= 3
ORDER BY "Rang", "Région"
  );
