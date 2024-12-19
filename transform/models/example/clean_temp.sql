WITH temperature_data AS (
    -- Chargement des données de température et sélection des informations nécessaires
    SELECT
        ID,
        Date,
        "Code INSEE région" AS code_insee_region,  -- Correction ici : changement de nom de la colonne
        Région,
        "TMin (°C)" AS tmin,
        "TMax (°C)" AS tmax,
        "TMoy (°C)" AS tmoy
    FROM {{ ref('temp') }}  -- Utilisation de la référence DBT pour la table temp
),
clean_data_with_temp AS (
    -- Jointure entre la table clean et les données de température
    SELECT
        c."Code INSEE région",
        c.Région,
        c.Date,
        c.Heure,
        c.consommation,
        c.eolien,
        c.thermique,
        c.bioenergies,
        c.nucleaire,
        c.solaire,
        c.hydraulique,
        c.pompage,
        c.ech_physiques,
        c.production_totale,
        t.tmin,  -- Utilisation de tmin ici
        t.tmax,  -- Utilisation de tmax ici
        t.tmoy   -- Utilisation de tmoy ici
    FROM {{ ref('clean') }} c
    LEFT JOIN temperature_data t
        ON c.Date = t.Date
        AND c."Code INSEE région" = t.code_insee_region  -- Correction : assurez-vous que les noms correspondent
)
-- Création d'une table finale avec les données intégrées
SELECT
    "Code INSEE région",
    "Région",
    Date,
    Heure,
    consommation,
    eolien,
    thermique,
    bioenergies,
    nucleaire,
    solaire,
    hydraulique,
    pompage,
    ech_physiques,
    production_totale,
    tmin,  -- Utilisation de tmin ici
    tmax,  -- Utilisation de tmax ici
    tmoy   -- Utilisation de tmoy ici
FROM clean_data_with_temp
