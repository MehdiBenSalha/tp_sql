WITH temperature_data AS (
    -- Chargement des données de température
    SELECT
        ID,
        Date,
        "Code INSEE région" AS code_insee_region,
        Région,
        "TMin (°C)" AS tmin,
        "TMax (°C)" AS tmax,
        "TMoy (°C)" AS tmoy
    FROM temperature  -- La table existante
),
clean_data_with_temp AS (
    -- Jointure entre les données nettoyées et les données de température
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
        t.tmin,
        t.tmax,
        t.tmoy
    FROM {{ ref('clean') }} c  -- Table clean, issue de DBT
    LEFT JOIN temperature_data t
        ON c.Date = t.Date
        AND c."Code INSEE région" = t.code_insee_region
)
-- Création de la table finale
SELECT
    "Code INSEE région",
    Région,
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
    tmin,
    tmax,
    tmoy
FROM clean_data_with_temp
