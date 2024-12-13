WITH data_clean AS (
    SELECT 
        *,
        COALESCE(TRY_CAST("Consommation (MW)" AS INT), 0) AS consommation,
        COALESCE(TRY_CAST("Eolien (MW)" AS INT), 0) AS eolien,
        COALESCE(TRY_CAST("Thermique (MW)" AS INT), 0) AS thermique,
        COALESCE(TRY_CAST("Bioénergies (MW)" AS INT), 0) AS bioenergies,
        COALESCE(TRY_CAST("Nucléaire (MW)" AS INT), 0) AS nucleaire,
        COALESCE(TRY_CAST("Solaire (MW)" AS INT), 0) AS solaire,
        COALESCE(TRY_CAST("Hydraulique (MW)" AS INT), 0) AS hydraulique,
        COALESCE(TRY_CAST("Pompage (MW)" AS INT), 0) AS pompage,
        COALESCE(TRY_CAST("Ech. physiques (MW)" AS INT), 0) AS ech_physiques
    FROM eco2
    WHERE "Région" IS NOT NULL AND "Date" IS NOT NULL
),
data_final AS (
    SELECT 
        "Code INSEE région",
        "Région", 
        "Date", 
        "Heure",
        consommation,
        eolien,
        thermique,
        bioenergies,
        nucleaire,
        solaire,
        hydraulique,
        pompage,
        ech_physiques,
        COALESCE(nucleaire, 0) + 
        COALESCE(eolien, 0) +
        COALESCE(solaire, 0) +
        COALESCE(hydraulique, 0) +
        COALESCE(thermique, 0) AS production_totale
    FROM data_clean
)

SELECT * FROM data_final
