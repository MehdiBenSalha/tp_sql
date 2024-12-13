WITH data_clean AS (
    SELECT *
    from clean
          
)
SELECT 
    "Date", 
    "Région", 
    SUM(COALESCE(TRY_CAST("consommation" AS INT), 0)) AS consommation_totale,
    SUM(COALESCE(TRY_CAST("thermique" AS INT), 0)) AS thermique_total,
    SUM(COALESCE(TRY_CAST("bioenergies" AS INT), 0)) AS bioenergies_total,
    SUM(COALESCE(TRY_CAST("eolien" AS INT), 0)) AS eolien_total,
    SUM(COALESCE(TRY_CAST("nucleaire" AS INT), 0)) AS nucleaire_total,
    SUM(COALESCE(TRY_CAST("solaire" AS INT), 0)) AS solaire_total,
    SUM(COALESCE(TRY_CAST("hydraulique" AS INT), 0)) AS hydraulique_total,
    SUM(COALESCE(TRY_CAST("pompage" AS INT), 0)) AS pompage_total,
    SUM(COALESCE(TRY_CAST("ech_physiques" AS INT), 0)) AS ech_physiques_total,
    SUM(COALESCE(TRY_CAST("production_totale" AS INT), 0)) AS production_totale
   
FROM data_clean
GROUP BY "Date", "Région"
ORDER BY "Date", "Région"