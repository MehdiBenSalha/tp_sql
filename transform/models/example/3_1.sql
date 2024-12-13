SELECT 
    strftime('%Y-%m', Date) AS mois,  -- Utilisation de strftime pour extraire l'année et le mois
    "Région", 
    SUM(COALESCE(TRY_CAST("consommation" AS INT), 0))/2000 AS consommation_totale_en_GWH,
    SUM(COALESCE(production_totale, 0))/2000 AS production_totale_en_GWH,

    MIN(COALESCE(TRY_CAST("consommation" AS INT), 0)) AS min_consommation_totale,
    MIN(COALESCE(production_totale, 0)) AS min_production_totale,

    MAX(COALESCE(TRY_CAST("consommation" AS INT), 0)) AS max_consommation_totale,
    MAX(COALESCE(production_totale, 0)) AS max_production_totale,
    
    AVG(COALESCE(TRY_CAST("consommation" AS INT), 0)) AS moyenne_consommation_totale,

    AVG(COALESCE(production_totale, 0)) AS moyenne_production_totale
   from  {{ ref('clean') }}
GROUP BY strftime('%Y-%m', Date), "Région"  -- Remplacer TO_CHAR par strftime
ORDER BY mois, "Région"  -- Tri par mois et région
