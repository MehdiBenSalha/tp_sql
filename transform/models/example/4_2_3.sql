WITH consommation_par_periode AS (
    SELECT
        -- Agrégation par jour
        "Date" AS jour,
        SUM(consommation) AS consommation_total_GWh,
        SUM(pompage) AS pompage_GWh,
        SUM(thermique) AS Thermique_GWh,
        SUM(nucleaire) AS Nucléaire_GWh,
        SUM(eolien) AS Eolien_GWh,
        SUM(solaire) AS Solaire_GWh,
        SUM(hydraulique) AS Hydraulique_GWh,
        SUM(bioenergies) AS Bioénergies_GWh,
        SUM(production_totale) AS Production_Totale_GWh,
        AVG(tmin) AS temperature_min_moyenne,
        AVG(tmax) AS temperature_max_moyenne,
        AVG(tmoy) AS temperature_avg_moyenne,
        MIN(tmin) AS temperature_min_min,
        MAX(tmax) AS temperature_max_max,
        
        -- Agrégation par mois
        strftime('%m', "Date") AS mois,
        
        -- Agrégation par année
        strftime('%Y', "Date") AS annee,
        
        -- Agrégation pour toute la période
        'Toute la période' AS periode_totale
        
    FROM clean_temp
    GROUP BY
        "Date",  -- Jour
        mois,    -- Mois
        annee,   -- Année
        periode_totale  -- Toute la période
)

SELECT * FROM consommation_par_periode
