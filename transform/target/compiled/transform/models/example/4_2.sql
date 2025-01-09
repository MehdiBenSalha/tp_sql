WITH consommation_par_saison AS (
    SELECT
        -- Identifier la saison
        CASE
            WHEN strftime('%m', "Date") IN ('12', '01', '02') THEN 'Hiver'
            WHEN strftime('%m', "Date") IN ('03', '04', '05') THEN 'Printemps'
            WHEN strftime('%m', "Date") IN ('06', '07', '08') THEN 'Eté'
            ELSE 'Automne'
        END AS saison,
        -- Région et code INSEE
        "Code INSEE région" AS region_code,
        "Région",
        -- Sommes des consommations par type
        SUM(consommation) AS consommation_total_GWh,
        SUM(eolien) AS eolien_GWh,
        SUM(thermique) AS thermique_GWh,
        SUM(bioenergies) AS bioenergies_GWh,
        SUM(nucleaire) AS nucleaire_GWh,
        SUM(solaire) AS solaire_GWh,
        SUM(hydraulique) AS hydraulique_GWh,
        SUM(pompage) AS pompage_GWh,
        SUM(production_totale) AS production_totale_GWh,
        -- Moyenne, minimum et maximum des températures par saison et région
        AVG(tmin) AS temperature_min_moyenne,
        AVG(tmax) AS temperature_max_moyenne,
        AVG(tmoy) AS temperature_avg_moyenne,
        MIN(tmin) AS temperature_min_min,
        MAX(tmax) AS temperature_max_max
    FROM clean_temp
    GROUP BY
        saison,
        "Code INSEE région",
        "Région"
)
SELECT * FROM consommation_par_saison