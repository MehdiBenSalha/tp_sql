
  
  create view "dev"."main"."4_2_2__dbt_tmp" as (
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
        SUM(pompage) AS pompage_GWh,
        SUM(thermique) AS Thermique_GWh,
        SUM(nucleaire) AS Nucléaire_GWh,
        SUM(eolien) AS Eolien_GWh,
        SUM(solaire) AS Solaire_GWh,
        SUM(hydraulique) AS Hydraulique_GWh,
        SUM(bioenergies) AS Bioénergies_GWh,
        SUM(production_totale) AS Production_Totale_GWh,
        -- Moyenne, minimum et maximum des températures par saison et région
        AVG(tmin) AS temperature_min_moyenne,
        AVG(tmax) AS temperature_max_moyenne,
        AVG(tmoy) AS temperature_avg_moyenne,
        MIN(tmin) AS temperature_min_min,
        MAX(tmax) AS temperature_max_max,
        
        -- Classification des températures par intervalle
        CASE
            WHEN tmin < 0 THEN 'Glacial'
            WHEN tmin >= 0 AND tmin < 8 THEN 'Froid'
            WHEN tmin >= 8 AND tmin < 17 THEN 'Modéré'
            WHEN tmin >= 17 AND tmin < 25 THEN 'Idéal'
            WHEN tmin >= 25 AND tmin < 33 THEN 'Chaud'
            ELSE 'Extrême'
        END AS temperature_intervalle
    FROM clean_temp
    GROUP BY
        saison,
        "Code INSEE région",
        "Région",
        tmin -- Ajouter tmin au GROUP BY
)

SELECT * FROM consommation_par_saison
  );
