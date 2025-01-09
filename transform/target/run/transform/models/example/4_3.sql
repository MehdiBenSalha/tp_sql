
  
  create view "dev"."main"."4_3__dbt_tmp" as (
    WITH temperature_interval AS (
    SELECT 
        CASE
            WHEN tmin < 0 THEN 'Glacial'
            WHEN tmin >= 0 AND tmin < 8 THEN 'Froid'
            WHEN tmin >= 8 AND tmin < 17 THEN 'Modéré'
            WHEN tmin >= 17 AND tmin < 25 THEN 'Idéal'
            WHEN tmin >= 25 AND tmin < 33 THEN 'Chaud'
            ELSE 'Extrême'
        END AS temperature_intervalle,
        "Date",
        "Heure",
        "Code INSEE région",
        "Région",
        consommation,
        eolien,
        thermique,
        bioenergies,
        nucleaire,
        solaire,
        hydraulique,
        pompage,
        production_totale
    FROM clean_temp
),
quart_de_jour AS (
    SELECT 
        ti."Code INSEE région",
        ti."Région",
        ti."Date",
        ti."Heure",
        CASE 
            WHEN strftime('%H', CAST('2000-01-01 ' || ti."Heure" AS TIMESTAMP)) BETWEEN '00' AND '06' THEN 'Nuit'
            WHEN strftime('%H', CAST('2000-01-01 ' || ti."Heure" AS TIMESTAMP)) BETWEEN '07' AND '12' THEN 'Matin'
            WHEN strftime('%H', CAST('2000-01-01 ' || ti."Heure" AS TIMESTAMP)) BETWEEN '13' AND '18' THEN 'Après-midi'
            ELSE 'Soir'
        END AS quart,
        ti.temperature_intervalle,
        ti.consommation,
        ti.eolien,
        ti.thermique,
        ti.bioenergies,
        ti.nucleaire,
        ti.solaire,
        ti.hydraulique,
        ti.pompage,
        ti.production_totale
    FROM temperature_interval ti
),
cuboide_par_mois_quart_temp AS (
    SELECT 
        strftime('%Y-%m', q."Date") AS mois,
        q.quart,
        q.temperature_intervalle,
        SUM(q.consommation) AS consommation_total_GWh,
        SUM(q.eolien) AS Eolien_GWh,
        SUM(q.thermique) AS Thermique_GWh,
        SUM(q.bioenergies) AS Bioénergies_GWh,
        SUM(q.nucleaire) AS Nucléaire_GWh,
        SUM(q.solaire) AS Solaire_GWh,
        SUM(q.hydraulique) AS Hydraulique_GWh,
        SUM(q.pompage) AS Pompage_GWh,
        SUM(q.production_totale) AS Production_Totale_GWh
    FROM quart_de_jour q
    GROUP BY
        mois, 
        q.quart,
        q.temperature_intervalle
)
SELECT * FROM cuboide_par_mois_quart_temp
  );
