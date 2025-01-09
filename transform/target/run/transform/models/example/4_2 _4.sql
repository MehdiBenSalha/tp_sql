
  
  create view "dev"."main"."4_2 _4__dbt_tmp" as (
    WITH taux_de_couverture AS (
    SELECT
        "Date",
        "Code INSEE région",
        "Région",
        SUM(production_totale) AS production_totale_GWh,
        SUM(consommation) AS consommation_GWh,
        
        -- Calcul du TCO (Production / Consommation)
        (SUM(production_totale) / NULLIF(SUM(consommation), 0)) AS TCO
    FROM clean_temp
    GROUP BY
        "Date",
        "Code INSEE région",
        "Région"
)

SELECT * FROM taux_de_couverture
  );
