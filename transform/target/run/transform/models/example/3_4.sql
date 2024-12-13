
  
  create view "dev"."main"."3_4__dbt_tmp" as (
    WITH daily_consumption AS (
    SELECT 
        "Région",
        "Date",
        SUM(COALESCE(consommation, 0)) / 2000 AS consommation_gwh  -- Conversion MW → GWh
    FROM "dev"."main"."clean"
    GROUP BY "Région", "Date"
),
daily_variation AS (
    SELECT 
        "Région",
        "Date",
        consommation_gwh,
        consommation_gwh - LAG(consommation_gwh) OVER (
            PARTITION BY "Région"
            ORDER BY "Date"
        ) AS variation_gwh,
        CASE 
            WHEN LAG(consommation_gwh) OVER (
                PARTITION BY "Région"
                ORDER BY "Date"
            ) IS NOT NULL AND LAG(consommation_gwh) OVER (
                PARTITION BY "Région"
                ORDER BY "Date"
            ) != 0 
            THEN (consommation_gwh - LAG(consommation_gwh) OVER (
                    PARTITION BY "Région"
                    ORDER BY "Date"
                )) / LAG(consommation_gwh) OVER (
                    PARTITION BY "Région"
                    ORDER BY "Date"
                ) * 100
            ELSE NULL
        END AS taux_variation_pct
    FROM daily_consumption
),
top_variations AS (
    SELECT 
        "Région",
        "Date",
        consommation_gwh,
        variation_gwh,
        taux_variation_pct
    FROM daily_variation
    WHERE variation_gwh IS NOT NULL  -- Ignore les premières lignes de chaque région
    ORDER BY ABS(variation_gwh) DESC  -- Trier par écart absolu décroissant
    LIMIT 20
)
SELECT *
FROM top_variations
  );
