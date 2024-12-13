WITH yearly_renewable_production AS (
    SELECT
        EXTRACT(YEAR FROM "Date") AS year,
        SUM(COALESCE(eolien, 0) + 
            COALESCE(solaire, 0) + 
            COALESCE(hydraulique, 0) + 
            COALESCE(bioenergies, 0)) / 2000 AS production_renouvelable_totale_gwh -- Conversion MW → GWh
    FROM {{ ref('clean') }}
    WHERE EXTRACT(YEAR FROM "Date") BETWEEN 2013 AND 2022
    GROUP BY EXTRACT(YEAR FROM "Date")
),
daily_cumulative_consumption AS (
    SELECT
        EXTRACT(YEAR FROM "Date") AS year,
        "Date",
        SUM(COALESCE(consommation, 0)) / 2000 AS consommation_journaliere_gwh,
        SUM(SUM(COALESCE(consommation, 0)) / 2000) OVER (
            PARTITION BY EXTRACT(YEAR FROM "Date")
            ORDER BY "Date"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS consommation_cumulee_gwh -- Consommation cumulée en GWh
    FROM {{ ref('clean') }}
    WHERE EXTRACT(YEAR FROM "Date") BETWEEN 2013 AND 2022
    GROUP BY EXTRACT(YEAR FROM "Date"), "Date"
),
threshold_exceedance AS (
    SELECT
        dcc.year,
        dcc."Date",
        yrp.production_renouvelable_totale_gwh,
        dcc.consommation_cumulee_gwh
    FROM daily_cumulative_consumption dcc
    JOIN yearly_renewable_production yrp
        ON dcc.year = yrp.year
    WHERE dcc.consommation_cumulee_gwh >= yrp.production_renouvelable_totale_gwh
),
first_exceed_date AS (
    SELECT
        year,
        MIN("Date") AS date_depassement -- Première date où la consommation dépasse la production annuelle renouvelable
    FROM threshold_exceedance
    GROUP BY year
)
SELECT *
FROM first_exceed_date
ORDER BY year