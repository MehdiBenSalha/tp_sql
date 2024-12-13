WITH data_with_consumption AS (
    SELECT 
        "Code INSEE région",
        "Région", 
        "Date", 
        consommation,  -- Consommation en MW
        -- Calcul de la consommation cumulée en GWh pour chaque jour du mois écoulé
        SUM(COALESCE(consommation, 0)) OVER (
            PARTITION BY "Région", EXTRACT(YEAR FROM "Date"), EXTRACT(MONTH FROM "Date")
            ORDER BY "Date"
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / 2000 AS consommation_gwh  -- Conversion de la consommation en GWh
    FROM clean
)

SELECT 
    "Code INSEE région", 
    "Région", 
    "Date", 
    consommation, 
    consommation_gwh
FROM data_with_consumption
ORDER BY "Région", "Date"
