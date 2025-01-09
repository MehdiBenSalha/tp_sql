WITH raw_data AS (
    SELECT 
        *
    FROM read_csv_auto('/home/ETUDIANT/e24a518u/Bureau/tp sql/transform/temperature-quotidienne-regionale.csv')
),
renamed_columns AS (
    SELECT
        ID,
        Date,
        "Code INSEE région" AS code_insee_region,
        Région,
        "TMin (°C)" AS tmin,
        "TMax (°C)" AS tmax,
        "TMoy (°C)" AS tmoy
    FROM raw_data
)