WITH consommation_par_filiere AS (
    SELECT
        "Date",
        "Code INSEE région",
        "Région",
        SUM(eolien) AS Eolien_GWh,
        SUM(thermique) AS Thermique_GWh,
        SUM(bioenergies) AS Bioénergies_GWh,
        SUM(nucleaire) AS Nucléaire_GWh,
        SUM(solaire) AS Solaire_GWh,
        SUM(hydraulique) AS Hydraulique_GWh,
        SUM(pompage) AS Pompage_GWh,
        SUM(consommation) AS Consommation_GWh
    FROM clean_temp
    GROUP BY
        "Date",
        "Code INSEE région",
        "Région"
)

SELECT * FROM consommation_par_filiere