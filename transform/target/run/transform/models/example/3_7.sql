
  
  create view "dev"."main"."3_7__dbt_tmp" as (
    SELECT
    "Date",
    strftime('%Y-%m', "Date") AS mois,
    EXTRACT(YEAR FROM "Date") AS annee,
    "Région",
    CASE 
        WHEN "Région" IN ('Normandie','Pays de la Loire','Bretagne', 'Centre-Val de Loire') THEN 'NO'
        WHEN "Région" IN ('Grand Est', 'Bourgogne-Franche-Comté','Hauts-de-France') THEN 'NE'
        WHEN "Région" IN ('Auvergne-Rhône-Alpes', 'Provence-Alpes-Côte d''Azur') THEN 'SE'
        WHEN "Région" IN ('Occitanie', 'Nouvelle-Aquitaine') THEN 'SO'
        ELSE 'IdF' -- Pour les régions spécifiques à chaque zone géographique
    END AS zone,
    SUM(COALESCE(TRY_CAST("consommation" AS INT), 0)) / 2000 AS consommation_en_GWH
FROM 
    "dev"."main"."clean"
WHERE 
    "Région" IS NOT NULL
    AND "Date" IS NOT NULL
    AND "Région" != 'Corse'  -- Exclure la Corse
GROUP BY CUBE(
    "Date", 
    strftime('%Y-%m', "Date"), 
    EXTRACT(YEAR FROM "Date"), 
    "Région", 
    CASE 
        WHEN "Région" IN ('Normandie','Pays de la Loire','Bretagne', 'Centre-Val de Loire') THEN 'NO'
        WHEN "Région" IN ('Grand Est', 'Bourgogne-Franche-Comté','Hauts-de-France') THEN 'NE'
        WHEN "Région" IN ('Auvergne-Rhône-Alpes', 'Provence-Alpes-Côte d''Azur') THEN 'SE'
        WHEN "Région" IN ('Occitanie', 'Nouvelle-Aquitaine') THEN 'SO'
        ELSE 'IdF' 
    END
)
HAVING
    "Région" IS NOT NULL AND zone IS NOT NULL
ORDER BY 
    "Date", mois, annee, "Région", zone
  );
