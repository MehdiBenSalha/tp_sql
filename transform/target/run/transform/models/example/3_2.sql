
  
  create view "dev"."main"."3_2__dbt_tmp" as (
    -- Récupérer les régions distinctes dans un set Jinja


SELECT 
    Date,
    
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Bourgogne-Franche-Comté' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Bourgogne-Franche-Comté"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Centre-Val de Loire' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Centre-Val de Loire"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Nouvelle-Aquitaine' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Nouvelle-Aquitaine"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Occitanie' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Occitanie"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Île-de-France' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Île-de-France"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Normandie' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Normandie"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Grand Est' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Grand Est"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Provence-Alpes-Côte d''Azur' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Provence-Alpes-Côte d''Azur"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Bretagne' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Bretagne"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Hauts-de-France' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Hauts-de-France"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Auvergne-Rhône-Alpes' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Auvergne-Rhône-Alpes"
        , 
    
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = 'Pays de la Loire' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "Pays de la Loire"
        
    
    
FROM 
    "dev"."main"."clean"
GROUP BY 
    Date
ORDER BY 
    Date
  );
