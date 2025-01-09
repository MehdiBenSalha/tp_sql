# Projet DBT - Analyse et Intégration des Données de Température et de Consommation

Ce projet DBT (Data Build Tool) vise à analyser et intégrer des données relatives à la consommation énergétique régionale et aux températures quotidiennes régionales. Il comprend la modélisation des données, des transformations SQL, des tests, et une documentation complète.  

# Travail réalisé par :  
### Jrad Ghassen  
### Ben Salha Mehdi  


## Structure du Projet

- **Dossier des transformations :** Les requêtes SQL sont situées dans le répertoire `transform/models/example/`. 
  - Chaque fichier SQL dans ce répertoire correspond à une étape de transformation ou d'intégration des données.
  - Les fichiers suivent une structure logique qui facilite leur exécution et leur maintenance.
  
- **Modèles :**
  - Les données de température (`temp.sql`) sont transformées et nettoyées pour l'intégration avec les données de consommation énergétique.
  - Les données de consommation énergétique (`clean.sql`) sont enrichies avec les informations climatiques pour des analyses croisées.



## Instructions pour la Mise en Route

1. **Installation des Dépendances :**
   - Assurez-vous d'avoir `dbt` installé. Vous pouvez suivre les [instructions officielles de DBT](https://docs.getdbt.com/docs/installation).

2. **Configuration du Projet :**
   - Configurez les connexions à votre entrepôt de données dans le fichier `profiles.yml`.
   - Placez les fichiers de données source dans le répertoire approprié.

3. **Exécution des Modèles :**
   - Lancez le projet en exécutant la commande :
     ```bash
     dbt run
     ```
   - Pour vérifier la qualité des données, utilisez :
     ```bash
     dbt test
     ```

4. **Documentation :**
   - Générez et visualisez la documentation DBT en exécutant :
     ```bash
     dbt docs generate
     dbt docs serve
     ```

5. **Répertoire des Données :**
   - Les modèles de données transformées sont disponibles dans `transform/models/example`.


### Nettoyage et Préparation des Données

Cette étape de transformation nettoie et prépare les données pour l'analyse. Elle inclut la conversion des colonnes en types appropriés, la gestion des valeurs manquantes, et le calcul de nouveaux attributs utiles, comme la production totale d'énergie.

#### Requête SQL : Nettoyage des Données

```sql
WITH data_clean AS (
    SELECT 
        *,
        COALESCE(TRY_CAST("Consommation (MW)" AS INT), 0) AS consommation,
        COALESCE(TRY_CAST("Eolien (MW)" AS INT), 0) AS eolien,
        COALESCE(TRY_CAST("Thermique (MW)" AS INT), 0) AS thermique,
        COALESCE(TRY_CAST("Bioénergies (MW)" AS INT), 0) AS bioenergies,
        COALESCE(TRY_CAST("Nucléaire (MW)" AS INT), 0) AS nucleaire,
        COALESCE(TRY_CAST("Solaire (MW)" AS INT), 0) AS solaire,
        COALESCE(TRY_CAST("Hydraulique (MW)" AS INT), 0) AS hydraulique,
        COALESCE(TRY_CAST("Pompage (MW)" AS INT), 0) AS pompage,
        COALESCE(TRY_CAST("Ech. physiques (MW)" AS INT), 0) AS ech_physiques
    FROM eco2
    WHERE "Région" IS NOT NULL AND "Date" IS NOT NULL
),
data_final AS (
    SELECT 
        "Code INSEE région",
        "Région", 
        "Date", 
        "Heure",
        consommation,
        eolien,
        thermique,
        bioenergies,
        nucleaire,
        solaire,
        hydraulique,
        pompage,
        ech_physiques,
        COALESCE(nucleaire, 0) + 
        COALESCE(eolien, 0) +
        COALESCE(solaire, 0) +
        COALESCE(hydraulique, 0) +
        COALESCE(thermique, 0) AS production_totale
    FROM data_clean
)

SELECT * FROM data_final;
```
```sql
WITH data_clean AS (
    SELECT *
    from clean
          
)
SELECT 
    "Date", 
    "Région", 
    SUM(COALESCE(TRY_CAST("consommation" AS INT), 0)) AS consommation_totale,
    SUM(COALESCE(TRY_CAST("thermique" AS INT), 0)) AS thermique_total,
    SUM(COALESCE(TRY_CAST("bioenergies" AS INT), 0)) AS bioenergies_total,
    SUM(COALESCE(TRY_CAST("eolien" AS INT), 0)) AS eolien_total,
    SUM(COALESCE(TRY_CAST("nucleaire" AS INT), 0)) AS nucleaire_total,
    SUM(COALESCE(TRY_CAST("solaire" AS INT), 0)) AS solaire_total,
    SUM(COALESCE(TRY_CAST("hydraulique" AS INT), 0)) AS hydraulique_total,
    SUM(COALESCE(TRY_CAST("pompage" AS INT), 0)) AS pompage_total,
    SUM(COALESCE(TRY_CAST("ech_physiques" AS INT), 0)) AS ech_physiques_total,
    SUM(COALESCE(TRY_CAST("production_totale" AS INT), 0)) AS production_totale
   
FROM data_clean
GROUP BY "Date", "Région"
ORDER BY "Date", "Région"


```


## Exploration des Données : Groupement et Agrégation Simples

### Objectif
Analyser la production et la consommation d'énergie par mois et par région, avec des indicateurs tels que la production et la consommation totales (en GWh), ainsi que les valeurs minimale, maximale et moyenne instantanées (en MW).

### Requête SQL

```sql
SELECT 
    strftime('%Y-%m', Date) AS mois,  -- Extraction de l'année et du mois
    "Région", 
    SUM(COALESCE(TRY_CAST("consommation" AS INT), 0))/2000 AS consommation_totale_en_GWH,
    SUM(COALESCE(production_totale, 0))/2000 AS production_totale_en_GWH,

    MIN(COALESCE(TRY_CAST("consommation" AS INT), 0)) AS min_consommation_totale,
    MIN(COALESCE(production_totale, 0)) AS min_production_totale,

    MAX(COALESCE(TRY_CAST("consommation" AS INT), 0)) AS max_consommation_totale,
    MAX(COALESCE(production_totale, 0)) AS max_production_totale,
    
    AVG(COALESCE(TRY_CAST("consommation" AS INT), 0)) AS moyenne_consommation_totale,
    AVG(COALESCE(production_totale, 0)) AS moyenne_production_totale
FROM {{ ref('clean') }}
GROUP BY strftime('%Y-%m', Date), "Région"
ORDER BY mois, "Région";
```

## Requête de consommation quotidienne par région

Cette requête calcule la consommation quotidienne d'électricité (en GWh) par région, avec une ligne pour chaque date et une colonne pour chaque région.

### Requête SQL :

```sql
{% set regions = [
    "Bourgogne-Franche-Comté",
    "Centre-Val de Loire",
    "Nouvelle-Aquitaine",
    "Occitanie",
    "Île-de-France",
    "Normandie",
    "Grand Est",
    "Provence-Alpes-Côte d'Azur",
    "Bretagne",
    "Hauts-de-France",
    "Auvergne-Rhône-Alpes",
    "Pays de la Loire"
] %}

SELECT 
    Date,
    {% for region in regions %}
        SUM(CASE WHEN "Région" = '{{ region | replace("'", "''") }}' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "{{ region | replace("'", "''") }}"
        {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM 
    {{ ref('clean') }}
GROUP BY 
    Date
ORDER BY 
    Date
```

## Requête de consommation cumulée du mois écoulé

Cette requête calcule la consommation cumulative (en GWh) pour chaque jour du mois écoulé, par région, en utilisant une fonction de fenêtre avec la clause `RANGE` pour inclure toutes les données précédentes du même mois.

### Requête SQL :

```sql
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
    FROM {{ ref('clean') }}
)

SELECT 
    "Code INSEE région", 
    "Région", 
    "Date", 
    consommation, 
    consommation_gwh
FROM data_with_consumption
ORDER BY "Région", "Date"
```

## Requête des 20 plus grands écarts de consommation quotidienne

Cette requête calcule les 20 plus grands écarts de consommation (en GWh) d'un jour à l'autre, par région, en utilisant une fonction de fenêtre pour calculer la variation de consommation. Elle décompose le calcul en plusieurs étapes à l'aide de CTEs (Common Table Expressions).

### Requête SQL :

```sql
WITH daily_consumption AS (
    SELECT 
        "Région",
        "Date",
        SUM(COALESCE(consommation, 0)) / 2000 AS consommation_gwh  -- Conversion MW → GWh
    FROM {{ ref('clean') }}
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
```

## Requête du jour de dépassement des énergies renouvelables

Cette requête calcule la date à laquelle la consommation quotidienne dépasse la production annuelle totale des filières renouvelables, pour chaque année de 2013 à 2021. Elle utilise des fonctions de fenêtre et des CTEs pour effectuer les calculs.

### Requête SQL :

```sql
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

```
## Requête des 3 plus longues séquences d'augmentation de consommation

Cette requête utilise une CTE récursive pour identifier et classer les séquences continues d'augmentation de la consommation instantanée, puis sélectionne les trois plus longues séquences pour chaque région, en incluant la date, la durée et la séquence de consommation.

### Requête SQL :

```sql
WITH sequenced_consumption AS (
    -- Identifie les séquences d'augmentation de consommation
    SELECT
        CONCAT("Date", ' ', "Heure") AS "Date_Heure",  -- Combinaison de la date et de l'heure
        "Région",
        "consommation",
        ROW_NUMBER() OVER (PARTITION BY "Région" ORDER BY "Date", "Heure") AS row_num,
        CASE 
            WHEN "consommation" > LAG("consommation") OVER (PARTITION BY "Région" ORDER BY "Date", "Heure") THEN 0 
            ELSE 1 
        END AS is_new_sequence
    FROM {{ ref('clean') }}
    WHERE "consommation" IS NOT NULL
),
sequence_grouped AS (
    -- Crée les groupes de séquences continues
    SELECT 
        "Date_Heure",
        "Région",
        "consommation",
        row_num,
        SUM(is_new_sequence) OVER (PARTITION BY "Région" ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sequence_group
    FROM sequenced_consumption
),
sequence_details AS (
    -- Agrège les séquences par groupe et calcule la durée
    SELECT 
        MIN(CAST("Date_Heure" AS TIMESTAMP)) AS start_datetime,
        MAX(CAST("Date_Heure" AS TIMESTAMP)) AS end_datetime,
        "Région",
        ARRAY_AGG("consommation" ORDER BY "Date_Heure") AS sequence,
        COUNT(*) AS sequence_length
    FROM sequence_grouped
    GROUP BY "Région", sequence_group
    HAVING COUNT(*) > 1  -- Filtre les séquences de plus d'une valeur (augmentation continue)
),
ranked_sequences AS (
    -- Classe les séquences par longueur décroissante et par date de début pour gérer les égalités
    SELECT 
        start_datetime,
        end_datetime,
        "Région",
        sequence,
        sequence_length,
        ROW_NUMBER() OVER (PARTITION BY "Région" ORDER BY sequence_length DESC, start_datetime) AS rank
    FROM sequence_details
)
-- Sélectionne les trois plus longues séquences
SELECT 
    start_datetime AS "Date - Heure",
    -- Calcul de la durée manuellement (en heures, minutes, secondes)
    EXTRACT(HOUR FROM end_datetime - start_datetime) || ':' || 
    LPAD(CAST(EXTRACT(MINUTE FROM end_datetime - start_datetime) AS VARCHAR), 2, '0') || ':' || 
    LPAD(CAST(EXTRACT(SECOND FROM end_datetime - start_datetime) AS VARCHAR), 2, '0') AS "Durée (hh:mm:ss)",
    "Région",
    sequence AS "Séquence (MW*)",
    rank AS "Rang"
FROM ranked_sequences
WHERE rank <= 3
ORDER BY "Rang", "Région"
```

## Requête de construction du cube de consommation

Cette requête génère un cube de consommation (en GWh) en agrégant les données par jour, mois, année et sur toute la période, ainsi que par région et zone géographique (NO, NE, SO, SE, et IdF). Elle exclut la Corse et fournit toutes les combinaisons de ces dimensions temporelles et géographiques.

### Requête SQL :

```sql
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
    {{ ref('clean') }}
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
```

## Intégration des données de température dans un entrepôt de données multi-dimensionnel

### Description :
Cette requête permet d'intégrer un second jeu de données d'historique des températures (2016 à 2022) dans une base de données multi-dimensionnelle, en résolvant les problèmes d'alignement des données sur la base d'un relevé par jour. Les températures minimales, maximales et moyennes sont conservées et jointes avec les données existantes de consommation.

### Requête SQL :

```sql
WITH temperature_data AS (
    -- Chargement des données de température
    SELECT
        ID,
        Date,
        "Code INSEE région" AS code_insee_region,
        Région,
        "TMin (°C)" AS tmin,
        "TMax (°C)" AS tmax,
        "TMoy (°C)" AS tmoy
    FROM temperature  -- La table existante
),
clean_data_with_temp AS (
    -- Jointure entre les données nettoyées et les données de température
    SELECT
        c."Code INSEE région",
        c.Région,
        c.Date,
        c.Heure,
        c.consommation,
        c.eolien,
        c.thermique,
        c.bioenergies,
        c.nucleaire,
        c.solaire,
        c.hydraulique,
        c.pompage,
        c.ech_physiques,
        c.production_totale,
        t.tmin,
        t.tmax,
        t.tmoy
    FROM {{ ref('clean') }} c  -- Table clean, issue de DBT
    LEFT JOIN temperature_data t
        ON c.Date = t.Date
        AND c."Code INSEE région" = t.code_insee_region
)
-- Création de la table finale
SELECT
    "Code INSEE région",
    Région,
    Date,
    Heure,
    consommation,
    eolien,
    thermique,
    bioenergies,
    nucleaire,
    solaire,
    hydraulique,
    pompage,
    ech_physiques,
    production_totale,
    tmin,
    tmax,
    tmoy
FROM clean_data_with_temp
```

# Requêtes SQL pour la base de données multi-dimensionnelle

## 1. `consommation_par_saison`

**Description :**
Cette requête agrège les données de consommation et de production d'énergie par saison, en calculant la consommation totale et les différentes productions par filière, ainsi que les températures moyennes, minimales et maximales pour chaque saison.

```sql
WITH consommation_par_saison AS (
    SELECT
        CASE
            WHEN strftime('%m', "Date") IN ('12', '01', '02') THEN 'Hiver'
            WHEN strftime('%m', "Date") IN ('03', '04', '05') THEN 'Printemps'
            WHEN strftime('%m', "Date") IN ('06', '07', '08') THEN 'Eté'
            ELSE 'Automne'
        END AS saison,
        "Code INSEE région" AS region_code,
        "Région",
        SUM(consommation) AS consommation_total_GWh,
        SUM(eolien) AS eolien_GWh,
        SUM(thermique) AS thermique_GWh,
        SUM(bioenergies) AS bioenergies_GWh,
        SUM(nucleaire) AS nucleaire_GWh,
        SUM(solaire) AS solaire_GWh,
        SUM(hydraulique) AS hydraulique_GWh,
        SUM(pompage) AS pompage_GWh,
        SUM(production_totale) AS production_totale_GWh,
        AVG(tmin) AS temperature_min_moyenne,
        AVG(tmax) AS temperature_max_moyenne,
        AVG(tmoy) AS temperature_avg_moyenne,
        MIN(tmin) AS temperature_min_min,
        MAX(tmax) AS temperature_max_max
    FROM clean_temp
    GROUP BY
        saison,
        "Code INSEE région",
        "Région"
)
SELECT * FROM consommation_par_saison;
```
## 2. consommation_par_saison (avec classification de température)
**Description :**
Cette requête agrège également les données par saison, mais inclut une classification des températures en différents intervalles (Glacial, Froid, Modéré, Idéal, Chaud, Extrême) pour chaque région, en fonction de la température minimale quotidienne.

```sql

WITH consommation_par_saison AS (
    SELECT
        CASE
            WHEN strftime('%m', "Date") IN ('12', '01', '02') THEN 'Hiver'
            WHEN strftime('%m', "Date") IN ('03', '04', '05') THEN 'Printemps'
            WHEN strftime('%m', "Date") IN ('06', '07', '08') THEN 'Eté'
            ELSE 'Automne'
        END AS saison,
        "Code INSEE région" AS region_code,
        "Région",
        SUM(consommation) AS consommation_total_GWh,
        SUM(pompage) AS pompage_GWh,
        SUM(thermique) AS Thermique_GWh,
        SUM(nucleaire) AS Nucléaire_GWh,
        SUM(eolien) AS Eolien_GWh,
        SUM(solaire) AS Solaire_GWh,
        SUM(hydraulique) AS Hydraulique_GWh,
        SUM(bioenergies) AS Bioénergies_GWh,
        SUM(production_totale) AS Production_Totale_GWh,
        AVG(tmin) AS temperature_min_moyenne,
        AVG(tmax) AS temperature_max_moyenne,
        AVG(tmoy) AS temperature_avg_moyenne,
        MIN(tmin) AS temperature_min_min,
        MAX(tmax) AS temperature_max_max,
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
        tmin
)
SELECT * FROM consommation_par_saison;
```

## 3. `consommation_par_periode`
**Description :**
Cette requête agrège les données par jour, mois, année et sur toute la période, tout en calculant la consommation et la production d'énergie, ainsi que les températures moyennes et extrêmes pour chaque période.

```sql
WITH consommation_par_periode AS (
    SELECT
        "Date" AS jour,
        SUM(consommation) AS consommation_total_GWh,
        SUM(pompage) AS pompage_GWh,
        SUM(thermique) AS Thermique_GWh,
        SUM(nucleaire) AS Nucléaire_GWh,
        SUM(eolien) AS Eolien_GWh,
        SUM(solaire) AS Solaire_GWh,
        SUM(hydraulique) AS Hydraulique_GWh,
        SUM(bioenergies) AS Bioénergies_GWh,
        SUM(production_totale) AS Production_Totale_GWh,
        AVG(tmin) AS temperature_min_moyenne,
        AVG(tmax) AS temperature_max_moyenne,
        AVG(tmoy) AS temperature_avg_moyenne,
        MIN(tmin) AS temperature_min_min,
        MAX(tmax) AS temperature_max_max,
        strftime('%m', "Date") AS mois,
        strftime('%Y', "Date") AS annee,
        'Toute la période' AS periode_totale
    FROM clean_temp
    GROUP BY
        "Date",
        mois,
        annee,
        periode_totale
)
SELECT * FROM consommation_par_periode;
```
## 4.`taux_de_couverture`
**Description :**
Cette requête calcule le taux de couverture (TCO) pour chaque jour, région et code INSEE. Le taux de couverture est le rapport entre la production totale d'énergie et la consommation, avec une gestion des divisions par zéro.

```sql
WITH taux_de_couverture AS (
    SELECT
        "Date",
        "Code INSEE région",
        "Région",
        SUM(production_totale) AS production_totale_GWh,
        SUM(consommation) AS consommation_GWh,
        (SUM(production_totale) / NULLIF(SUM(consommation), 0)) AS TCO
    FROM clean_temp
    GROUP BY
        "Date",
        "Code INSEE région",
        "Région"
)
SELECT * FROM taux_de_couverture;
```
## 5.`consommation_par_filiere`
**Description :**
Cette requête calcule la production d'énergie par filière (éolien, thermique, bioénergies, nucléaire, solaire, hydraulique, pompage) et la consommation pour chaque jour, région et code INSEE.

```sql
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
SELECT * FROM consommation_par_filiere;
```
## Requête pour construire le cuboïde par mois, quart de jour et intervalle de température

**Description :**  
Cette requête calcule la consommation et la production d'énergie pour chaque mois, chaque quart de jour (Nuit, Matin, Après-midi, Soir) et chaque intervalle de température (Glacial, Froid, Modéré, Idéal, Chaud, Extrême). Elle agrège les données de consommation et de production par ces dimensions, à partir d'une table de données de température et d'énergie.

```sql
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
SELECT * FROM cuboide_par_mois_quart_temp;
```


## Procédure de mise à jour incrémentale de l'entrepôt de données

**Description :**  
Cette procédure vise à mettre à jour l'entrepôt de données de manière incrémentale, en évitant le recalcul de l'intégralité des données. Elle prend en compte les statuts des données : temps réel, consolidée et définitive.

### 1. Identification des nouvelles données

**Objectif :** Identifier les nouvelles données arrivant dans le système, par rapport à la dernière mise à jour de l'entrepôt.

- **Sources de données :** Vérifier les nouvelles données disponibles dans les sources externes.
- **Critère de mise à jour :** Les données sont identifiées comme nouvelles si elles ont une date de collecte ou un identifiant supérieur à ceux déjà présents dans l'entrepôt.
  
**Étapes :**
- Vérifier la source de données pour les nouvelles entrées.
- Identifier les enregistrements dont les dates ou identifiants sont plus récents que ceux enregistrés dans l'entrepôt.
  
### 2. Gestion du statut des données

Les données peuvent avoir différents statuts : **temps réel**, **consolidée**, ou **définitive**.

- **Données temps réel :** Ce sont des données brutes collectées en temps réel, souvent sujettes à des corrections et mises à jour fréquentes.
- **Données consolidées :** Ce sont des données agrégées ou corrigées, représentant une version plus fiable mais qui peut encore subir des modifications.
- **Données définitives :** Ce sont les données finalisées et validées, qui ne doivent plus être modifiées.

**Procédure de mise à jour des statuts :**
1. **Temps réel à consolidée :**  
   - Dès que des données sont consolidées, leur statut passe de "temps réel" à "consolidée".
   - Cette mise à jour se produit après une étape de validation des données par des processus de correction ou d’agrégation.
  
2. **Consolidée à définitive :**  
   - Une fois que les données ont été validées et qu'aucune mise à jour supplémentaire n'est prévue, elles sont marquées comme définitives.
   - Cela signifie que les données ne doivent plus être modifiées, sauf en cas d’erreurs graves nécessitant une correction.

**Étapes :**
- Vérifier le statut actuel de chaque nouvelle donnée.
- Mettre à jour les données avec un statut "temps réel" en "consolidée" lorsque les corrections ou agrégations sont effectuées.
- Mettre à jour les données avec un statut "consolidée" en "définitive" lorsqu'elles sont validées et finalisées.

### 3. Mise à jour incrémentale des données dans l'entrepôt

**Objectif :** Mettre à jour l'entrepôt de données de manière incrémentale pour ne pas recalculer l'intégralité des données.

**Étapes :**
1. **Récupérer les nouvelles données :**  
   - Extraire les nouvelles données à partir de la source (par exemple, fichiers CSV, API, bases de données).
   - Identifier les données correspondant aux nouveaux enregistrements à ajouter à l'entrepôt.
  
2. **Vérification des duplications :**  
   - Comparer les nouvelles données avec celles déjà présentes dans l'entrepôt (par exemple, par identifiant unique ou date).
   - S'assurer qu'il n'y a pas de doublons ou de mises à jour non souhaitées des anciennes données.
  
3. **Insertion des nouvelles données :**  
   - Insérer uniquement les nouvelles données ou les données mises à jour (basées sur la date de dernière mise à jour).
   - S'assurer que les nouvelles données sont insérées avec le bon statut (temps réel, consolidée, définitive).

4. **Mise à jour des données existantes :**  
   - Pour les enregistrements existants dont les données ont été modifiées (en raison de nouvelles agrégations ou corrections), mettre à jour uniquement les champs concernés.
   - Le statut des données doit être ajusté en fonction des étapes de validation (temps réel à consolidée, consolidée à définitive).

5. **Gestion des données temporelles :**  
   - Pour les données temporelles (par exemple, consommations horaires), ne mettre à jour que les périodes concernées sans recalculer l’intégralité des historiques.

6. **Validation des données mises à jour :**  
   - Vérifier la qualité des données après chaque mise à jour.
   - Mettre en place des tests de cohérence pour s'assurer que les agrégations et transformations ont été correctement effectuées.

### 4. Archivage et gestion des versions

**Objectif :** Garder une trace de l’historique des modifications et assurer une gestion des versions des données.

**Étapes :**
- Archivez les anciennes versions des données (avant la mise à jour) pour une traçabilité.
- Assurez-vous que chaque modification est enregistrée avec une date et un identifiant de version pour chaque mise à jour.

### 5. Planification des mises à jour

**Objectif :** Automatiser la mise à jour incrémentale sur des intervalles réguliers.

**Étapes :**
- Planifier des mises à jour régulières (par exemple, quotidiennement ou hebdomadairement) selon la fréquence de collecte des nouvelles données.
- Automatiser le processus pour minimiser les interventions manuelles et garantir une mise à jour rapide.

---

### Exemple de flux de travail :

1. **Données entrantes :** Collecte de données en temps réel.
2. **Validation et agrégation :** Les données sont traitées et consolidées.
3. **Mise à jour incrémentale :** Les nouvelles données consolidées sont ajoutées ou mises à jour dans l’entrepôt sans recalculer l’intégralité des anciennes données.
4. **Changement de statut :** Les données passent de "temps réel" à "consolidée", puis de "consolidée" à "définitive" après validation.

Cette procédure permet de maintenir l'intégrité de l'entrepôt tout en optimisant les ressources et en évitant des recalculs inutiles.

