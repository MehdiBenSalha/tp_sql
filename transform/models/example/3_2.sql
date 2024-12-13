-- Récupérer les régions distinctes dans un set Jinja
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
        -- On s'assure que les noms de régions contenant des caractères spéciaux sont correctement échappés
        SUM(CASE WHEN "Région" = '{{ region | replace("'", "''") }}' THEN COALESCE(CAST("consommation" AS INT), 0) / 2000 ELSE 0 END) AS "{{ region | replace("'", "''") }}"
        {% if not loop.last %}, {% endif %}
    {% endfor %}
    
FROM 
    {{ ref('clean') }}
GROUP BY 
    Date
ORDER BY 
    Date