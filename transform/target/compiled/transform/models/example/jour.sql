SELECT "Déstockage batterie", COUNT(*) AS value_count
FROM eco2
GROUP BY "Déstockage batterie"
ORDER BY value_count DESC;