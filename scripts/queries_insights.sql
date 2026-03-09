
-- Insight 1
SELECT continente,
       ROUND(AVG(costo_diario_prom_usd), 2) AS costo_promedio,
       ROUND(AVG(precio_big_mac_usd), 2)    AS big_mac_promedio,
       COUNT(*) AS num_paises
FROM fact_turismo_mundial
GROUP BY continente
ORDER BY costo_promedio DESC;

-- Insight 2
SELECT pais, continente,
       tasa_de_envejecimiento,
       costo_diario_prom_usd
FROM fact_turismo_mundial
ORDER BY tasa_de_envejecimiento DESC
LIMIT 15;

-- Insight 3
SELECT pais, continente,
       costo_diario_bajo_usd,
       precio_big_mac_usd,
       poblacion
FROM fact_turismo_mundial
WHERE costo_diario_bajo_usd < 100
  AND precio_big_mac_usd < 4
ORDER BY costo_diario_bajo_usd ASC
LIMIT 15;