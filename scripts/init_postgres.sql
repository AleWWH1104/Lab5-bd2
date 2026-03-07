-- DDL para la base de datos fuente (postgres-source)
-- Se ejecuta automáticamente al crear el contenedor

-- ── Tabla: pais_envejecimiento ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pais_envejecimiento (
    id_pais                 SERIAL PRIMARY KEY,
    nombre_pais             VARCHAR(100) NOT NULL,
    capital                 VARCHAR(100),
    continente              VARCHAR(50),
    region                  VARCHAR(100),
    poblacion               BIGINT,
    tasa_de_envejecimiento  NUMERIC(5,2)
);

-- ── Tabla: pais_poblacion ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pais_poblacion (
    id                              SERIAL PRIMARY KEY,
    mongo_id                        VARCHAR(30),
    continente                      VARCHAR(50),
    pais                            VARCHAR(100),
    poblacion                       BIGINT,
    costo_bajo_hospedaje            NUMERIC(10,2),
    costo_promedio_comida           NUMERIC(10,2),
    costo_bajo_transporte           NUMERIC(10,2),
    costo_promedio_entretenimiento  NUMERIC(10,2)
);
