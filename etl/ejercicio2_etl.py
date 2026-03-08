"""
Lab 05 - Base de Datos 2, UVG
Ejercicio 2 - Integración de datos con un lenguaje de programación

Flujo:
  2.1  Extrae y limpia datos de PostgreSQL (fuente SQL)
  2.2  Extrae y limpia datos de MongoDB Atlas (fuente NoSQL)
  2.3  Integra ambas fuentes en memoria con pandas
  2.4  Carga el resultado en PostgreSQL Warehouse (data warehouse)

Ejecutar desde la raíz del proyecto:
    uv run etl/ejercicio2_etl.py
  o con pip:
    python etl/ejercicio2_etl.py

Requiere: pip install -r requirements.txt
          (o uv sync si usas uv)
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "psycopg2-binary>=2.9.0",
#   "pymongo>=4.0.0",
#   "python-dotenv>=1.0.0",
#   "pandas>=2.0.0",
#   "sqlalchemy>=2.0.0",
# ]
# ///

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine, text

# ── Cargar variables de entorno ────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env")

POSTGRES_SOURCE = (
    f"postgresql+psycopg2://"
    f"{os.getenv('PG_SOURCE_USER', 'labuser')}:"
    f"{os.getenv('PG_SOURCE_PASS', 'labpass')}@"
    f"localhost:5433/"
    f"{os.getenv('PG_SOURCE_DB', 'labdb')}"
)
WAREHOUSE_CONN = (
    f"postgresql+psycopg2://"
    f"{os.getenv('PG_WH_USER', 'labuser')}:"
    f"{os.getenv('PG_WH_PASS', 'labpass')}@"
    f"localhost:5434/"
    f"{os.getenv('PG_WH_DB', 'warehousedb')}"
)
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB  = os.getenv("MONGO_DB", "lab5_db")


# ══════════════════════════════════════════════════════════════════════════════
# 2.1  EXTRACCIÓN Y LIMPIEZA — PostgreSQL (fuente SQL)
# ══════════════════════════════════════════════════════════════════════════════
def extract_sql() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lee las tablas pais_envejecimiento y pais_poblacion desde PostgreSQL.
    Limpieza aplicada:
      - Espacios sobrantes en nombres de países recortados con .strip()
      - Columna auxiliar 'pais_norm' (minúsculas) para el join posterior
      - Filas sin nombre de país se descartan (no aportan al join)
    Retorna: (df_envejecimiento, df_poblacion_sql)
    """
    print("\n[2.1] Extrayendo datos de PostgreSQL...")
    engine = create_engine(POSTGRES_SOURCE)

    # --- pais_envejecimiento ---
    df_env = pd.read_sql("SELECT * FROM pais_envejecimiento", engine)
    print(f"  pais_envejecimiento: {len(df_env)} filas cargadas")

    # Limpieza: descartar filas sin nombre de país
    antes = len(df_env)
    df_env = df_env[df_env["nombre_pais"].notna() & (df_env["nombre_pais"].str.strip() != "")]
    if len(df_env) < antes:
        print(f"  [limpieza] pais_envejecimiento: {antes - len(df_env)} filas sin nombre_pais descartadas")

    # Limpieza: nulos en tasa_de_envejecimiento
    nulos_tasa = df_env["tasa_de_envejecimiento"].isna().sum()
    if nulos_tasa:
        print(f"  [limpieza] {nulos_tasa} registros sin tasa_de_envejecimiento (se conservan, quedarán NULL)")

    df_env["nombre_pais_norm"] = df_env["nombre_pais"].str.strip().str.lower()

    # --- pais_poblacion ---
    df_pob = pd.read_sql("SELECT * FROM pais_poblacion", engine)
    print(f"  pais_poblacion:      {len(df_pob)} filas cargadas")

    antes = len(df_pob)
    df_pob = df_pob[df_pob["pais"].notna() & (df_pob["pais"].str.strip() != "")]
    if len(df_pob) < antes:
        print(f"  [limpieza] pais_poblacion: {antes - len(df_pob)} filas sin nombre descartadas")

    df_pob["pais_norm"] = df_pob["pais"].str.strip().str.lower()

    engine.dispose()
    print("[2.1] Extracción SQL completada.")
    return df_env, df_pob


# ══════════════════════════════════════════════════════════════════════════════
# 2.2  EXTRACCIÓN Y LIMPIEZA — MongoDB (fuente NoSQL)
# ══════════════════════════════════════════════════════════════════════════════
def extract_mongo() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lee las colecciones costos_turisticos y paises_big_mac desde MongoDB Atlas.
    Limpieza aplicada:
      - Documentos sin campo 'país' descartados
      - Duplicados por nombre de país eliminados (keep='first')
      - Columna auxiliar 'pais_norm' para el join posterior
      - Campos anidados de costos aplanados a columnas planas
    Retorna: (df_costos_turisticos, df_big_mac)
    """
    print("\n[2.2] Extrayendo datos de MongoDB Atlas...")

    if not MONGO_URI or "<db_password>" in MONGO_URI:
        print("ERROR: Configura MONGO_URI en el archivo .env")
        sys.exit(1)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    try:
        client.admin.command("ping")
        print("  Conexion a MongoDB exitosa.")
    except Exception as exc:
        print(f"  ERROR al conectar a MongoDB: {exc}")
        sys.exit(1)

    db = client[MONGO_DB]

    # --- costos_turisticos ---
    docs = list(db["costos_turisticos"].find({}, {"_id": 0}))
    print(f"  costos_turisticos: {len(docs)} documentos cargados")

    rows = []
    for doc in docs:
        pais = doc.get("país", "")
        if not pais:
            continue  # descartar documentos sin nombre de país
        costos = doc.get("costos_diarios_estimados_en_dólares", {})
        hosp   = costos.get("hospedaje", {})
        comida = costos.get("comida", {})
        transp = costos.get("transporte", {})
        entret = costos.get("entretenimiento", {})
        rows.append({
            "continente":      doc.get("continente", ""),
            "region":          doc.get("región", ""),
            "pais":            pais,
            "capital":         doc.get("capital", ""),
            "poblacion":       doc.get("población"),
            "hospedaje_bajo":  hosp.get("precio_bajo_usd"),
            "hospedaje_prom":  hosp.get("precio_promedio_usd"),
            "hospedaje_alto":  hosp.get("precio_alto_usd"),
            "comida_bajo":     comida.get("precio_bajo_usd"),
            "comida_prom":     comida.get("precio_promedio_usd"),
            "comida_alto":     comida.get("precio_alto_usd"),
            "transporte_bajo": transp.get("precio_bajo_usd"),
            "transporte_prom": transp.get("precio_promedio_usd"),
            "transporte_alto": transp.get("precio_alto_usd"),
            "entret_bajo":     entret.get("precio_bajo_usd"),
            "entret_prom":     entret.get("precio_promedio_usd"),
            "entret_alto":     entret.get("precio_alto_usd"),
        })

    df_costos = pd.DataFrame(rows)
    df_costos["pais_norm"] = df_costos["pais"].str.strip().str.lower()

    # Limpieza: eliminar duplicados por país
    antes = len(df_costos)
    df_costos.drop_duplicates(subset=["pais_norm"], keep="first", inplace=True)
    if len(df_costos) < antes:
        print(f"  [limpieza] costos_turisticos: {antes - len(df_costos)} duplicados eliminados")

    # --- paises_big_mac ---
    docs_bm = list(db["paises_big_mac"].find({}, {"_id": 0}))
    print(f"  paises_big_mac:    {len(docs_bm)} documentos cargados")

    df_bm = pd.DataFrame(docs_bm)
    df_bm.rename(columns={"país": "pais"}, inplace=True)

    # Descartar filas sin nombre de país
    df_bm = df_bm[df_bm["pais"].notna() & (df_bm["pais"].str.strip() != "")]
    df_bm["pais_norm"] = df_bm["pais"].str.strip().str.lower()

    antes = len(df_bm)
    df_bm.drop_duplicates(subset=["pais_norm"], keep="first", inplace=True)
    if len(df_bm) < antes:
        print(f"  [limpieza] paises_big_mac: {antes - len(df_bm)} duplicados eliminados")

    client.close()
    print("[2.2] Extracción MongoDB completada.")
    return df_costos, df_bm


# ══════════════════════════════════════════════════════════════════════════════
# 2.3  INTEGRACIÓN EN MEMORIA
# ══════════════════════════════════════════════════════════════════════════════
def integrate(
    df_env: pd.DataFrame,
    df_costos: pd.DataFrame,
    df_bm: pd.DataFrame,
) -> pd.DataFrame:
    """
    Integra las tres fuentes en un único DataFrame en memoria.

    Joins realizados:
      1. costos_turisticos  LEFT JOIN  pais_envejecimiento  (por nombre de país)
      2. resultado          LEFT JOIN  paises_big_mac        (por nombre de país)

    Columnas derivadas calculadas:
      - costo_diario_bajo_usd  = hospedaje_bajo + comida_bajo + transporte_bajo + entret_bajo
      - costo_diario_prom_usd  = hospedaje_prom + comida_prom + transporte_prom + entret_prom
      - costo_diario_alto_usd  = hospedaje_alto + comida_alto + transporte_alto + entret_alto
      - etl_loaded_at          = timestamp de carga

    Retorna: df_final listo para cargar al warehouse
    """
    print("\n[2.3] Integrando datos en memoria...")

    # JOIN 1: costos_turisticos + tasa_de_envejecimiento
    df_merged = pd.merge(
        df_costos,
        df_env[["nombre_pais_norm", "tasa_de_envejecimiento"]],
        left_on="pais_norm",
        right_on="nombre_pais_norm",
        how="left",
    )
    df_merged.drop(columns=["nombre_pais_norm"], errors="ignore", inplace=True)

    matches_env = df_merged["tasa_de_envejecimiento"].notna().sum()
    print(f"  JOIN costos x envejecimiento: {matches_env}/{len(df_merged)} paises emparejados")

    # JOIN 2: + precio Big Mac
    df_final = pd.merge(
        df_merged,
        df_bm[["pais_norm", "precio_big_mac_usd"]],
        on="pais_norm",
        how="left",
    )

    matches_bm = df_final["precio_big_mac_usd"].notna().sum()
    print(f"  JOIN x big_mac:              {matches_bm}/{len(df_final)} paises emparejados")

    # Columnas derivadas: costos diarios totales
    df_final["costo_diario_bajo_usd"] = (
        df_final["hospedaje_bajo"] +
        df_final["comida_bajo"] +
        df_final["transporte_bajo"] +
        df_final["entret_bajo"]
    )
    df_final["costo_diario_prom_usd"] = (
        df_final["hospedaje_prom"] +
        df_final["comida_prom"] +
        df_final["transporte_prom"] +
        df_final["entret_prom"]
    )
    df_final["costo_diario_alto_usd"] = (
        df_final["hospedaje_alto"] +
        df_final["comida_alto"] +
        df_final["transporte_alto"] +
        df_final["entret_alto"]
    )

    # Metadata de carga
    df_final["etl_loaded_at"] = datetime.now(tz=timezone.utc)

    # Eliminar columna auxiliar de join
    df_final.drop(columns=["pais_norm"], errors="ignore", inplace=True)

    print(f"  Dataset integrado: {df_final.shape[0]} filas x {df_final.shape[1]} columnas")
    print("[2.3] Integración completada.")
    return df_final


# ══════════════════════════════════════════════════════════════════════════════
# 2.4  CARGA AL DATA WAREHOUSE
# ══════════════════════════════════════════════════════════════════════════════
def load_warehouse(df: pd.DataFrame) -> None:
    """
    Crea (si no existe) la tabla fact_turismo_mundial en el data warehouse
    y carga el DataFrame integrado. Trunca la tabla en cada ejecución para
    garantizar idempotencia (recarga completa).
    """
    print("\n[2.4] Cargando datos al data warehouse (PostgreSQL)...")
    engine = create_engine(WAREHOUSE_CONN)

    ddl = """
        CREATE TABLE IF NOT EXISTS fact_turismo_mundial (
            id                      SERIAL PRIMARY KEY,
            continente              VARCHAR(50),
            region                  VARCHAR(100),
            pais                    VARCHAR(100),
            capital                 VARCHAR(100),
            poblacion               BIGINT,
            tasa_de_envejecimiento  NUMERIC(5,2),
            hospedaje_bajo          NUMERIC(10,2),
            hospedaje_prom          NUMERIC(10,2),
            hospedaje_alto          NUMERIC(10,2),
            comida_bajo             NUMERIC(10,2),
            comida_prom             NUMERIC(10,2),
            comida_alto             NUMERIC(10,2),
            transporte_bajo         NUMERIC(10,2),
            transporte_prom         NUMERIC(10,2),
            transporte_alto         NUMERIC(10,2),
            entret_bajo             NUMERIC(10,2),
            entret_prom             NUMERIC(10,2),
            entret_alto             NUMERIC(10,2),
            costo_diario_bajo_usd   NUMERIC(10,2),
            costo_diario_prom_usd   NUMERIC(10,2),
            costo_diario_alto_usd   NUMERIC(10,2),
            precio_big_mac_usd      NUMERIC(6,2),
            etl_loaded_at           TIMESTAMP WITH TIME ZONE
        );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text("TRUNCATE TABLE fact_turismo_mundial RESTART IDENTITY;"))
        print("  Tabla fact_turismo_mundial lista (truncada para recarga limpia).")

    df.to_sql(
        "fact_turismo_mundial",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=100,
    )

    engine.dispose()
    print(f"  {len(df)} filas insertadas correctamente.")
    print("[2.4] Carga al warehouse completada.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  Ejercicio 2 - ETL con lenguaje de programacion (Python)")
    print("  Lab 05 - Base de Datos 2, UVG")
    print("=" * 60)

    # 2.1 - Fuente SQL
    df_env, _df_pob = extract_sql()

    # 2.2 - Fuente NoSQL
    df_costos, df_bm = extract_mongo()

    # 2.3 - Integración en memoria
    df_final = integrate(df_env, df_costos, df_bm)

    # 2.4 - Carga al warehouse
    load_warehouse(df_final)

    print("\n" + "=" * 60)
    print("  ETL Ejercicio 2 finalizado exitosamente.")
    print("=" * 60)


if __name__ == "__main__":
    main()
