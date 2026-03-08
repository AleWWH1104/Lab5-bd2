"""
Lab 05 - Base de Datos 2, UVG
DAG de Apache Airflow para el proceso ETL (Ejercicio 1)

Flujo:
  1. Extrae datos de PostgreSQL (SQL source)
  2. Extrae datos de MongoDB (NoSQL source)
  3. Limpia y transforma ambas fuentes
  4. Integra los datos en memoria
  5. Carga el resultado en PostgreSQL Warehouse

Frecuencia: cada día a la medianoche (configurable)
"""

from datetime import datetime, timedelta
import json
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Conexiones (tomadas de variables de entorno del docker-compose) ────────────
POSTGRES_SOURCE = os.getenv(
    "POSTGRES_SOURCE_CONN",
    "postgresql+psycopg2://labuser:labpass@postgres-source:5432/labdb"
)
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://labuser:labpass@mongodb:27017/?authSource=admin"
)
WAREHOUSE_CONN = os.getenv(
    "WAREHOUSE_CONN",
    "postgresql+psycopg2://labuser:labpass@postgres-warehouse:5432/warehousedb"
)

# ── Argumentos por defecto del DAG ─────────────────────────────────────────────
default_args = {
    "owner": "lab05",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# TASK 1 — Extraer y limpiar datos de PostgreSQL (fuente SQL)
def extract_sql(**context):
    engine = create_engine(POSTGRES_SOURCE)

    # --- pais_envejecimiento ---
    df_env = pd.read_sql("SELECT * FROM pais_envejecimiento", engine)
    print(f"[SQL] pais_envejecimiento: {len(df_env)} filas")

    # Limpieza: normalizar nombre de país a minúsculas para join
    df_env["nombre_pais_norm"] = df_env["nombre_pais"].str.strip().str.lower()

    # Revisar nulos relevantes
    nulos = df_env[["nombre_pais", "tasa_de_envejecimiento"]].isnull().sum()
    print(f"[SQL] Nulos en columnas clave:\n{nulos}")

    # --- pais_poblacion ---
    df_pob = pd.read_sql("SELECT * FROM pais_poblacion", engine)
    print(f"[SQL] pais_poblacion: {len(df_pob)} filas")
    df_pob["pais_norm"] = df_pob["pais"].str.strip().str.lower()

    # Guardar en XCom como JSON
    context["ti"].xcom_push(key="df_envejecimiento", value=df_env.to_json(orient="records"))
    context["ti"].xcom_push(key="df_poblacion_sql", value=df_pob.to_json(orient="records"))
    print("[SQL] Extracción completada ✓")


# TASK 2 — Extraer y limpiar datos de MongoDB (fuente NoSQL)
def extract_mongo(**context):
    client = MongoClient(MONGO_URI)
    db = client[os.getenv("MONGO_DB", "lab5_db")]

    # --- costos_turisticos (todas las regiones) ---
    docs = list(db["costos_turisticos"].find({}, {"_id": 0}))
    print(f"[Mongo] costos_turisticos: {len(docs)} documentos")

    rows = []
    for doc in docs:
        costos = doc.get("costos_diarios_estimados_en_dólares", {})
        rows.append({
            "continente":     doc.get("continente", ""),
            "region":         doc.get("región", ""),
            "pais":           doc.get("país", ""),
            "capital":        doc.get("capital", ""),
            "poblacion":      doc.get("población", None),
            # Hospedaje
            "hospedaje_bajo": costos.get("hospedaje", {}).get("precio_bajo_usd"),
            "hospedaje_prom": costos.get("hospedaje", {}).get("precio_promedio_usd"),
            "hospedaje_alto": costos.get("hospedaje", {}).get("precio_alto_usd"),
            # Comida
            "comida_bajo":    costos.get("comida", {}).get("precio_bajo_usd"),
            "comida_prom":    costos.get("comida", {}).get("precio_promedio_usd"),
            "comida_alto":    costos.get("comida", {}).get("precio_alto_usd"),
            # Transporte
            "transporte_bajo": costos.get("transporte", {}).get("precio_bajo_usd"),
            "transporte_prom": costos.get("transporte", {}).get("precio_promedio_usd"),
            "transporte_alto": costos.get("transporte", {}).get("precio_alto_usd"),
            # Entretenimiento
            "entret_bajo":    costos.get("entretenimiento", {}).get("precio_bajo_usd"),
            "entret_prom":    costos.get("entretenimiento", {}).get("precio_promedio_usd"),
            "entret_alto":    costos.get("entretenimiento", {}).get("precio_alto_usd"),
        })

    df_costos = pd.DataFrame(rows)
    df_costos["pais_norm"] = df_costos["pais"].str.strip().str.lower()

    # --- paises_big_mac ---
    docs_bm = list(db["paises_big_mac"].find({}, {"_id": 0}))
    print(f"[Mongo] paises_big_mac: {len(docs_bm)} documentos")
    df_bm = pd.DataFrame(docs_bm)
    df_bm.rename(columns={"país": "pais", "precio_big_mac_usd": "precio_big_mac_usd"}, inplace=True)
    df_bm["pais_norm"] = df_bm["pais"].str.strip().str.lower()

    # Limpieza: duplicados
    df_costos.drop_duplicates(subset=["pais_norm"], inplace=True)
    df_bm.drop_duplicates(subset=["pais_norm"], inplace=True)

    context["ti"].xcom_push(key="df_costos_turisticos", value=df_costos.to_json(orient="records"))
    context["ti"].xcom_push(key="df_big_mac", value=df_bm.to_json(orient="records"))
    print("[Mongo] Extracción completada ✓")


# ═══════════════════════════════════════════════════════════════════
# TASK 3 — Integrar ambas fuentes y cargar al Warehouse
def transform_and_load(**context):
    ti = context["ti"]

    # Recuperar DataFrames
    df_env    = pd.read_json(ti.xcom_pull(key="df_envejecimiento"))
    df_costos = pd.read_json(ti.xcom_pull(key="df_costos_turisticos"))
    df_bm     = pd.read_json(ti.xcom_pull(key="df_big_mac"))

    print(f"[ETL] Shapes antes de join: env={df_env.shape}, costos={df_costos.shape}, bm={df_bm.shape}")

    # ── JOIN 1: costos_turisticos + pais_envejecimiento (por nombre de país) ──
    # Se excluye "region" de df_env porque df_costos ya trae su propia región
    # de MongoDB; incluirla generaría region_x / region_y rompiendo el schema.
    df_merged = pd.merge(
        df_costos,
        df_env[["nombre_pais_norm", "tasa_de_envejecimiento"]],
        left_on="pais_norm",
        right_on="nombre_pais_norm",
        how="left"
    )

    # ── JOIN 2: + big_mac ──────────────────────────────────────────────────────
    df_final = pd.merge(
        df_merged,
        df_bm[["pais_norm", "precio_big_mac_usd"]],
        on="pais_norm",
        how="left"
    )

    # Calcular costo diario total estimado (bajo, promedio, alto)
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

    # Columna de carga
    df_final["etl_loaded_at"] = datetime.utcnow()

    # Limpiar columnas auxiliares de join
    df_final.drop(columns=["pais_norm", "nombre_pais_norm"], errors="ignore", inplace=True)

    print(f"[ETL] Dataset final: {df_final.shape[0]} filas, {df_final.shape[1]} columnas")

    # ── Cargar al Warehouse ────────────────────────────────────────────────────
    engine = create_engine(WAREHOUSE_CONN)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_turismo_mundial (
                id                          SERIAL PRIMARY KEY,
                continente                  VARCHAR(50),
                region                      VARCHAR(100),
                pais                        VARCHAR(100),
                capital                     VARCHAR(100),
                poblacion                   BIGINT,
                tasa_de_envejecimiento      NUMERIC(5,2),
                -- Costos hospedaje
                hospedaje_bajo              NUMERIC(10,2),
                hospedaje_prom              NUMERIC(10,2),
                hospedaje_alto              NUMERIC(10,2),
                -- Costos comida
                comida_bajo                 NUMERIC(10,2),
                comida_prom                 NUMERIC(10,2),
                comida_alto                 NUMERIC(10,2),
                -- Costos transporte
                transporte_bajo             NUMERIC(10,2),
                transporte_prom             NUMERIC(10,2),
                transporte_alto             NUMERIC(10,2),
                -- Costos entretenimiento
                entret_bajo                 NUMERIC(10,2),
                entret_prom                 NUMERIC(10,2),
                entret_alto                 NUMERIC(10,2),
                -- Costos diarios totales calculados
                costo_diario_bajo_usd       NUMERIC(10,2),
                costo_diario_prom_usd       NUMERIC(10,2),
                costo_diario_alto_usd       NUMERIC(10,2),
                -- Big Mac index
                precio_big_mac_usd          NUMERIC(6,2),
                -- Metadata
                etl_loaded_at               TIMESTAMP
            );
        """))
        # Truncar para recarga limpia en cada ejecución
        conn.execute(text("TRUNCATE TABLE fact_turismo_mundial RESTART IDENTITY;"))

    df_final.to_sql(
        "fact_turismo_mundial",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=100
    )

    print(f"[ETL] {len(df_final)} filas cargadas al warehouse ✓")


# Definición del DAG
with DAG(
    dag_id="lab05_etl_sql_nosql",
    default_args=default_args,
    description="ETL Lab05: PostgreSQL + MongoDB → Data Warehouse",
    schedule_interval="0 0 * * *",   # Diario a medianoche (cron)
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["lab05", "etl", "uvg"],
) as dag:

    t1_extract_sql = PythonOperator(
        task_id="extract_sql",
        python_callable=extract_sql,
    )

    t2_extract_mongo = PythonOperator(
        task_id="extract_mongo",
        python_callable=extract_mongo,
    )

    t3_transform_load = PythonOperator(
        task_id="transform_and_load_warehouse",
        python_callable=transform_and_load,
    )

    # Dependencias: SQL y Mongo en paralelo, luego integración
    [t1_extract_sql, t2_extract_mongo] >> t3_transform_load
