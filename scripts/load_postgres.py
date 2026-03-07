"""
Ingesta de datos CSV al PostgreSQL fuente (postgres-source).

Ejecutar desde la raiz del proyecto DESPUES de levantar docker-compose:
    python scripts/load_postgres.py

Requiere: pip install -r requirements.txt
"""

import csv
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

PG_USER = os.getenv("PG_SOURCE_USER", "labuser")
PG_PASS = os.getenv("PG_SOURCE_PASS", "labpass")
PG_DB   = os.getenv("PG_SOURCE_DB",   "labdb")

DATA_DIR = Path(__file__).parent.parent / "data" / "sql"


def get_conn():
    return psycopg2.connect(
        host="localhost", port=5433,
        user=PG_USER, password=PG_PASS, dbname=PG_DB
    )


def load_pais_envejecimiento(conn):
    csv_path = DATA_DIR / "pais_envejecimiento.csv"

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                int(row["id_pais"]),
                row["nombre_pais"].strip(),
                row["capital"].strip(),
                row["continente"].strip(),
                row["region"].strip(),
                int(float(row["poblacion"])) if row["poblacion"] else None,
                float(row["tasa_de_envejecimiento"]) if row["tasa_de_envejecimiento"] else None,
            )
            for row in reader
        ]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE pais_envejecimiento RESTART IDENTITY CASCADE;")
        cur.executemany(
            """
            INSERT INTO pais_envejecimiento
                (id_pais, nombre_pais, capital, continente, region, poblacion, tasa_de_envejecimiento)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()
    print(f"[PostgreSQL] pais_envejecimiento: {len(rows)} filas cargadas.")


def load_pais_poblacion(conn):
    csv_path = DATA_DIR / "pais_poblacion.csv"

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                row["_id"].strip(),
                row["continente"].strip(),
                row["pais"].strip(),
                int(float(row["poblacion"])) if row["poblacion"] else None,
                float(row["costo_bajo_hospedaje"]) if row["costo_bajo_hospedaje"] else None,
                float(row["costo_promedio_comida"]) if row["costo_promedio_comida"] else None,
                float(row["costo_bajo_transporte"]) if row["costo_bajo_transporte"] else None,
                float(row["costo_promedio_entretenimiento"]) if row["costo_promedio_entretenimiento"] else None,
            )
            for row in reader
        ]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE pais_poblacion RESTART IDENTITY CASCADE;")
        cur.executemany(
            """
            INSERT INTO pais_poblacion
                (mongo_id, continente, pais, poblacion,
                 costo_bajo_hospedaje, costo_promedio_comida,
                 costo_bajo_transporte, costo_promedio_entretenimiento)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.commit()
    print(f"[PostgreSQL] pais_poblacion: {len(rows)} filas cargadas.")


def main():
    print(f"Conectando a PostgreSQL source en localhost:5433 / {PG_DB} ...")
    try:
        conn = get_conn()
        print("Conexion exitosa.\n")
    except Exception as e:
        print(f"ERROR al conectar: {e}")
        sys.exit(1)

    try:
        load_pais_envejecimiento(conn)
        load_pais_poblacion(conn)
        print("\nIngesta a PostgreSQL completada.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
