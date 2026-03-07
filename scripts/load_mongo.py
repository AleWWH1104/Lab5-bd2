"""
Ingesta de datos JSON a MongoDB Atlas.

Ejecutar desde la raiz del proyecto (MongoDB Atlas debe estar accesible):
    python scripts/load_mongo.py

Requiere: pip install pymongo[srv] python-dotenv
"""

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

# Cargar variables del .env
load_dotenv(Path(__file__).parent.parent / ".env")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB  = os.getenv("MONGO_DB", "lab5_db")

if not MONGO_URI or "<db_password>" in MONGO_URI:
    print("ERROR: Completa la variable MONGO_URI en el archivo .env con tu URI de Atlas.")
    sys.exit(1)

DATA_DIR = Path(__file__).parent.parent / "data" / "mongodb"

# Archivos de costos turisticos por region
COSTOS_FILES = [
    "costos_turisticos_africa.json",
    "costos_turisticos_america.json",
    "costos_turisticos_asia.json",
    "costos_turisticos_europa.json",
]


def load_costos_turisticos(db):
    collection = db["costos_turisticos"]
    collection.drop()  # Reinicio limpio

    all_docs = []
    for filename in COSTOS_FILES:
        path = DATA_DIR / filename
        with open(path, encoding="utf-8") as f:
            docs = json.load(f)
        all_docs.extend(docs)
        print(f"  Leido {filename}: {len(docs)} documentos")

    if all_docs:
        collection.insert_many(all_docs)
    print(f"[MongoDB] costos_turisticos: {len(all_docs)} documentos insertados.")


def load_paises_big_mac(db):
    collection = db["paises_big_mac"]
    collection.drop()

    path = DATA_DIR / "paises_mundo_big_mac.json"
    with open(path, encoding="utf-8") as f:
        docs = json.load(f)

    if docs:
        collection.insert_many(docs)
    print(f"[MongoDB] paises_big_mac: {len(docs)} documentos insertados.")


def main():
    print(f"Conectando a MongoDB Atlas / base de datos '{MONGO_DB}' ...")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        print("Conexion exitosa.\n")
    except Exception as e:
        print(f"ERROR al conectar a MongoDB: {e}")
        sys.exit(1)

    db = client[MONGO_DB]

    load_costos_turisticos(db)
    load_paises_big_mac(db)

    client.close()
    print("\nIngesta a MongoDB Atlas completada.")


if __name__ == "__main__":
    main()
