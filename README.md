# Lab 05 — SQL + NoSQL | CC3089 Base de Datos 2

## Estructura del repositorio

```
lab05/
├── .env                        ← Credenciales (NO subir a Git)
├── docker-compose.yml          ← Levanta todos los servicios
├── data/
│   ├── sql/
│   │   ├── pais_envejecimiento.csv
│   │   └── pais_poblacion.csv
│   └── mongodb/
│       ├── costos_turisticos_africa.json
│       ├── costos_turisticos_america.json
│       ├── costos_turisticos_asia.json
│       ├── costos_turisticos_europa.json
│       └── paises_mundo_big_mac.json
├── dags/
│   └── lab05_etl_dag.py        ← DAG de Airflow (Ejercicio 1)
├── etl/
│   └── ejercicio2_etl.py       ← Script Python (Ejercicio 2)
└── scripts/
    ├── init_postgres.sql       ← DDL tablas fuente (se ejecuta al crear contenedor)
    ├── load_postgres.py        ← Ingesta CSVs → PostgreSQL source
    ├── load_mongo.py           ← Ingesta JSONs → MongoDB local
    ├── queries_insights.sql    ← Queries Ejercicio 3
    └── queries_mongodb.js      ← Queries MongoDB
```

---

## Cómo levantar el proyecto

### Pre-requisitos

- Docker Desktop instalado y corriendo
- Python 3.11+ — se recomienda **uv** para gestionar el entorno

**Con uv (recomendado):**

```bash
uv sync          # instala todas las dependencias del pyproject.toml
```

**Con pip:**

```bash
pip install -r requirements.txt
```

### 1. Levantar los contenedores Docker

```bash
docker compose up -d
```

### 2. Ingestar datos en PostgreSQL source

```bash
uv run python scripts/load_postgres.py
```

### 3. Ingestar datos en MongoDB local

```bash
uv run python scripts/load_mongo.py
```

Verifica con mongosh que las colecciones tengan datos:

```bash
mongosh "mongodb://labuser:labpass@localhost:27017/?authSource=admin" --eval "use lab5_db; db.costos_turisticos.countDocuments()"
```

### 4. Acceder a Airflow y activar el DAG

- URL: http://localhost:8080
- Usuario: `admin` / Contraseña: `admin`

---

## Ejercicio 1 — ETL con Apache Airflow

### Activar el DAG

1. Ir a http://localhost:8080
2. Buscar el DAG `lab05_etl_sql_nosql`
3. Activarlo con el toggle (columna izquierda)
4. Ejecutarlo manualmente con el botón ▶ para no esperar a medianoche

### Verificar ejecución

```bash
docker logs lab5-airflow-scheduler-1 -f
```

### Verificar datos en Warehouse

```bash
docker exec -it lab5-postgres-warehouse-1 psql -U labuser -d warehousedb \
  -c "SELECT COUNT(*) FROM fact_turismo_mundial;"
```

---

## Ejercicio 2 — ETL con Python (script standalone)

Requiere que los contenedores Docker estén corriendo (`docker compose up -d`).

```bash
# Con uv (recomendado — instala deps automáticamente)
uv run etl/ejercicio2_etl.py

# Con pip (requiere haber instalado dependencias antes)
python etl/ejercicio2_etl.py
```

---

## Ejercicio 3 — Insights

Ejecutar los queries en `scripts/queries_insights.sql` contra el **Data Warehouse**:

Conectarse al warehouse:

```bash
docker exec -it lab5-postgres-warehouse-1 psql -U labuser -d warehousedb
# Luego: \i /ruta/queries_insights.sql
```

O con cualquier cliente SQL (DBeaver, TablePlus, pgAdmin):

- Host: `localhost`
- Puerto: `5434`
- Base: `warehousedb`
- Usuario: `labuser`
- Password: `labpass`

## Apagar el proyecto

```bash
docker compose down           # Detiene contenedores
docker compose down -v        # Detiene + borra volúmenes (reset completo)
```
