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
    ├── load_mongo.py           ← Ingesta JSONs → MongoDB Atlas
    ├── queries_insights.sql    ← Queries Ejercicio 3
    └── queries_mongodb.js      ← Queries MongoDB
```

---

## Cómo levantar el proyecto

### Pre-requisitos

- Docker Desktop instalado y corriendo
- Python 3.9+ con las dependencias: `pip install pandas sqlalchemy psycopg2-binary pymongo[srv] python-dotenv`
- Cuenta en **MongoDB Atlas** con un cluster creado

### 1. Configurar credenciales

Editar `.env` y reemplazar la línea `MONGO_URI` con tu URI real de Atlas:

### 2. Levantar los contenedores Docker

```bash
docker compose up -d
```

### 3. Ingestar datos en PostgreSQL source

```bash
python scripts/load_postgres.py
```

### 4. Ingestar datos en MongoDB Atlas

```bash
python scripts/load_mongo.py
```

Verifica desde la UI de Atlas o con mongosh que las colecciones `costos_turisticos` y `paises_big_mac` tengan datos.

### 5. Activar el DAG en Airflow

---

## Ejercicio 1 — ETL con Apache Airflow

### Acceder a Airflow

- URL: http://localhost:8080
- Usuario: `admin`
- Contraseña: `admin`

### Activar el DAG

1. Ir a http://localhost:8080
2. Buscar el DAG `lab05_etl_sql_nosql`
3. Activarlo con el toggle (columna izquierda)
4. Ejecutarlo manualmente con el botón ▶ para no esperar a medianoche

### Verificar ejecución

```bash
# Ver logs del scheduler
docker logs lab05_airflow_scheduler -f
```

### Verificar datos en Warehouse

```bash
docker exec -it lab5-bd2-postgres-warehouse-1 psql -U labuser -d warehousedb \
  -c "SELECT COUNT(*) FROM fact_turismo_mundial;"
```

---

## Ejercicio 2 — ETL con Python (script standalone)

```bash
# Instalar dependencias (si es local)
pip install pymongo psycopg2-binary pandas sqlalchemy

# Ejecutar (con Docker corriendo)
python etl/ejercicio2_etl.py
```

O desde dentro del contenedor de Airflow:

```bash
docker exec -it lab05_airflow_webserver python /opt/airflow/etl/ejercicio2_etl.py
```

---

## Ejercicio 3 — Insights

Ejecutar los queries en `scripts/queries_insights.sql` contra el **Data Warehouse**:

Conectarse al warehouse:

```bash
docker exec -it lab05_postgres_warehouse psql -U labuser -d warehousedb
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
