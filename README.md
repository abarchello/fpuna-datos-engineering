# Pipeline de Datos — MIAAD FPUNA

**Autor:** [@abarchello](https://github.com/abarchello)
**Materia:** Introducción a la Ingeniería de Datos
**Profesor:** [@rparrapy](https://github.com/rparrapy)
**Cohorte:** MIAAD FPUNA 2026
**Repositorio de referencia del curso:** https://github.com/rparrapy/fpuna-maven

---

## 1. Descripción del proyecto

Pipeline ELT (Extract – Load – Transform) end-to-end que ingesta datos de múltiples fuentes,
los carga en un Data Warehouse cloud y aplica transformaciones modeladas en tres capas con
dbt. La orquestación se realiza con Prefect y la visualización con Metabase.

Las fuentes incluyen datos públicos vía Airbyte (PokeAPI, GitHub, CoinGecko), la base de datos
operacional MySQL del curso (Maven Fuzzy Factory: 32K órdenes, 472K sesiones, 1.1M pageviews)
también vía Airbyte, y, para el Trabajo Final, una API REST institucional de mediciones
hidrométricas integrada mediante un extractor Python custom.

---

## 2. Arquitectura

```
Fuentes (Extracción)
├── PokeAPI ──────────┐
├── GitHub  ──────────┤
├── CoinGecko ────────┤──► Airbyte ──┐
├── MySQL Ecommerce ──┘   (Docker)   │
│   (Maven Fuzzy Factory)            ├──► MotherDuck ──► Metabase
│                                    │    (DWH)         (Dashboards)
└── API Hidrométrica ─► Extractor ───┘
   (Itaipú, custom)     Python (httpx)
                                              │
                                              ▼
                                          dbt — 3 capas
                                          ├── staging/      (views — limpieza y tipado)
                                          ├── intermediate/ (views — joins y enriquecimiento)
                                          └── marts/        (tables — métricas finales)
                                              │
                                              ▼
                                          Prefect (Orquestación)
                                              │
                                              ▼
                                          Metabase (Visualización)
```

---

## 3. Stack tecnológico

| Componente | Herramienta | Rol |
|---|---|---|
| Extracción | Airbyte (Docker local) | Conectores nativos a PokeAPI, GitHub, CoinGecko, MySQL |
| Extracción custom | Python + httpx | Cliente HTTP para la API REST hidrométrica institucional |
| Data Warehouse | MotherDuck (DuckDB cloud) | Almacenamiento columnar — `airbyte_curso` |
| Transformación | dbt-duckdb 1.11 | Modelado en 3 capas con tests y documentación |
| Calidad | dbt-expectations | Validaciones de calidad de datos avanzadas |
| Orquestación | Prefect 3.x | Flujos con `@flow` y `@task`, integración Airbyte + dbt |
| Visualización | Metabase | Dashboards interactivos con filtros |

---

## 4. Estructura del repositorio

```
fpuna-datos-engineering/
├── dbt/
│   ├── dbt_project.yml              # Configuración del proyecto dbt
│   ├── profiles.yml                 # Conexión a MotherDuck (lee MOTHERDUCK_TOKEN de env)
│   ├── packages.yml                 # dbt-expectations
│   ├── models/
│   │   ├── staging/                 # Capa raw → limpia (views)
│   │   │   ├── _sources.yml         # Definición de fuentes
│   │   │   ├── _stg__models.yml     # Tests y documentación
│   │   │   ├── stg_pokemon.sql
│   │   │   ├── stg_github_issues.sql
│   │   │   ├── stg_github_pull_requests.sql
│   │   │   ├── stg_sessions.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_pageviews.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_refunds.sql
│   │   │   └── stg_hidro_mediciones.sql
│   │   ├── intermediate/            # Joins y métricas intermedias
│   │   │   ├── int_github_activity.sql
│   │   │   └── int_hidro_diario.sql
│   │   └── marts/                   # Métricas finales (tables)
│   │       ├── _marts__models.yml
│   │       ├── mart_pokemon_analytics.sql       # OBT Pokemon
│   │       ├── mart_github_summary.sql          # Resumen mensual GitHub
│   │       ├── mart_hidro_resumen.sql           # OBT hidrométrica (Trabajo Final)
│   │       ├── mart_hidro_calidad.sql           # Trazabilidad de outliers (Trabajo Final)
│   │       ├── fct_daily_sales.sql              # Ecommerce — ventas diarias
│   │       ├── fct_channel_performance.sql      # Ecommerce — performance por canal/UTM
│   │       ├── fct_product_performance.sql      # Ecommerce — performance por producto
│   │       └── obt_orders_enriched.sql          # Ecommerce — OBT órdenes
│   └── tests/                       # Singular tests personalizados
│       ├── test_pokemon_no_duplicate_names.sql
│       ├── test_hidro_no_future_measurements.sql
│       └── test_orders_margin_not_negative.sql  # Regla de negocio ecommerce
├── extraction/
│   ├── hidro_extractor.py           # Extractor Python (modos demo / producción)
│   └── sample_data/                 # Serie histórica real (gzipped) para demo
│       ├── hidro_estacion_itabo_guazu.json.gz
│       └── hidro_estacion_pozuelo_puente.json.gz
├── prefect/
│   └── ecommerce_pipeline.py        # Flujo Prefect end-to-end
├── .env.example                     # Plantilla de variables de entorno
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 5. Cómo ejecutar el pipeline (instrucciones reproducibles)

### 5.1 Requisitos previos

- Python 3.11 o superior
- Docker Desktop (para Airbyte y Metabase)
- Cuenta en MotherDuck con token activo
- Cuenta en Airbyte (instancia local o cloud)

### 5.2 Clonar e instalar dependencias

```bash
git clone https://github.com/abarchello/fpuna-datos-engineering.git
cd fpuna-datos-engineering
python -m venv .venv
# Activar el entorno virtual:
#   Linux / macOS:  source .venv/bin/activate
#   Windows:        .venv\Scripts\activate
pip install -r requirements.txt
```

### 5.3 Configurar variables de entorno

Copiar la plantilla y completar los valores reales:

```bash
cp .env.example .env
# Editar .env con tus credenciales
```

Variables mínimas requeridas:

| Variable | Descripción |
|---|---|
| `MOTHERDUCK_TOKEN` | Token de acceso a MotherDuck (obtenible en app.motherduck.com) |
| `AIRBYTE_HOST`, `AIRBYTE_PORT` | URL de la instancia Airbyte |
| `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET` | Credenciales API de Airbyte |
| `AIRBYTE_CONNECTION_ID_*` | UUIDs de las conexiones configuradas |
| `HIDRO_API_BASE_URL` | URL base del API institucional (opcional — sin esto, el extractor usa el demo data incluido) |
| `HIDRO_STATION_*_ID` | IDs de las estaciones a sincronizar (solo si se usa el API real) |

> **Importante:** `.env` está incluido en `.gitignore`. **Nunca** lo subas al repositorio.

### 5.4 Configurar Airbyte (extracción)

1. Levantar Airbyte localmente con Docker:
   ```bash
   abctl local install
   ```
2. Acceder a `http://localhost:8000` y crear las sources:
   - PokeAPI (conector nativo)
   - GitHub (requiere Personal Access Token)
   - CoinGecko Free (conector custom, ver `Clase_3/Ordenar/coin_gecko_free.yaml`)
   - MySQL Maven Fuzzy Factory (opcional — solo si la BD del curso está activa)
3. Crear un destination apuntando a MotherDuck (`airbyte_curso`).
4. Crear las connections source → destination y ejecutar el primer sync.
5. Copiar los Connection IDs (UUIDs) de la URL de cada conexión y pegarlos en `.env`.

### 5.5 Inicializar dbt

```bash
cd dbt
dbt deps --profiles-dir .          # Instala dbt-expectations
dbt debug --profiles-dir .         # Verifica la conexión a MotherDuck
```

### 5.6 Ejecutar las transformaciones

```bash
# Build completo (run + tests) de todos los modelos
dbt build --profiles-dir .

# Solo modelos de PokeAPI y GitHub
dbt run --select "stg_pokemon stg_github_issues stg_github_pull_requests int_github_activity mart_pokemon_analytics mart_github_summary" --profiles-dir .

# Solo modelos de Ecommerce
dbt run --select "stg_sessions stg_orders stg_pageviews stg_order_items stg_refunds fct_daily_sales fct_channel_performance fct_product_performance obt_orders_enriched" --profiles-dir .

# Solo modelos del Trabajo Final (hidrométrica + monitoreo de calidad)
dbt run --select "stg_hidro_mediciones int_hidro_diario mart_hidro_resumen mart_hidro_calidad" --profiles-dir .
```

### 5.7 Generar documentación y DAG

```bash
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

Abrir `http://localhost:8080` para explorar el DAG y la documentación.

### 5.8 Ejecutar el extractor del Trabajo Final

```bash
python extraction/hidro_extractor.py
```

El extractor tiene dos modos de operación:

- **Modo demo (por defecto)**: si `HIDRO_API_BASE_URL` no está configurado en el
  `.env`, el extractor lee la serie histórica completa desde los archivos
  `extraction/sample_data/*.json.gz` incluidos en este repositorio. Esto permite
  ejecutar el pipeline end-to-end sin acceso al API institucional.

- **Modo producción**: si `HIDRO_API_BASE_URL` apunta a un endpoint válido, el
  extractor hace requests HTTP al API real. El acceso al API requiere autorización
  institucional formal y queda fuera del alcance de esta entrega.

Las dos estaciones automáticas incluidas son:

| ID | Nombre | Cobertura |
|---|---|---|
| 134 | Itabó Guazú | nov-2018 a mar-2025 (~162 mil mediciones) |
| 236 | Pozuelo Puente | abr-2021 hasta la fecha (~108 mil mediciones) |

Las mediciones se hacen cada 20 minutos aproximadamente e incluyen nivel del agua,
conductividad eléctrica, pH, turbidez, oxígeno disuelto y temperatura del agua.

Después de la extracción, los datos crudos quedan en `airbyte_curso.main.hidro_estacion_*`
y dbt los transforma en las tres capas (`stg_hidro_mediciones`, `int_hidro_diario`,
`mart_hidro_resumen`). El mart adicional `mart_hidro_calidad` cuantifica los outliers
descartados por día y estación, garantizando trazabilidad completa de las transformaciones
de limpieza aplicadas en la capa staging.

### 5.9 Orquestar todo con Prefect

```bash
# Terminal 1 — servidor Prefect
prefect server start

# Terminal 2 — ejecutar el flujo
python prefect/ecommerce_pipeline.py
```

Abrir `http://localhost:4200` para ver la ejecución del flujo.

### 5.10 Visualizar con Metabase

La imagen oficial de Metabase (basada en Alpine Linux) tiene incompatibilidades con el driver
nativo de DuckDB. Se construye una imagen propia basada en Debian con el driver de
MotherDuck pre-instalado:

```dockerfile
# metabase-debian/Dockerfile
FROM eclipse-temurin:21-jre
ENV MB_PLUGINS_DIR=/plugins
RUN mkdir -p ${MB_PLUGINS_DIR} /app
ADD https://downloads.metabase.com/v0.58.8/metabase.jar /app/metabase.jar
ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/1.4.3.1/duckdb.metabase-driver.jar ${MB_PLUGINS_DIR}/duckdb.metabase-driver.jar
EXPOSE 3000
CMD ["java", "-jar", "/app/metabase.jar"]
```

```bash
docker build -t metabase-debian:local metabase-debian/
docker run -d -p 3000:3000 --name metabase metabase-debian:local
```

Acceder a `http://localhost:3000`, completar el setup inicial y conectar la base de datos:

- **Database type:** DuckDB
- **Display name:** `Ecommerce DWH` (o el nombre que prefieras)
- **Database file:** `md:airbyte_curso`
- **MotherDuck Token:** el valor de `MOTHERDUCK_TOKEN` del archivo `.env`

Con la conexión activa, construir el dashboard con al menos cinco visualizaciones y dos
filtros interactivos. Las queries SQL usadas en el dashboard del proyecto están documentadas
en el Reporte Técnico del Trabajo Final.

---

## 6. Modelado de datos

El proyecto implementa los dos enfoques discutidos en clase:

- **Esquema en estrella (Kimball)** — orientado a casos con múltiples dimensiones reutilizables
  y necesidad de drill-down (ejemplo: `fct_daily_sales`, `fct_channel_performance`).
- **One Big Table (OBT)** — orientado a análisis exploratorio sobre warehouses columnares
  como DuckDB/MotherDuck (ejemplos: `mart_pokemon_analytics`, `obt_orders_enriched`,
  `mart_hidro_resumen`).

La justificación detallada del enfoque elegido para cada dominio se encuentra en el reporte
técnico del Trabajo Final.

---

## 7. Calidad de datos

| Tipo de test | Cantidad | Ubicación |
|---|---|---|
| Genéricos de dbt (`unique`, `not_null`, `accepted_values`) | 20+ | `_stg__models.yml`, `_marts__models.yml` |
| dbt-expectations (`expect_column_values_to_be_between`, `expect_table_row_count_to_be_between`) | 10+ | `_stg__models.yml`, `_marts__models.yml` |
| Singular tests personalizados | 3 | `dbt/tests/` |

Resultado de la ejecución más reciente sobre datos reales:
`PASS=50  WARN=7  ERROR=0  TOTAL=57`. Los 7 warnings están documentados y corresponden a
outliers reales de sensores y a la dinámica de Append mode del conector PokeAPI.

Ejecutar todos los tests:

```bash
cd dbt
dbt test --profiles-dir .
```

---

## 8. Estado de las entregas

| Tarea | Contenido | Estado |
|---|---|---|
| Tarea 4 | Diseño de Star Schema y OBT (documento) | Entregado |
| Tarea 5 | Proyecto dbt con 3 capas (este repositorio) | Entregado |
| Tarea 6 | Tests + documentación dbt (este repositorio) | Entregado |
| Tarea 7 | Pipeline completo Airbyte + dbt + Prefect + Metabase | Entregado |
| Trabajo Final | Pipeline end-to-end con datos hidrométricos + reporte + video | Entregado |

---

## 9. Licencia y uso

Proyecto académico desarrollado en el marco de la Maestría en Inteligencia Artificial y
Análisis de Datos (MIAAD) de la Facultad Politécnica – Universidad Nacional de Asunción.

---

*Introducción a la Ingeniería de Datos — MIAAD FPUNA 2026*
