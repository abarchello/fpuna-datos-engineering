"""
Extractor de datos hidrométricos desde la API REST Institucional.

Lee datos de estaciones automáticas de monitoreo hidrológico y los carga
en MotherDuck para su posterior transformación con dbt.

Estaciones objetivo:
    134 — Itabó Guazú
    236 — Pozuelo Puente

Campos por registro (cada ~20 minutos):
    fecha, nivel, conductividad, ph, turbidez, od, tempagua

Modos de operación:
    1) MODO PRODUCCIÓN — si HIDRO_API_BASE_URL está configurado en .env,
       se hace GET al API institucional.
    2) MODO DEMO — si HIDRO_API_BASE_URL no está configurado (o es un
       placeholder), se cargan los datos sintéticos desde
       extraction/sample_data/*.json.gz. Esto permite a cualquier
       evaluador correr el pipeline end-to-end sin acceso al API privado.

Uso:
    python extraction/hidro_extractor.py
"""
import os
import json
import gzip
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
import duckdb
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Configuración desde .env ──────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
HIDRO_API_BASE   = os.getenv("HIDRO_API_BASE_URL", "").strip()
HIDRO_FORMAT     = os.getenv("HIDRO_API_FORMAT", "json")
FECHA_INICIO     = os.getenv("HIDRO_FECHA_INICIO", "2015-01-01")
FECHA_FIN        = os.getenv("HIDRO_FECHA_FIN", datetime.today().strftime("%Y-%m-%d"))

# Estaciones automáticas a sincronizar — diccionario nombre interno → ID API
STATIONS = {
    "itabo_guazu":    os.getenv("HIDRO_STATION_ITABO_GUAZU_ID",    "134"),
    "pozuelo_puente": os.getenv("HIDRO_STATION_POZUELO_PUENTE_ID", "236"),
}

SAMPLE_DIR = Path(__file__).parent / "sample_data"

# Detección automática del modo: si la URL no está o es un placeholder,
# el extractor cae a modo demo.
DEMO_MODE = (
    not HIDRO_API_BASE
    or HIDRO_API_BASE.startswith("your_")
    or HIDRO_API_BASE.lower() in {"demo", "placeholder"}
)


# ── Validación de configuración ───────────────────────────────────────────────
def validate_config() -> None:
    """Verifica que las variables mínimas estén cargadas."""
    if not MOTHERDUCK_TOKEN:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN no está configurado en .env.\n"
            "Crear una cuenta gratuita en https://app.motherduck.com y generar "
            "un Personal Access Token."
        )


# ── Extracción desde API (modo producción) ────────────────────────────────────
def fetch_station_from_api(station_id: str, start: str, end: str) -> list[dict]:
    """Descarga las mediciones de una estación entre dos fechas.

    Endpoint:
        {BASE_URL}/{start}/{end}/{station_id}/?format=json
    """
    url = f"{HIDRO_API_BASE}/{start}/{end}/{station_id}/"
    print(f"  -> GET API (modo produccion) - id={station_id}")

    response = httpx.get(url, params={"format": HIDRO_FORMAT}, timeout=180.0)
    response.raise_for_status()

    data = response.json()
    if isinstance(data, dict) and "results" in data:
        data = data["results"]
    elif not isinstance(data, list):
        data = [data]

    print(f"     {len(data)} registros recibidos desde el API")
    return data


# ── Extracción desde JSON local (modo demo) ───────────────────────────────────
def fetch_station_from_sample(station_name: str) -> list[dict]:
    """Carga las mediciones desde el JSON.gz de muestra incluido en el repo."""
    archivo = SAMPLE_DIR / f"hidro_estacion_{station_name}.json.gz"
    if not archivo.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo de muestra: {archivo}\n"
            f"Verificar que extraction/sample_data/ tenga los .json.gz comiteados."
        )

    print(f"  -> Leyendo JSON.gz local (modo demo) - {archivo.name}")
    with gzip.open(archivo, "rt", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        data = [data]

    print(f"     {len(data)} registros cargados desde la muestra")
    return data


# ── Carga en MotherDuck ───────────────────────────────────────────────────────
def load_to_motherduck(rows: list[dict], station_name: str, conn) -> int:
    """Carga las mediciones en MotherDuck.

    Crea/reemplaza la tabla:
        airbyte_curso.main.hidro_estacion_{station_name}
    """
    if not rows:
        print(f"     (sin filas para {station_name})")
        return 0

    import tempfile
    import os as _os

    table_name = f"hidro_estacion_{station_name}"
    full_path  = f"airbyte_curso.main.{table_name}"

    extracted_at_iso = datetime.utcnow().isoformat()

    # Escribimos un NDJSON (un objeto por linea) a un archivo temporal local
    # y dejamos que DuckDB lo lea de un solo golpe con read_json_auto.
    # Esto es MUCHO mas rapido que executemany contra MotherDuck cloud porque
    # se envia un solo bulk insert en lugar de N round-trips.
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".ndjson", prefix=f"hidro_{station_name}_")
    _os.close(tmp_fd)
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            for r in rows:
                rec = {
                    "station_name":   station_name,
                    "measurement_ts": r.get("fecha"),
                    "raw_payload":    r,
                    "extracted_at":   extracted_at_iso,
                }
                f.write(json.dumps(rec, ensure_ascii=False, default=str))
                f.write("\n")

        size_mb = _os.path.getsize(tmp_path) / (1024 * 1024)
        print(f"     Volcado a archivo temporal ({size_mb:.1f} MB), cargando a MotherDuck...")

        conn.execute("CREATE SCHEMA IF NOT EXISTS airbyte_curso.main")
        conn.execute(f"DROP TABLE IF EXISTS {full_path}")
        # read_json_auto lee NDJSON y deja DuckDB inferir el esquema.
        conn.execute(f"""
            CREATE TABLE {full_path} AS
            SELECT
                station_name::VARCHAR    AS station_name,
                measurement_ts::VARCHAR  AS measurement_ts,
                to_json(raw_payload)     AS raw_payload,
                extracted_at::TIMESTAMP  AS extracted_at
            FROM read_json_auto('{tmp_path.replace(chr(92), chr(92)*2)}', format='newline_delimited')
        """)

        print(f"     {len(rows)} filas cargadas en {full_path}")
        return len(rows)
    finally:
        try:
            _os.remove(tmp_path)
        except Exception:
            pass


# ── Orquestación local ────────────────────────────────────────────────────────
def run_extraction(start: Optional[str] = None, end: Optional[str] = None) -> dict:
    """Ejecuta la extracción completa de todas las estaciones configuradas."""
    validate_config()

    start = start or FECHA_INICIO
    end   = end   or FECHA_FIN

    modo = "DEMO (datos de muestra incluidos en el repo)" if DEMO_MODE else "PRODUCCION (API real)"
    print(f"=== Extraccion Hidrometrica Institucional ===")
    print(f"Modo:       {modo}")
    if not DEMO_MODE:
        print(f"Rango:      {start}  ->  {end}")
    print(f"Estaciones: {', '.join(STATIONS.keys())}")
    print(f"Destino:    MotherDuck airbyte_curso.main.hidro_estacion_*\n")

    conn = duckdb.connect(f"md:airbyte_curso?motherduck_token={MOTHERDUCK_TOKEN}")
    results = {}

    for name, station_id in STATIONS.items():
        print(f"[{name.upper()}]  id={station_id}")
        try:
            if DEMO_MODE:
                rows = fetch_station_from_sample(name)
            else:
                rows = fetch_station_from_api(station_id, start, end)
            loaded = load_to_motherduck(rows, name, conn)
            results[name] = {"status": "ok", "rows": loaded}
        except Exception as e:
            print(f"     ERROR: {e}")
            results[name] = {"status": "failed", "error": str(e)}

    conn.close()

    print("\n=== Resumen ===")
    for name, r in results.items():
        if r["status"] == "ok":
            print(f"  {name:18s} OK    ({r['rows']:>7} filas)")
        else:
            print(f"  {name:18s} FALLA ({r['error'][:60]})")
    return results


if __name__ == "__main__":
    run_extraction()
