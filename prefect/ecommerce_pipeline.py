"""
Pipeline de datos completo: Airbyte -> API Institucional -> dbt -> Tests
Introduccion a la Ingenieria de Datos - MIAAD FPUNA
"""
import os
import sys
import time
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect_dbt.cli.commands import DbtCoreOperation

# Permitir importar extraction/hidro_extractor.py
sys.path.insert(0, str(Path(__file__).parent.parent))
from extraction.hidro_extractor import run_extraction as run_hidro_extraction

load_dotenv(Path(__file__).parent.parent / ".env")

AIRBYTE_HOST   = os.getenv("AIRBYTE_HOST", "localhost")
AIRBYTE_PORT   = os.getenv("AIRBYTE_PORT", "8000")
AIRBYTE_BASE   = f"http://{AIRBYTE_HOST}:{AIRBYTE_PORT}/api/public/v1"
CLIENT_ID      = os.getenv("AIRBYTE_CLIENT_ID")
CLIENT_SECRET  = os.getenv("AIRBYTE_CLIENT_SECRET")
CONN_POKEAPI   = os.getenv("AIRBYTE_CONNECTION_ID_POKEAPI")
CONN_GITHUB    = os.getenv("AIRBYTE_CONNECTION_ID_GITHUB")
CONN_ECOMMERCE = os.getenv("AIRBYTE_CONNECTION_ID_ECOMMERCE")
DBT_PROJECT    = str(Path(__file__).parent.parent / "dbt")


def get_airbyte_token() -> str:
    resp = httpx.post(
        f"http://{AIRBYTE_HOST}:{AIRBYTE_PORT}/api/v1/applications/token",
        json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "grant_type": "client_credentials"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def trigger_sync(connection_id: str, token: str) -> str:
    resp = httpx.post(
        f"{AIRBYTE_BASE}/jobs",
        json={"connectionId": connection_id, "jobType": "sync"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if resp.status_code == 409:
        # Ya hay un job corriendo
        return "already_running"
    resp.raise_for_status()
    return resp.json()["jobId"]


def wait_for_job(job_id: str, token: str, max_wait: int = 600) -> str:
    logger = get_run_logger()
    elapsed = 0
    while elapsed < max_wait:
        resp = httpx.get(
            f"{AIRBYTE_BASE}/jobs/{job_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status", "pending")
        logger.info(f"Job {job_id}: {status} ({elapsed}s)")
        if status == "succeeded":
            return "succeeded"
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"Airbyte job {job_id} terminó con estado: {status}")
        time.sleep(20)
        elapsed += 20
    raise TimeoutError(f"Job {job_id} no terminó en {max_wait}s")


@task(retries=2, retry_delay_seconds=30)
def sync_source(connection_id: str, source_name: str) -> None:
    logger = get_run_logger()
    logger.info(f"Iniciando sync: {source_name}")
    token = get_airbyte_token()
    job_id = trigger_sync(connection_id, token)
    if job_id == "already_running":
        logger.warning(f"{source_name}: sync ya en ejecucion, esperando...")
        time.sleep(30)
        return
    wait_for_job(job_id, token)
    logger.info(f"{source_name}: sync completado OK")


@task(retries=2, retry_delay_seconds=30)
def extract_hidro_api() -> dict:
    """Extrae datos de la API REST Institucional Hidrometeorologica.

    Lee directamente desde el endpoint configurado en .env y carga los
    datos crudos en MotherDuck (schema main, tablas hidro_estacion_*).
    """
    logger = get_run_logger()
    logger.info("Iniciando extraccion API hidrometrica institucional")
    results = run_hidro_extraction()
    for station, r in results.items():
        if r["status"] == "ok":
            logger.info(f"  {station}: {r['rows']} filas cargadas")
        else:
            logger.error(f"  {station}: FALLO - {r['error']}")
    return results


@task(retries=1, retry_delay_seconds=10)
def run_dbt(command: str, select: Optional[str] = None) -> None:
    logger = get_run_logger()
    cmd = [command]
    if select:
        cmd += ["--select", select]
    cmd += ["--profiles-dir", DBT_PROJECT]
    logger.info(f"dbt {' '.join(cmd)}")
    op = DbtCoreOperation(
        commands=[f"dbt {' '.join(cmd)}"],
        project_dir=DBT_PROJECT,
        profiles_dir=DBT_PROJECT,
    )
    op.run()
    logger.info(f"dbt {command} completado OK")


@flow(name="pipeline-completo", log_prints=True)
def main_pipeline(
    sync_pokeapi: bool = True,
    sync_github: bool = True,
    sync_ecommerce: bool = False,
    sync_hidro: bool = True,
    run_transform: bool = True,
    run_tests: bool = True,
    dbt_select: Optional[str] = None,
) -> None:
    """Pipeline completo: Airbyte/API extract -> dbt transform -> dbt test"""
    logger = get_run_logger()
    logger.info("=== Iniciando pipeline de datos FPUNA ===")

    # 1.a Extraccion via Airbyte (PokeAPI, GitHub, MySQL Ecommerce)
    if sync_pokeapi and CONN_POKEAPI:
        sync_source(CONN_POKEAPI, "PokeAPI")
    if sync_github and CONN_GITHUB:
        sync_source(CONN_GITHUB, "GitHub")
    if sync_ecommerce and CONN_ECOMMERCE and CONN_ECOMMERCE != "PENDIENTE_AGREGAR_MYSQL":
        sync_source(CONN_ECOMMERCE, "MySQL-Ecommerce")

    # 1.b Extraccion via API REST Institucional (datos hidrometricos)
    if sync_hidro:
        extract_hidro_api()

    # 2. Transformacion dbt
    if run_transform:
        run_dbt("run", dbt_select)

    # 3. Tests de calidad
    if run_tests:
        run_dbt("test", dbt_select)

    logger.info("=== Pipeline completado exitosamente ===")


@flow(name="solo-dbt", log_prints=True)
def dbt_only_pipeline(select: Optional[str] = None) -> None:
    """Solo ejecuta dbt sin sincronizar Airbyte (util para desarrollo)"""
    run_dbt("run", select)
    run_dbt("test", select)


if __name__ == "__main__":
    main_pipeline(
        sync_pokeapi=True,
        sync_github=True,
        sync_ecommerce=False,
        sync_hidro=True,
        run_transform=True,
        run_tests=True,
    )
