# DAG diario: generación eléctrica de REE -> BigQuery
#
# Cada día a las 06:00 (hora de Madrid) procesa el día anterior, el último completo.
# Para cargar un día concreto: Trigger DAG -> parámetro "fecha" (YYYY-MM-DD).
# La carga sustituye la partición del día, así que reintentos y re-ejecuciones no duplican datos.
#
# El código del pipeline (src/) se monta en /opt/project/src y está en el PYTHONPATH (ver docker-compose.yaml).

from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, task


@dag(
    dag_id="ree_generacion_diaria",
    description="Generación eléctrica diaria por tecnología (API REE) -> BigQuery",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Madrid"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "fecha": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Día a procesar (YYYY-MM-DD). Vacío = el día anterior a la ejecución.",
        )
    },
    tags=["ree", "bigquery", "prueba-tecnica"],
)
def ree_generacion_diaria():

    @task
    def resolve_day(params=None, logical_date=None, dag_run=None) -> str:
        # Día a procesar: el parámetro "fecha" si viene informado; si no, el día anterior a la ejecución
        if params and params.get("fecha"):
            return params["fecha"]
        run_date = logical_date or dag_run.run_after
        return (run_date.in_timezone("Europe/Madrid").date() - timedelta(days=1)).isoformat()

    @task
    def extract(day: str) -> list[dict]:
        from ree_extractor import GenerationExtractor, REEClient

        d = date.fromisoformat(day)
        records = GenerationExtractor(REEClient()).extract(d, d)
        if not records:
            raise ValueError(f"La API de REE no devolvió datos para {day}")
        return [r.to_dict() for r in records]

    @task
    def load(rows: list[dict], day: str) -> int:
        from main import build_loader

        return build_loader().load(rows, date.fromisoformat(day))

    @task
    def validate(loaded: int, day: str) -> None:
        from main import build_loader

        loader = build_loader()
        in_table = loader.count_rows(date.fromisoformat(day))
        if in_table != loaded:
            raise ValueError(f"{day}: se cargaron {loaded} filas pero la tabla tiene {in_table}")
        print(f"OK: {in_table} filas para {day} en {loader.table_id}")
        print(f"URL: {loader.console_url}")

    day = resolve_day()
    rows = extract(day)
    loaded = load(rows, day)
    validate(loaded, day)


ree_generacion_diaria()
