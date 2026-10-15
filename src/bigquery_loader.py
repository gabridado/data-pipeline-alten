import logging
from datetime import date

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryLoader:
    # Carga registros en una tabla de BigQuery particionada por día.
    # Cada carga sustituye la partición del día, así que re-ejecutar un día no duplica filas.

    SCHEMA = [
        bigquery.SchemaField("fecha", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("tecnologia", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tipo", "STRING"),
        bigquery.SchemaField("valor_mwh", "FLOAT"),
        bigquery.SchemaField("porcentaje", "FLOAT"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    def __init__(self, project_id: str, dataset: str, table: str, location: str = "EU"):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = f"{project_id}.{dataset}"
        self.table_id = f"{self.dataset_id}.{table}"
        self.location = location
        # Enlace a la tabla en la consola de BigQuery
        self.console_url = (
            f"https://console.cloud.google.com/bigquery?project={project_id}"
            f"&ws=!1m5!1m4!4m3!1s{project_id}!2s{dataset}!3s{table}"
        )

    def load(self, rows: list[dict], day: date) -> int:
        self._ensure_dataset()
        # El sufijo $YYYYMMDD apunta a la partición de ese día
        destination = f"{self.table_id}${day:%Y%m%d}"
        job_config = bigquery.LoadJobConfig(
            schema=self.SCHEMA,
            time_partitioning=bigquery.TimePartitioning(field="fecha"),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = self.client.load_table_from_json(rows, destination, job_config=job_config)
        job.result()
        logger.info("Cargadas %d filas en %s", job.output_rows, destination)
        return job.output_rows

    def _ensure_dataset(self) -> None:
        dataset = bigquery.Dataset(self.dataset_id)
        dataset.location = self.location
        self.client.create_dataset(dataset, exists_ok=True)
