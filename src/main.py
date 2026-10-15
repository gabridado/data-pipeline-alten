
####### USO #######
# python src/main.py ---> ayer
# python src/main.py --date 2026-10-13 ---> otro día
# python src/main.py --no-load ---> solo extrae y muestra la tabla, sin subir a BigQuery
########################################################################################

import argparse
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from ree_extractor import GenerationExtractor, GenerationRecord, REEClient

# Configuración (se puede sobrescribir con variables de entorno)
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "alten-data")
DATASET = os.getenv("BQ_DATASET", "ree_energy")
TABLE = os.getenv("BQ_TABLE", "generacion_diaria")
DEFAULT_KEY = Path(__file__).resolve().parent.parent / "keys" / "gcp-key.json"


class REEPipeline:
    # Orquestración principal: extrae un día y, si hay loader, lo carga en BigQuery

    def __init__(self, extractor: GenerationExtractor, loader=None):
        self.extractor = extractor
        self.loader = loader

    def run(self, day: date) -> list[GenerationRecord]:
        records = self.extractor.extract(day, day)
        if self.loader and records:
            self.loader.load([r.to_dict() for r in records], day)
        return records


def print_table(records: list[GenerationRecord]) -> None:
    df = pd.DataFrame([r.to_dict() for r in records])
    print(df.sort_values("valor_mwh", ascending=False).to_string(index=False))


def build_loader():
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ and DEFAULT_KEY.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(DEFAULT_KEY)
    from bigquery_loader import BigQueryLoader

    return BigQueryLoader(PROJECT_ID, DATASET, TABLE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline REE -> BigQuery")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
        help="Día a procesar (YYYY-MM-DD). Por defecto, ayer.",
    )
    parser.add_argument("--no-load", action="store_true", help="No cargar en BigQuery")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = parse_args()
    loader = None if args.no_load else build_loader()
    pipeline = REEPipeline(GenerationExtractor(REEClient()), loader)
    records = pipeline.run(args.date)
    print_table(records)
    logging.info("***********************")
    logging.info("F I N: %d filas", len(records))
    if loader:
        logging.info("Tabla cargada en %s", loader.table_id)
        logging.info("URL: %s", loader.console_url)
    logging.info("***********************")
