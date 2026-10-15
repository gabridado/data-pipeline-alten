import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class GenerationRecord:
    #Generación eléctrica diaria en España y por tecnología. Nos va a sacar fecha, tech, tipo (renovabble/no), energía, porcentaje y fecha ingesta en UTC.

    fecha: str
    tecnologia: str
    tipo: str
    valor_mwh: float
    porcentaje: float
    ingested_at: str

    def to_dict(self) -> dict: # a dict para usarlo en BQ
        return asdict(self)


class REEClient:
    #Cliente HTTP de la API de REE
    BASE_URL = "https://apidatos.ree.es/es/datos"

    def __init__(self, timeout: int = 30, max_retries: int = 3):
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def get(self, category: str, widget: str, start: date, end: date, time_trunc: str) -> dict:
        url = f"{self.BASE_URL}/{category}/{widget}"
        params = {
            "start_date": f"{start.isoformat()}T00:00",
            "end_date": f"{end.isoformat()}T23:59",
            "time_trunc": time_trunc,
        }
        logger.info("GET %s %s", url, params)
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()


class GenerationExtractor:
    #Descarga la estructura de generación por tecnología y la convierte en filas planas

    CATEGORY = "generacion"
    WIDGET = "estructura-generacion"
    # La API incluye el total junto a las tecnologías; se descarta para no duplicar sumas.
    EXCLUDED = {"Generación total"}

    def __init__(self, client: REEClient):
        self.client = client

    def extract(self, start: date, end: date) -> list[GenerationRecord]:
        raw = self.client.get(self.CATEGORY, self.WIDGET, start, end, time_trunc="day")
        records = self._transform(raw)
        logger.info("Extraídas %d filas entre %s y %s", len(records), start, end)
        return records

    def _transform(self, raw: dict) -> list[GenerationRecord]:
        ingested_at = datetime.now(timezone.utc).isoformat()
        records = []
        for item in raw.get("included", []):
            attrs = item["attributes"]
            if attrs["title"] in self.EXCLUDED:
                continue
            for value in attrs["values"]:
                records.append(
                    GenerationRecord(
                        fecha=value["datetime"][:10],
                        tecnologia=attrs["title"],
                        tipo=attrs.get("type") or "Desconocido",
                        valor_mwh=float(value["value"]),
                        porcentaje=float(value["percentage"]),
                        ingested_at=ingested_at,
                    )
                )
        return records
