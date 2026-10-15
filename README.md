# data-pipeline-alten
Prueba técnica 20261510

Pipeline de datos que descarga la **generación eléctrica diaria en España por tecnología** desde la API pública de
[Red Eléctrica de España (REData)](https://www.ree.es/es/datos/apidatos) y la carga en **BigQuery**.

```
API REE  ──>  Python (extracción + transformación)  ──>  BigQuery
                                                         alten-data.ree_energy.generacion_diaria
```

## Estructura

```
data-pipeline-alten/
├── src/
│   ├── ree_extractor.py     # Cliente de la API de REE y transformación a filas
│   ├── bigquery_loader.py   # Carga en BigQuery
│   └── main.py              # Punto de entrada del pipeline
├── airflow/                 # Orquestación con Airflow (Parte 3)
├── keys/                    # Credenciales de GCP (ignorado por git, nunca se sube)
└── requirements.txt
```

## Datos

Una fila por día y tecnología (eólica, nuclear, solar fotovoltaica, ciclo combinado...):

| Campo         | Tipo      | Descripción                                   |
|---------------|-----------|-----------------------------------------------|
| `fecha`       | DATE      | Día de los datos (columna de partición)       |
| `tecnologia`  | STRING    | Fuente de generación                          |
| `tipo`        | STRING    | `Renovable` / `No-Renovable`                  |
| `valor_mwh`   | FLOAT     | Energía generada ese día (MWh)                |
| `porcentaje`  | FLOAT     | Peso sobre el total del día, en tanto por uno |
| `ingested_at` | TIMESTAMP | Momento de la carga (UTC)                     |

## Instalación

Requiere Python 3.11+.

```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### Credenciales de GCP

El pipeline usa una service account con los roles **BigQuery Data Editor** y **BigQuery Job User**.
Su clave JSON se guarda en `keys/gcp-key.json` (la carpeta `keys/` está en `.gitignore`).

Alternativas:
- Definir `GOOGLE_APPLICATION_CREDENTIALS` con la ruta a otra clave.
- Sin clave: `gcloud auth application-default login` (Application Default Credentials).

## Uso

```bash
python src/main.py                                # procesa ayer y lo carga en BigQuery
python src/main.py --date 2026-10-13              # procesa un día concreto
python src/main.py --date 2026-10-13 --no-load    # solo extrae y muestra la tabla
```

Configuración opcional por variables de entorno:

| Variable         | Por defecto         |
|------------------|---------------------|
| `GCP_PROJECT_ID` | `alten-data`        |
| `BQ_DATASET`     | `ree_energy`        |
| `BQ_TABLE`       | `generacion_diaria` |

