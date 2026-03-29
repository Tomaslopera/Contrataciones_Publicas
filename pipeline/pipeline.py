from datetime import datetime, timedelta
import logging
from typing import Dict, Any

import pandas as pd
from sodapy import Socrata
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['tomas@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'secop_ii_etl_pipeline',
    default_args=default_args,
    description='Extract, transform, and load Colombian procurement data (SECOP II)',
    schedule='0 2 * * *',
    catchup=False,
    tags=['data-pipeline', 'procurement', 'secop'],
    doc_md=__doc__,
)

# Data paths (use Airflow Variables for environment-specific paths)
RAW_DATA_PATH = Variable.get('secop_raw_data_path', '../data/raw/contrataciones_publicas.csv')
STAGING_DATA_PATH = Variable.get('secop_staging_data_path', '../data/staging/contrataciones_publicas_staging.csv')
PROCESSED_DATA_PATH = Variable.get('secop_processed_data_path', '../data/processed/')

# SODA API Configuration
SODA_DOMAIN = 'www.datos.gov.co'
SODA_DATASET_ID = 'jbjy-vk9h'
SODA_LIMIT = 2000

# ============================================================================
# Extract Tasks
# ============================================================================

def extract_from_soda_api(**context) -> Dict[str, Any]:
    execution_date = context['execution_date']
    
    try:
        logger.info(f"Connecting to SODA API at {SODA_DOMAIN}...")
        client = Socrata(SODA_DOMAIN, None)
        
        logger.info(f"Fetching dataset {SODA_DATASET_ID} with limit {SODA_LIMIT}...")
        results = client.get(SODA_DATASET_ID, limit=SODA_LIMIT)
        
        if not results:
            raise AirflowException("No data returned from SODA API")
        
        logger.info(f"Successfully fetched {len(results)} records from SODA API")
        
        data = pd.DataFrame.from_records(results)
        
        # Basic validation
        if data.empty:
            raise AirflowException("DataFrame is empty after API fetch")
        
        # Save raw data
        logger.info(f"Saving raw data to {RAW_DATA_PATH}...")
        data.to_csv(RAW_DATA_PATH, index=False)
        
        # Push metadata for downstream tasks
        context['task_instance'].xcom_push(
            key='extraction_metadata',
            value={
                'record_count': len(results),
                'column_count': len(data.columns),
                'execution_date': execution_date.isoformat(),
            }
        )
        
        logger.info("Extract task completed successfully")
        return {'status': 'success', 'records': len(results)}
        
    except Exception as e:
        logger.error(f"Extract task failed: {str(e)}")
        raise AirflowException(f"SODA API extraction failed: {str(e)}")


# ============================================================================
# Transform Stage 1: Data Cleaning & Column Standardization
# ============================================================================

def delete_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Remove unnecessary columns and NaN rows."""
    columns_to_drop = [
        'ultima_actualizacion',
        'fecha_inicio_liquidacion',
        'fecha_fin_liquidacion',
        'fecha_de_notificaci_n_de_prorrogaci_n',
        'localizaci_n'
    ]
    data = data.drop(columns=[col for col in columns_to_drop if col in data.columns])
    data = data.dropna()
    return data


def rename_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names (remove special characters, fix encoding)."""
    rename_mapping = {
        'liquidaci_n': 'liquidacion',
        'obligaci_n_ambiental': 'oblicacion_ambiental',
        'tipo_de_identificaci_n_representante_legal': 'tipo_identificacion_representante_legal',
        'identificaci_n_representante_legal': 'identificacion_representante_legal',
        'g_nero_representante_legal': 'genero_representante_legal',
        'sistema_general_de_regal_as': 'sistema_general_de_regalias',
        'recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_': 'recursos_propios_alcaldias_gobernaciones_y_resguardos_indigenas',
        'duraci_n_del_contrato': 'duracion_del_contrato',
        'n_mero_de_cuenta': 'numero_de_cuenta',
        'n_mero_de_documento_ordenador_del_gasto': 'numero_de_documento_ordenador_del_gasto',
        'n_mero_de_documento_supervisor': 'numero_de_documento_supervisor',
        'n_mero_de_documento_ordenador_de_pago': 'numero_de_documento_ordenador_de_pago',
    }
    data = data.rename(columns=rename_mapping)
    return data


def date_transformations(data: pd.DataFrame) -> pd.DataFrame:
    """Convert date columns to datetime type."""
    date_columns = [
        'fecha_de_firma',
        'fecha_de_inicio_del_contrato',
        'fecha_de_fin_del_contrato'
    ]
    for col in date_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], errors='coerce')
    return data


def numeric_transformations(data: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns to proper types."""
    numeric_columns = [
        "valor_del_contrato", "valor_de_pago_adelantado", "valor_facturado",
        "valor_pendiente_de_pago", "valor_pagado", "valor_amortizado",
        "valor_pendiente_de", "valor_pendiente_de_ejecucion", "saldo_cdp",
        "saldo_vigencia", "dias_adicionados", "sistema_general_de_participaciones",
        "sistema_general_de_regalias", "recursos_propios_alcaldias_gobernaciones_y_resguardos_indigenas",
        "recursos_de_credito", "recursos_propios"
    ]
    for col in numeric_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')
    return data


def boolean_transformations(data: pd.DataFrame) -> pd.DataFrame:
    """Convert boolean columns (SI/NO) to True/False."""
    boolean_columns = [
        "es_grupo", "es_pyme", "liquidacion", "oblicacion_ambiental",
        "obligaciones_postconsumo", "reversion", "espostconflicto",
        "el_contrato_puede_ser_prorrogado"
    ]
    for col in boolean_columns:
        if col in data.columns:
            data[col] = (
                data[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({'si': True, 'no': False})
            )
    return data


def uppercase_transformations(data: pd.DataFrame) -> pd.DataFrame:
    """Normalize text columns to uppercase and clean special characters."""
    text_columns = [
        "nombre_entidad",
        "descripcion_del_proceso",
        "proveedor_adjudicado",
        "nombre_representante_legal",
        "objeto_del_contrato",
        "nombre_ordenador_del_gasto",
        "nombre_supervisor",
        "nombre_ordenador_de_pago",
    ]
    for col in text_columns:
        if col in data.columns:
            data[col] = (
                data[col]
                .str.upper()
                .str.replace("*", "", regex=False)
                .str.replace("**", "", regex=False)
                .str.replace("****", "", regex=False)
            )
    return data


def url_transformations(data: pd.DataFrame) -> pd.DataFrame:
    """Extract clean URL from nested format."""
    if 'urlproceso' in data.columns:
        data["urlproceso"] = data["urlproceso"].str.extract(r"url':\s*'([^']+)'")
    return data


def duration_transformations(data: pd.DataFrame) -> pd.DataFrame:
    """Split contract duration into value and unit."""
    if 'duracion_del_contrato' in data.columns:
        data[["duracion_valor", "duracion_unidad"]] = (
            data["duracion_del_contrato"]
            .str.extract(r"(\d+)\s*(Mes\(es\)|Dia\(s\))")
        )
        data = data.drop(columns=['duracion_del_contrato'])
    return data


def clean_and_stage_data(**context) -> Dict[str, Any]:
    """
    Orchestrate all cleaning and transformation steps (Stage 1).
    
    Implements the full transformation pipeline in a single task
    for simplicity and performance (avoids multiple I/O operations).
    """
    try:
        logger.info(f"Reading raw data from {RAW_DATA_PATH}...")
        data = pd.read_csv(RAW_DATA_PATH)
        initial_rows = len(data)
        
        logger.info("Starting Stage 1: Data Cleaning & Standardization...")
        
        # Apply transformations in order
        data = delete_columns(data)
        logger.info(f"After delete_columns: {len(data)} rows")
        
        data = rename_columns(data)
        data = date_transformations(data)
        data = numeric_transformations(data)
        data = boolean_transformations(data)
        data = uppercase_transformations(data)
        data = url_transformations(data)
        data = duration_transformations(data)
        
        final_rows = len(data)
        logger.info(f"Stage 1 complete: {initial_rows} → {final_rows} rows")
        
        # Save staging data
        logger.info(f"Writing staging data to {STAGING_DATA_PATH}...")
        data.to_csv(STAGING_DATA_PATH, index=False)
        
        # Push metadata
        context['task_instance'].xcom_push(
            key='staging_metadata',
            value={
                'initial_rows': initial_rows,
                'final_rows': final_rows,
                'rows_dropped': initial_rows - final_rows,
                'columns': len(data.columns),
            }
        )
        
        logger.info("Stage 1 (Clean & Stage) task completed successfully")
        return {'status': 'success', 'rows_staged': final_rows}
        
    except Exception as e:
        logger.error(f"Stage 1 task failed: {str(e)}")
        raise AirflowException(f"Data cleaning failed: {str(e)}")


# ============================================================================
# Transform Stage 2: Dimensional Modeling (Star Schema)
# ============================================================================

import uuid

def create_geography_dimension(data: pd.DataFrame) -> pd.DataFrame:
    """Create geography dimension (departamento, ciudad)."""
    dim = data[['departamento', 'ciudad']].drop_duplicates().reset_index(drop=True)
    dim["id_geografia"] = dim.apply(lambda _: str(uuid.uuid4()), axis=1)
    return dim[["id_geografia", "departamento", "ciudad"]]


def create_date_dimensions(data: pd.DataFrame) -> tuple:
    """Create three date dimensions (firma, inicio, fin)."""
    def build_date_dim(date_col_name: str) -> pd.DataFrame:
        dim = data[[date_col_name]].drop_duplicates().sort_values(by=date_col_name).reset_index(drop=True)
        dim["year"] = pd.to_datetime(dim[date_col_name], errors='coerce').dt.year
        dim["month"] = pd.to_datetime(dim[date_col_name], errors='coerce').dt.month
        dim["day"] = pd.to_datetime(dim[date_col_name], errors='coerce').dt.day
        return dim.rename(columns={date_col_name: 'fecha'})
    
    dim_firma = build_date_dim('fecha_de_firma')
    dim_inicio = build_date_dim('fecha_de_inicio_del_contrato')
    dim_fin = build_date_dim('fecha_de_fin_del_contrato')
    
    return dim_firma, dim_inicio, dim_fin


def create_details_dimension(data: pd.DataFrame) -> pd.DataFrame:
    """Create contract details dimension."""
    dim = data[[
        'id_contrato', 'referencia_del_contrato', 'estado_contrato',
        'codigo_de_categoria_principal', 'descripcion_del_proceso',
        'tipo_de_contrato', 'modalidad_de_contratacion', 'justificacion_modalidad_de'
    ]].drop_duplicates().reset_index(drop=True)
    dim["id_detalles"] = dim.apply(lambda _: str(uuid.uuid4()), axis=1)
    return dim[[
        "id_detalles", "id_contrato", "referencia_del_contrato", "estado_contrato",
        "codigo_de_categoria_principal", "descripcion_del_proceso", "tipo_de_contrato",
        "modalidad_de_contratacion", "justificacion_modalidad_de"
    ]]


def create_account_dimension(data: pd.DataFrame) -> pd.DataFrame:
    """Create bank account dimension."""
    dim = data[['id_contrato', 'nombre_del_banco', 'tipo_de_cuenta', 'numero_de_cuenta']]
    dim["id_cuenta"] = dim.apply(lambda _: str(uuid.uuid4()), axis=1)
    return dim[["id_cuenta", "id_contrato", "nombre_del_banco", "tipo_de_cuenta", "numero_de_cuenta"]]


def create_representative_dimension(data: pd.DataFrame) -> pd.DataFrame:
    """Create legal representative dimension."""
    dim = data[[
        'id_contrato', 'identificacion_representante_legal', 'nombre_representante_legal',
        'nacionalidad_representante_legal', 'domicilio_representante_legal',
        'tipo_identificacion_representante_legal', 'genero_representante_legal'
    ]].drop_duplicates().reset_index(drop=True)
    dim["id_representante"] = dim.apply(lambda _: str(uuid.uuid4()), axis=1)
    return dim[[
        "id_representante", "id_contrato", "identificacion_representante_legal",
        "nombre_representante_legal", "nacionalidad_representante_legal",
        "domicilio_representante_legal", "tipo_identificacion_representante_legal",
        "genero_representante_legal"
    ]]


def create_characteristics_dimension(data: pd.DataFrame) -> pd.DataFrame:
    """Create general characteristics dimension."""
    dim = data[[
        'es_grupo', 'es_pyme', 'liquidacion', 'oblicacion_ambiental',
        'espostconflicto', 'origen_de_los_recursos', 'destino_gasto'
    ]].drop_duplicates().reset_index(drop=True)
    dim["id_caracteristicas"] = range(1, len(dim) + 1)
    return dim[[
        "id_caracteristicas", "es_grupo", "es_pyme", "liquidacion",
        "oblicacion_ambiental", "espostconflicto", "origen_de_los_recursos",
        "destino_gasto"
    ]]


def create_provider_dimension(data: pd.DataFrame) -> pd.DataFrame:
    """Create provider dimension."""
    dim = data[[
        'id_contrato', 'codigo_proveedor', 'tipodocproveedor',
        'documento_proveedor', 'proveedor_adjudicado'
    ]].drop_duplicates().reset_index(drop=True)
    return dim


def dimensionalize_data(**context) -> Dict[str, Any]:
    """
    Orchestrate dimensional modeling (Stage 2).
    
    Creates a star schema with one fact table and multiple dimension tables.
    """
    try:
        logger.info(f"Reading staging data from {STAGING_DATA_PATH}...")
        data = pd.read_csv(STAGING_DATA_PATH)
        
        logger.info("Starting Stage 2: Dimensional Modeling...")
        
        # Create all dimensions
        logger.info("Creating geography dimension...")
        dim_geografia = create_geography_dimension(data)
        
        logger.info("Creating date dimensions...")
        dim_firma, dim_inicio, dim_fin = create_date_dimensions(data)
        
        logger.info("Creating details dimension...")
        dim_detalles = create_details_dimension(data)
        
        logger.info("Creating account dimension...")
        dim_cuenta = create_account_dimension(data)
        
        logger.info("Creating representative dimension...")
        dim_representante = create_representative_dimension(data)
        
        logger.info("Creating characteristics dimension...")
        dim_caracteristicas = create_characteristics_dimension(data)
        
        logger.info("Creating provider dimension...")
        dim_proveedor = create_provider_dimension(data)
        
        # Build fact table with foreign keys
        logger.info("Creating fact table (building lookups)...")
        fact_contrato = data[[
            'id_contrato', 'codigo_proveedor', 'fecha_de_firma',
            'fecha_de_inicio_del_contrato', 'fecha_de_fin_del_contrato',
            'valor_del_contrato', 'valor_de_pago_adelantado', 'valor_facturado',
            'valor_pendiente_de_pago', 'valor_pagado', 'valor_amortizado',
            'valor_pendiente_de', 'valor_pendiente_de_ejecucion',
            'saldo_cdp', 'saldo_vigencia', 'departamento', 'ciudad',
            'es_grupo', 'es_pyme', 'liquidacion', 'oblicacion_ambiental',
            'espostconflicto', 'origen_de_los_recursos', 'destino_gasto'
        ]].copy()
        
        # Add foreign keys via lookups
        lookup_geografia = dim_geografia.set_index(['departamento', 'ciudad'])['id_geografia'].to_dict()
        fact_contrato['id_geografia'] = fact_contrato.apply(
            lambda row: lookup_geografia.get((row['departamento'], row['ciudad'])),
            axis=1
        )
        fact_contrato = fact_contrato.drop(columns=['departamento', 'ciudad'])
        
        lookup_cuenta = dim_cuenta.set_index('id_contrato')['id_cuenta'].to_dict()
        fact_contrato['id_cuenta'] = fact_contrato['id_contrato'].map(lookup_cuenta)
        
        lookup_detalles = dim_detalles.set_index('id_contrato')['id_detalles'].to_dict()
        fact_contrato['id_detalles'] = fact_contrato['id_contrato'].map(lookup_detalles)
        
        lookup_representante = dim_representante.set_index('id_contrato')['id_representante'].to_dict()
        fact_contrato['id_representante'] = fact_contrato['id_contrato'].map(lookup_representante)
        
        lookup_caracteristicas = dim_caracteristicas.set_index([
            'es_grupo', 'es_pyme', 'liquidacion', 'oblicacion_ambiental',
            'espostconflicto', 'origen_de_los_recursos', 'destino_gasto'
        ])['id_caracteristicas'].to_dict()
        fact_contrato['id_caracteristicas'] = fact_contrato.apply(
            lambda row: lookup_caracteristicas.get((
                row['es_grupo'], row['es_pyme'], row['liquidacion'],
                row['oblicacion_ambiental'], row['espostconflicto'],
                row['origen_de_los_recursos'], row['destino_gasto']
            )),
            axis=1
        )
        fact_contrato = fact_contrato.drop(columns=[
            'es_grupo', 'es_pyme', 'liquidacion', 'oblicacion_ambiental',
            'espostconflicto', 'origen_de_los_recursos', 'destino_gasto'
        ])
        
        # Save all tables
        logger.info(f"Writing dimension and fact tables to {PROCESSED_DATA_PATH}...")
        fact_contrato.to_csv(f"{PROCESSED_DATA_PATH}fact_contrato.csv", index=False)
        dim_geografia.to_csv(f"{PROCESSED_DATA_PATH}dim_geografia.csv", index=False)
        dim_firma.to_csv(f"{PROCESSED_DATA_PATH}dim_fecha_firma.csv", index=False)
        dim_inicio.to_csv(f"{PROCESSED_DATA_PATH}dim_fecha_inicio.csv", index=False)
        dim_fin.to_csv(f"{PROCESSED_DATA_PATH}dim_fecha_fin.csv", index=False)
        dim_detalles.to_csv(f"{PROCESSED_DATA_PATH}dim_detalles.csv", index=False)
        dim_cuenta.to_csv(f"{PROCESSED_DATA_PATH}dim_cuenta.csv", index=False)
        dim_representante.to_csv(f"{PROCESSED_DATA_PATH}dim_representante.csv", index=False)
        dim_caracteristicas.to_csv(f"{PROCESSED_DATA_PATH}dim_caracteristicas.csv", index=False)
        dim_proveedor.to_csv(f"{PROCESSED_DATA_PATH}dim_proveedor.csv", index=False)
        
        logger.info("Stage 2 (Dimensionalize) completed successfully")
        return {
            'status': 'success',
            'fact_rows': len(fact_contrato),
            'dimensions_created': 9
        }
        
    except Exception as e:
        logger.error(f"Stage 2 task failed: {str(e)}")
        raise AirflowException(f"Dimensional modeling failed: {str(e)}")


# ============================================================================
# DAG Task Definition
# ============================================================================

# Start
start = EmptyOperator(task_id='start', dag=dag)

# Extract phase
extract = PythonOperator(
    task_id='extract_from_soda_api',
    python_callable=extract_from_soda_api,
    dag=dag,
)

# Transform Phase 1: Clean & Stage
clean_stage = PythonOperator(
    task_id='clean_and_stage_data',
    python_callable=clean_and_stage_data,
    dag=dag,
)

# Transform Phase 2: Dimensionalize
dimensionalize = PythonOperator(
    task_id='dimensionalize_data',
    python_callable=dimensionalize_data,
    dag=dag,
)

# End
end = EmptyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> extract >> clean_stage >> dimensionalize >> end