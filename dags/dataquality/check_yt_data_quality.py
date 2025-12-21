import logging
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)
SOAD_PATH ="/opt/airflow/include/soda"
DATA_SOURCE = "datasource_pg"

def yt_etl_data_quality(schema) :
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema}",
            bash_command=f" soda scan -d {DATA_SOURCE} -c {SOAD_PATH}/configurations.yaml -v SCHEMA={schema} {SOAD_PATH}/checks.yaml"
        )
        
        return task
    except Exception as err:
        logger.error(f"Error in running data quality check for schema- {schema}")