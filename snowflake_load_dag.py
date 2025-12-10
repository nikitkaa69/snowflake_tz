from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

FILE_PATH = '/opt/airflow/data/Airline_Dataset.csv'
TABLE_NAME = 'AIRLINE_DWH.RAW.AIRLINE_FLIGHTS_RAW'
STAGE_NAME = 'AIRLINE_DWH.RAW.AIRLINE_FLIGHTS_STAGE'

default_args = {
    'owner': 'airflow',
    'conn_id': 'snowflake_default'
}


@dag(
    dag_id='snowflake_load_dag',
    default_args=default_args,
    schedule=None,
    start_date=pendulum.today('UTC'),
    catchup=False,
    tags=['snowflake']
)
def snowflake_load_dag():
    # Upload local CSV to Snowflake Stage
    upload_to_stage = SQLExecuteQueryOperator(
        task_id='upload_to_stage',
        sql=f'PUT file://{FILE_PATH} @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;'
    )

    # Copy data from Stage to RAW table
    copy_into_table = SQLExecuteQueryOperator(
        task_id='copy_into_table',
        sql=f'''COPY INTO {TABLE_NAME} FROM @{STAGE_NAME}
                FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                ON_ERROR = 'ABORT_STATEMENT'
                FORCE = TRUE; '''
    )

    # Transform RAW -> CORE (Stored Procedure)
    call_core_procedure = SQLExecuteQueryOperator(
        task_id='call_procedure',
        sql='CALL AIRLINE_DWH.CORE.LOAD_CORE_LAYER();'
    )

    # Transform CORE -> MARTS (Stored Procedure)
    call_marts_procedure = SQLExecuteQueryOperator(
        task_id='call_marts_procedure',
        sql='CALL AIRLINE_DWH.MARTS.LOAD_MARTS_LAYER();'
    )

    upload_to_stage >> copy_into_table >> call_core_procedure >> call_marts_procedure


snowflake_load_dag()
