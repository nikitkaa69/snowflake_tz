from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
import config
import snowflake_sql

default_args = {
    'owner': 'airflow',
}


def run_snowflake_query(query):
    hook = SnowflakeHook(snowflake_conn_id=config.CONN_ID)
    hook.run(query)


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
    @task
    def upload_to_stage():
        run_snowflake_query(snowflake_sql.UPLOAD_TO_STAGE_SQL)
        print(f"File {config.FILE_PATH} uploaded to stage successfully.")

    # Copy data from Stage to RAW table
    @task
    def copy_into_table():
        run_snowflake_query(snowflake_sql.COPY_INTO_TABLE_SQL)
        print("Data copied from Stage to RAW table")

    # Transform RAW -> CORE (Stored Procedure)
    @task
    def call_core_procedure():
        run_snowflake_query(snowflake_sql.CALL_CORE_PROC_SQL)
        print("Core layer procedure executed")

    # Transform CORE -> MARTS (Stored Procedure)
    @task
    def call_marts_procedure():
        run_snowflake_query(snowflake_sql.CALL_MARTS_PROC_SQL)
        print("Marts layer procedure executed")

    upload_to_stage() >> copy_into_table() >> call_core_procedure() >> call_marts_procedure()


dag_instance = snowflake_load_dag()
