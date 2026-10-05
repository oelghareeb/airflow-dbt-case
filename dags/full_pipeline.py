import os
import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

WORKSPACE_DIR = "/workspaces/airflow-dbt-case"
DBT_DIR = os.path.join(WORKSPACE_DIR, "dbt_airflow_project")
DB_PATH = os.path.join(DBT_DIR, "dbt_airflow_project.duckdb")

@dag(
    dag_id="airflow_dbt_duckdb_elt_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["production", "dbt", "duckdb", "elt"],
)
def elt_pipeline():
    """
    ### Airflow + dbt + DuckDB ELT Pipeline
    1. Seed CSV raw data into DuckDB.
    2. Transform raw data with dbt models.
    3. Test transformed data using dbt test constraints.
    4. Validate output tables directly in DuckDB.
    """

    # Task 1: Load raw seed files (raw_customers, raw_orders, raw_payments) into DuckDB
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    # Task 2: Execute dbt transformation models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    # Task 3: Validate models using dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    # Task 4: Python data quality checks on DuckDB output
    @task()
    def validate_duckdb_output():
        import duckdb

        db_filepath = DB_PATH if os.path.exists(DB_PATH) else os.path.join(WORKSPACE_DIR, "dbt_airflow_project.duckdb")

        if not os.path.exists(db_filepath):
            raise FileNotFoundError(f"DuckDB database not found at {db_filepath}")

        conn = duckdb.connect(db_filepath, read_only=True)
        
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        table_names = [t[0] for t in tables]

        if not table_names:
            conn.close()
            raise ValueError("Data Quality Check Failed: No tables found in DuckDB 'main' schema!")

        print(f"Data Quality Check Passed! Found tables in DuckDB: {table_names}")
        conn.close()

    # Task Execution Order
    dbt_seed >> dbt_run >> dbt_test >> validate_duckdb_output()


airflow_dbt_duckdb_elt_pipeline = elt_pipeline()