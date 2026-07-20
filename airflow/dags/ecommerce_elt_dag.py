from pendulum import datetime
from airflow.sdk import dag, task  # Airflow 3 (Astro Runtime 3.x)
from cosmos import (
    DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig,
)
from cosmos.constants import InvocationMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt_ecommerce"
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_ecommerce",  # phai trung `profile:` trong dbt_project.yml
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"database": "ecommerce_db", "schema": "dev"},
    ),
)


@dag(
    dag_id="ecommerce_elt_dag",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,      # nguon khong partition theo ngay -> backfill vo nghia
    max_active_runs=1,  # 2 run chong nhau se dua nhau replace bang raw
    default_args={"owner": "trinpb04", "retries": 2},
    tags=["ecommerce", "elt"],
)
def ecommerce_elt_dag():

    @task
    def extract_load():
        # v1 full load ca 4 bang (replace, idempotent) - v2 incremental chi cover
        # ecom_sales, chay minh no thi 3 bang dim khong bao gio duoc refresh.
        # Import TRONG task: thieu lib/driver chi lam fail task nay, khong lam
        # ca DAG import error.
        from include.el.pipeline_ecommerce_v1_fullload import run_full_load
        run_full_load()

    transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            # dbt nam trong venv rieng -> phai chay bang subprocess;
            # mac dinh DBT_RUNNER doi hoi dbt cai chung voi Airflow
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        # render (dbt ls luc parse DAG) cung phai tro vao dbt trong venv,
        # vi PATH cua Airflow khong co dbt. RenderConfig co invocation_mode
        # RIENG (mac dinh DBT_RUNNER) -> cung phai ep SUBPROCESS
        render_config=RenderConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        operator_args={"install_deps": True},  # dam bao dbt_utils co mat
    )

    extract_load() >> transform


ecommerce_elt_dag()
