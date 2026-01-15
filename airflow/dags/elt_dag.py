from datetime import datetime
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
import subprocess
from airflow.sdk import dag, task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    dag_id='elt_and_dbt',
    description='An ELT and dbt taskflow using DockerOperator',
    start_date=datetime(2026, 1, 13),
    schedule='@daily',
    default_args=default_args
)
def elt_and_dbt_taskflow():
    @task(task_id='run_elt_script')
    def run_elt_script():
        result = subprocess.run(
            ['python', '/opt/airflow/elt/elt_script.py'],
            capture_output=True,
        )
        if result.returncode != 0:
            raise Exception(f"ELT script failed: {result.stderr.decode()}")
        else:
            print(result.stdout.decode())
        
    t1 = run_elt_script()

    t2 = DockerOperator(
        task_id='dbt_run',
        image='ghcr.io/dbt-labs/dbt-postgres:latest',
        command=[
            'run',
            '--project-dir', '/opt/dbt',
            '--profiles-dir', '/root/.dbt',
        ],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source='C:/Users/USER/Documents/coding/data/demo-elt/custom_postgres', target='/opt/dbt', type='bind'), # source path from the podman host
            Mount(source='C:/Users/USER/.dbt', target='/root/.dbt', type='bind'), # source path from the podman host
        ],
    )

    t1 >> t2

elt_and_dbt_taskflow()
