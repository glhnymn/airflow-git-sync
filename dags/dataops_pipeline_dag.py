from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators. python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'dataops_pipeline',
    default_args=default_args,
    description='DataOps Pipeline:  MinIO -> Clean -> PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['dataops', 'etl', 'production']
)

def log_start():
    """Pipeline başlangıcını logla"""
    logger.info("=" * 80)
    logger.info("DataOps Pipeline Started")
    logger.info(f"Execution Date: {datetime.now()}")
    logger.info("=" * 80)

def log_end():
    """Pipeline bitişini logla"""
    logger.info("=" * 80)
    logger.info("DataOps Pipeline Completed Successfully")
    logger.info(f"Completion Time: {datetime.now()}")
    logger.info("=" * 80)

# Task 1: Start logging
start_task = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_start,
    dag=dag
)

# Task 2: Copy Python script to spark_client
copy_script = SSHOperator(
    task_id='copy_python_script_to_spark',
    ssh_conn_id='spark_ssh',
    command="""
    # GitHub'dan direkt koda çekmek için
    apt-get update && apt-get install -y git
    cd /dataops
    
    # Eğer repo varsa güncelle, yoksa klonla
    if [ -d "airflow-git-sync" ]; then
        cd airflow-git-sync
        git pull origin main
    else
        git clone https://github.com/glhnymn/airflow-git-sync.git
        cd airflow-git-sync
    fi
    
    # Script'i kontrol et
    ls -lh dataops_demo/clean_data.py
    echo "Script copied successfully"
    """,
    cmd_timeout=300,
    dag=dag
)

# Task 3: Run data cleaning script on spark_client
run_cleaning = SSHOperator(
    task_id='run_data_cleaning_on_spark',
    ssh_conn_id='spark_ssh',
    command="""
    cd /dataops/airflow-git-sync/dataops_demo
    python3 clean_data.py
    """,
    cmd_timeout=600,
    dag=dag
)

# Task 4: Verify data in PostgreSQL
verify_data = SSHOperator(
    task_id='verify_data_in_postgres',
    ssh_conn_id='spark_ssh',
    command="""
    python3 -c "
from sqlalchemy import create_engine
engine = create_engine('postgresql://train:Ankara06@traindb: 5432/traindb')
with engine.connect() as conn:
    result = conn.execute('SELECT COUNT(*) FROM clean_data_transactions;')
    count = result.fetchone()[0]
    print(f'Total rows in clean_data_transactions: {count}')
    if count == 0:
        raise Exception('No data found in table!')
    print('Data verification successful!')
"
    """,
    cmd_timeout=120,
    dag=dag
)

# Task 5: End logging
end_task = PythonOperator(
    task_id='log_pipeline_end',
    python_callable=log_end,
    dag=dag
)

# Task dependencies
start_task >> copy_script >> run_cleaning >> verify_data >> end_task
