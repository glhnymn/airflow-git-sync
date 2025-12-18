"""
Data Cleaning DAG for Store Transactions
- Reads dirty data from MinIO
- Cleans data using Pandas on spark_client
- Writes clean data to PostgreSQL traindb
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'store_transactions_cleaning',
    default_args=default_args,
    description='Clean store transactions data from MinIO and load to PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 18),
    catchup=False,
    tags=['dataops', 'data-cleaning', 'minio', 'postgresql'],
) as dag:

    # Task 1: Check MinIO bucket and file (via SSH on spark_client)
    check_source = SSHOperator(
        task_id='check_minio_source',
        ssh_conn_id='spark_client_ssh',
        command="""
        python3 -c "
from minio import Minio
client = Minio('minio:9000', access_key='dataopsadmin', secret_key='dataopsadmin', secure=False)
if not client.bucket_exists('dataops-bronze'):
    raise Exception('Bucket does not exist!')
client.stat_object('dataops-bronze', 'raw/dirty_store_transactions.csv')
print('✓ File exists in MinIO')
        "
        """,
    )

    # Task 2: Copy script to spark_client
    copy_script = SSHOperator(
        task_id='copy_script_to_spark_client',
        ssh_conn_id='spark_client_ssh',
        command="""
        mkdir -p /dataops/scripts
        cat > /dataops/scripts/clean_transactions.py << 'EOFPYTHON'
#!/usr/bin/env python3
import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
import io
import sys

def clean_store_transactions():
    minio_client = Minio(
        "minio:9000",
        access_key="dataopsadmin",
        secret_key="dataopsadmin",
        secure=False
    )
    
    print("Reading data from MinIO...")
    try:
        response = minio_client.get_object("dataops-bronze", "raw/dirty_store_transactions.csv")
        df = pd.read_csv(io.BytesIO(response.read()))
        print(f"Successfully read {len(df)} rows from MinIO")
    except Exception as e:
        print(f"Error reading from MinIO: {e}")
        sys.exit(1)
    
    print(f"Original data shape: {df.shape}")
    print(f"Missing values:\\n{df.isnull().sum()}")
    print(f"Duplicates: {df.duplicated().sum()}")
    
    print("\\nCleaning data...")
    df_clean = df.drop_duplicates()
    print(f"After removing duplicates: {len(df_clean)} rows")
    
    df_clean = df_clean.dropna(subset=['STORE_ID', 'STORE_LOCATION'])
    
    numeric_columns = df_clean.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        if df_clean[col].isnull().sum() > 0:
            df_clean[col].fillna(df_clean[col].median(), inplace=True)
    
    categorical_columns = df_clean.select_dtypes(include=['object']).columns
    for col in categorical_columns:
        if df_clean[col].isnull().sum() > 0:
            df_clean[col].fillna('Unknown', inplace=True)
    
    print(f"After cleaning: {df_clean.shape}")
    print(f"Remaining missing values: {df_clean.isnull().sum().sum()}")
    
    print("\\nWriting cleaned data to PostgreSQL...")
    try:
        engine = create_engine('postgresql://train:train123@traindb:5432/traindb')
        df_clean.to_sql(
            'clean_store_transactions',
            engine,
            schema='public',
            if_exists='replace',
            index=False
        )
        print(f"Successfully wrote {len(df_clean)} rows to traindb.public.clean_store_transactions")
        
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM public.clean_store_transactions")
            count = result.scalar()
            print(f"Verification: Table has {count} rows")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
        sys.exit(1)
    
    print("\\nData cleaning pipeline completed successfully!")
    return True

if __name__ == "__main__":
    clean_store_transactions()
EOFPYTHON
        chmod +x /dataops/scripts/clean_transactions.py
        echo "Script copied successfully"
        """,
    )

    # Task 3: Install dependencies on spark_client
    install_deps = SSHOperator(
        task_id='install_dependencies',
        ssh_conn_id='spark_client_ssh',
        command="""
        pip install minio pandas sqlalchemy psycopg2-binary
        echo "Dependencies installed successfully"
        """,
    )

    # Task 4: Run data cleaning script on spark_client
    run_cleaning = SSHOperator(
        task_id='run_data_cleaning',
        ssh_conn_id='spark_client_ssh',
        command="""
        cd /dataops/scripts
        python3 clean_transactions.py
        """,
    )

    # Task 5: Verify data in PostgreSQL
    def verify_data():
        """Verify that data was loaded successfully"""
        pg_hook = PostgresHook(postgres_conn_id='traindb_conn')
        
        # Check if table exists and has data
        result = pg_hook.get_first("SELECT COUNT(*) FROM public.clean_store_transactions")
        
        if result and result[0] > 0:
            print(f"✓ Data verification successful: {result[0]} rows in clean_store_transactions")
            return True
        else:
            raise Exception("Data verification failed: No data found in table")
    
    verify_output = PythonOperator(
        task_id='verify_postgres_data',
        python_callable=verify_data,
    )

    # Task dependencies
    check_source >> copy_script >> install_deps >> run_cleaning >> verify_output
