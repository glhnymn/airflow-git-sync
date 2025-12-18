#!/usr/bin/env python3
"""
Data Cleaning Application
Reads dirty data from MinIO, cleans it, and writes to PostgreSQL
"""
import pandas as pd
import s3fs
from sqlalchemy import create_engine
import logging

# Logging yapılandırması
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MinIO Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "dataopsadmin"
MINIO_SECRET_KEY = "dataopsadmin"
MINIO_BUCKET = "dataops-bronze"
MINIO_FILE_PATH = "raw/dirty_store_transactions.csv"

# PostgreSQL Configuration
POSTGRES_HOST = "traindb"
POSTGRES_PORT = "5432"
POSTGRES_DB = "traindb"
POSTGRES_USER = "train"
POSTGRES_PASSWORD = "Ankara06"
POSTGRES_TABLE = "clean_data_transactions"

def read_from_minio():
    """MinIO'dan veri okur"""
    logger.info("Reading data from MinIO...")
    
    s3 = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={
            'endpoint_url': f'http://{MINIO_ENDPOINT}'
        }
    )
    
    file_path = f"{MINIO_BUCKET}/{MINIO_FILE_PATH}"
    
    with s3.open(file_path, 'rb') as f:
        df = pd.read_csv(f)
    
    logger.info(f"Data read successfully.  Shape: {df.shape}")
    return df

def clean_data(df):
    """Veriyi temizler"""
    logger.info("Starting data cleaning...")
    
    initial_rows = len(df)
    logger.info(f"Initial rows: {initial_rows}")
    
    # Missing values'ları kontrol et
    logger.info(f"Missing values:\n{df.isnull().sum()}")
    
    # Duplicate'leri kaldır
    df_cleaned = df.drop_duplicates()
    logger.info(f"Duplicates removed: {initial_rows - len(df_cleaned)}")
    
    # Missing values'ları handle et
    # Numeric columns için median ile doldur
    numeric_columns = df_cleaned.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        if df_cleaned[col].isnull().any():
            median_value = df_cleaned[col].median()
            df_cleaned[col]. fillna(median_value, inplace=True)
            logger.info(f"Filled missing values in {col} with median: {median_value}")
    
    # Categorical columns için mode ile doldur
    categorical_columns = df_cleaned.select_dtypes(include=['object']).columns
    for col in categorical_columns:
        if df_cleaned[col].isnull().any():
            mode_value = df_cleaned[col].mode()[0]
            df_cleaned[col].fillna(mode_value, inplace=True)
            logger.info(f"Filled missing values in {col} with mode: {mode_value}")
    
    # Strip whitespace from string columns
    for col in categorical_columns:
        df_cleaned[col] = df_cleaned[col].str.strip()
    
    logger.info(f"Data cleaning completed. Final rows: {len(df_cleaned)}")
    return df_cleaned

def write_to_postgres(df):
    """Temiz veriyi PostgreSQL'e yazar (Full Load)"""
    logger.info("Writing data to PostgreSQL...")
    
    # Connection string
    connection_string = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    # Engine oluştur
    engine = create_engine(connection_string)
    
    # Full load:  önce tabloyu drop et, sonra yeniden oluştur
    df. to_sql(
        POSTGRES_TABLE,
        engine,
        if_exists='replace',  # Tablo varsa önce sil, sonra oluştur
        index=False,
        method='multi',
        chunksize=1000
    )
    
    logger.info(f"Data written successfully to {POSTGRES_TABLE}.  Total rows: {len(df)}")

def main():
    """Ana fonksiyon"""
    try:
        # 1. MinIO'dan veriyi oku
        df = read_from_minio()
        
        # 2. Veriyi temizle
        df_cleaned = clean_data(df)
        
        # 3. PostgreSQL'e yaz
        write_to_postgres(df_cleaned)
        
        logger.info("Data pipeline completed successfully!")
        
    except Exception as e:
        logger. error(f"Error in data pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()
