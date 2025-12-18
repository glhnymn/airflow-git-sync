#!/usr/bin/env python3
"""
Data cleaning script for dirty_store_transactions.csv
Reads from MinIO, cleans data, writes to PostgreSQL
"""

import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
import io
import sys

def clean_store_transactions():
    """
    Clean store transactions data:
    - Read from MinIO bronze bucket
    - Clean missing values and duplicates
    - Write to PostgreSQL traindb
    """
    
    # MinIO configuration
    minio_client = Minio(
        "minio:9000",
        access_key="dataopsadmin",
        secret_key="dataopsadmin",
        secure=False
    )
    
    print("Reading data from MinIO...")
    try:
        # Read CSV from MinIO
        response = minio_client.get_object("dataops-bronze", "raw/dirty_store_transactions.csv")
        df = pd.read_csv(io.BytesIO(response.read()))
        print(f"Successfully read {len(df)} rows from MinIO")
    except Exception as e:
        print(f"Error reading from MinIO: {e}")
        sys.exit(1)
    
    print(f"Original data shape: {df.shape}")
    print(f"Missing values:\n{df.isnull().sum()}")
    print(f"Duplicates: {df.duplicated().sum()}")
    
    # Data cleaning
    print("\nCleaning data...")
    
    # Remove duplicates
    df_clean = df.drop_duplicates()
    print(f"After removing duplicates: {len(df_clean)} rows")
    
    # Handle missing values
    # Drop rows with missing critical values
    df_clean = df_clean.dropna(subset=['STORE_ID', 'STORE_LOCATION'])
    
    # Fill numeric missing values with median
    numeric_columns = df_clean.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        if df_clean[col].isnull().sum() > 0:
            df_clean[col].fillna(df_clean[col].median(), inplace=True)
    
    # Fill categorical missing values with 'Unknown'
    categorical_columns = df_clean.select_dtypes(include=['object']).columns
    for col in categorical_columns:
        if df_clean[col].isnull().sum() > 0:
            df_clean[col].fillna('Unknown', inplace=True)
    
    print(f"After cleaning: {df_clean.shape}")
    print(f"Remaining missing values: {df_clean.isnull().sum().sum()}")
    
    # Write to PostgreSQL
    print("\nWriting cleaned data to PostgreSQL...")
    try:
        engine = create_engine('postgresql://train:train123@traindb:5432/traindb')
        
        # Full load - replace existing data
        df_clean.to_sql(
            'clean_store_transactions',
            engine,
            schema='public',
            if_exists='replace',
            index=False
        )
        
        print(f"Successfully wrote {len(df_clean)} rows to traindb.public.clean_store_transactions")
        
        # Verify
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM public.clean_store_transactions")
            count = result.scalar()
            print(f"Verification: Table has {count} rows")
            
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
        sys.exit(1)
    
    print("\nData cleaning pipeline completed successfully!")
    return True

if __name__ == "__main__":
    clean_store_transactions()
