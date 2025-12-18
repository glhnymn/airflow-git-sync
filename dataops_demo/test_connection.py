#!/usr/bin/env python3
"""Test MinIO and PostgreSQL connections"""
import s3fs
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_minio():
    """Test MinIO connection"""
    try: 
        s3 = s3fs.S3FileSystem(
            key="dataopsadmin",
            secret="dataopsadmin",
            client_kwargs={'endpoint_url': 'http://minio:9000'}
        )
        buckets = s3.ls('')
        logger.info(f"MinIO connected.  Buckets: {buckets}")
        return True
    except Exception as e: 
        logger.error(f"MinIO connection failed: {e}")
        return False

def test_postgres():
    """Test PostgreSQL connection"""
    try: 
        engine = create_engine("postgresql://train:Ankara06@traindb:5432/traindb")
        with engine.connect() as conn:
            result = conn.execute("SELECT version();")
            logger.info(f"PostgreSQL connected:  {result.fetchone()[0]}")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        return False

if __name__ == "__main__": 
    test_minio()
    test_postgres()
