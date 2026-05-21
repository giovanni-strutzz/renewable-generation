import pytest
import os
import requests
from pymongo import MongoClient
from src.jobs.spark_etl_forecast import main  # Importamos o ponto de entrada real


def test_full_pipeline_integration():
    """
    Valida o fluxo fim-a-fim: S3 -> Spark -> MongoDB.
    """
    mongo_uri = os.getenv("MONGO_URI", "mongodb://admin:admin@mongodb:27017")
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    collection_name = "forecasts"

    client = MongoClient(mongo_uri)
    db = client[db_name]
    db[collection_name].delete_many({})

    try:
        main()

        count = db[collection_name].count_documents({})
        print(f"\nIntegration: Found {count} documents in MongoDB.")

        assert count > 0

        doc = db[collection_name].find_one()
        assert "estimated_generation_mwh" in doc
        assert doc["source"] == "wind"

    except Exception as e:
        pytest.fail(f"Pipeline integration failed: {e}")
    finally:
        client.close()