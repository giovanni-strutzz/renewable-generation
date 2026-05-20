import pytest
import os
import requests
from pymongo import MongoClient
from src.jobs.spark_etl_forecast import main  # Importamos o ponto de entrada real


def test_full_pipeline_integration(spark):
    """
    Valida o fluxo fim-a-fim: S3 -> Spark -> MongoDB.
    """
    # 1. Configurações de Conexão (Lendo do .env via ambiente do container)
    mongo_uri = os.getenv("MONGO_URI", "mongodb://admin:admin@mongodb:27017")
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    collection_name = "forecasts"

    # Limpar a coleção de teste antes de começar
    client = MongoClient(mongo_uri)
    db = client[db_name]
    db[collection_name].delete_many({})

    # 2. Verificar se o LocalStack está acessível e tem o bucket
    # (Opcional: Você pode injetar um arquivo aqui via código se quiser um teste 100% isolado)

    try:
        # 3. Executar o Job Principal
        # Como o main() já chama a SparkSession e a fecha,
        # ele vai usar as configs de prod (S3a e Mongo)
        main()

        # 4. Validar se os dados chegaram no MongoDB
        count = db[collection_name].count_documents({})
        print(f"\nIntegration: Found {count} documents in MongoDB.")

        # Se o seu arquivo de teste no S3 tinha dados, count deve ser > 0
        assert count > 0

        # Validar um documento para ver se a coluna calculada existe
        doc = db[collection_name].find_one()
        assert "estimated_generation_mwh" in doc
        assert doc["source"] == "wind"

    except Exception as e:
        pytest.fail(f"Pipeline integration failed: {e}")
    finally:
        client.close()