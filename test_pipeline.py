import asyncio
import json
import boto3
from src.infrastructure.integration.open_meteo_client import OpenMeteoClient
from src.application.services.extract_weather_data import ExtractWeatherDataService

# Configuração do cliente S3 apontando para o Localstack
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)


async def rodar_teste_de_mesa():
    print("🚀 Passo 1: Instanciando o serviço de extração...")
    client_api = OpenMeteoClient()
    service = ExtractWeatherDataService(weather_provider=client_api)

    # Coordenadas públicas aproximadas do Complexo Ventos do Piauí (Auren)
    PARQUE_ID = "Ventos-do-Piaui-01"
    LATITUDE = -8.45
    LONGITUDE = -41.50

    print(f"📡 Passo 2: Buscando dados na API Open-Meteo para o parque {PARQUE_ID}...")
    dados_enriquecidos = await service.execute(PARQUE_ID, LATITUDE, LONGITUDE)

    print("📦 Passo 3: Salvando o JSON bruto no S3 do Localstack (Raw Zone)...")
    nome_arquivo = f"weather-data/{PARQUE_ID}.json"

    s3_client.put_object(
        Bucket="raw-zone",
        Key=nome_arquivo,
        Body=json.dumps(dados_enriquecidos, indent=4)
    )
    print(f"✅ Arquivo 's3://raw-zone/{nome_arquivo}' criado com sucesso no Localstack!")


if __name__ == "__main__":
    asyncio.run(rodar_teste_de_mesa())