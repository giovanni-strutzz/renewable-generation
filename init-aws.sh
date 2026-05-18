#!/bin/bash
echo "=========== Inicializando Recursos AWS no Localstack ==========="

# 1. Criar o bucket S3 que funcionará como nossa Raw Zone (Data Lake)
aws s3 mb s3://raw-zone --profile localstack

# 2. Criar uma fila SQS (caso queira usar para o acionamento do Airflow depois)
aws sqs create-queue --queue-name energy-forecast-queue --profile localstack

echo "=========== Recursos criados com sucesso! ==========="