# ⚡ Renewable Generation Intelligence — ETL Platform

[![CI/CD Pipeline](https://github.com/seu-usuario/renewable-generation/actions/workflows/ci.yml/badge.svg)](https://github.com/seu-usuario/renewable-generation/actions)
![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-3.x-017CEE?logo=apacheairflow&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

> A production-grade, cloud-native ETL platform for ingesting and processing atmospheric forecast data applied to renewable energy generation (Wind & Solar). Built with **Hexagonal Architecture**, **Data Contracts**, and **real-time observability** as first-class concerns.

---

## Table of Contents

1. [Project Motivation](#-project-motivation)
2. [Architecture & Tech Stack](#-architecture--tech-stack)
3. [Senior Engineering Highlights](#-senior-engineering-highlights)
4. [Domain Physics & Data Model](#-domain-physics--data-model)
5. [Project Structure](#-project-structure)
6. [Getting Started](#-getting-started)
7. [CI/CD Strategy](#-cicd-strategy)
8. [Running Tests](#-running-tests)

---

## 🎯 Project Motivation

Renewable energy sources — solar and wind — are inherently **stochastic**: their output is governed by atmospheric conditions that are hard to predict and harder to model at scale. A single percentage point of forecast error in wind speed can cascade into massive discrepancies in estimated grid output, given the cubic relationship between wind speed and power ($P \propto v^3$).

This platform was built to address that challenge end-to-end:

- **Automate** the collection of physically meaningful atmospheric variables (Temperature, Cloud Cover, Wind Speed, Surface Pressure, Relative Humidity).
- **Process at scale** using distributed computing (Apache Spark), ensuring the pipeline remains performant across years of historical data.
- **Guarantee data integrity** through formal Data Contracts (Great Expectations), preventing corrupt data from ever reaching downstream ML models.
- **Persist with low latency** in a NoSQL layer (MongoDB) optimized for fast retrieval by forecasting services.
- **Observe in real-time** with a fully integrated ELK Stack, enabling operational dashboards without raw log access.

---

## 🏗️ Architecture & Tech Stack

The solution adopts a **Lakehouse + Hexagonal Architecture** paradigm, simulating a high-availability cloud environment locally via Docker. The Medallion layers (Bronze → Silver → Gold) are enforced by design.

| Layer | Technology | Strategic Purpose |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 3.x | Async task execution via the latest Task SDK; resilient DAG orchestration. |
| **Processing** | Apache Spark 3.5 + Java 17 | Distributed, scalable transformation of massive weather datasets. |
| **Storage (Silver)** | Delta Lake on LocalStack (S3) | ACID transactions, Time Travel, and `partitionOverwriteMode=dynamic` for idempotent reprocessing. |
| **Object Storage** | LocalStack | AWS S3-compatible mock for fully local, Cloud-Native development. |
| **Data Quality** | Great Expectations | Data Contract enforcement as a pipeline *gatekeeper* — Fail-Fast on schema or physics violations. |
| **Observability** | ELK Stack (Elastic + Logstash + Kibana) | Centralized structured logging via TCP; real-time dashboards for error rates and volumetry. |
| **NoSQL / API** | MongoDB + FastAPI | Low-latency persistence layer for ML model consumption via REST. |
| **Relational** | PostgreSQL | Airflow metadata backend. |
| **Infrastructure** | Docker & Docker Compose | Hermetic, reproducible environments for development and integration testing. |

---

## 🌟 Senior Engineering Highlights

### 1. Data Contracts as a Quality Gatekeeper

Unlike traditional pipelines that silently propagate bad data, this platform uses **Great Expectations** as a hard gate inside the Spark job. If extracted data fails physical domain rules — e.g., `wind_speed < 0`, null `cloud_cover`, or out-of-bounds `temperature` — the pipeline raises a **Fail-Fast** exception, ships a structured alert payload to the ELK Stack via Logstash, and halts before any write reaches the Silver layer. Data corruption is stopped at the source.

### 2. Idempotent Reprocessing with Delta Lake

The Silver layer is backed by **Delta Lake** configured with `partitionOverwriteMode=dynamic`. This means re-running any specific date partition is safe by design: no duplicates are generated, no adjacent partitions are touched, and full *Time Travel* is available for auditing or rollback. The Lakehouse state is always consistent, regardless of how many times a DAG run is retried.

### 3. Real-Time Observability via ELK

The Spark job ships **structured JSON log payloads** to a Logstash TCP input on every execution. This enables Kibana dashboards tracking transformation throughput, error rates, data volumetry, and pipeline SLA adherence — without requiring direct server access or parsing raw log files. Observability is a first-class citizen, not an afterthought.

### 4. Hexagonal Architecture (Ports & Adapters)

The `src/` layer enforces strict boundary separation: domain entities and business rules have **zero external dependencies**. Infrastructure adapters (MongoDB, Kafka, Open-Meteo API) implement abstract ports defined in the domain. This makes every adapter independently swappable and testable in isolation — a property validated by the unit test suite via mocked I/O.

---

## 📐 Domain Physics & Data Model

The ETL transformation layer is grounded in renewable energy physics. Each variable ingested carries a precise engineering rationale:

| Metric | Technical Importance | Impact on Generation |
| :--- | :--- | :--- |
| **Temperature (@2m)** | Affects PV cell efficiency and air density. | High temperatures reduce Solar output; density affects Wind kinetic energy. |
| **Wind Speed (@100m)** | Primary input for Wind Turbine Power Curves ($P \propto v^3$). | Cubic relationship: small forecast errors produce massive power discrepancies. |
| **Cloud Cover** | Main driver of Global Horizontal Irradiance (GHI). | Direct correlation with Solar intermittency and ramp events. |
| **Surface Pressure** | Used to calculate Air Density ($\rho$). | Essential for refining the Wind Power curve accuracy at altitude. |
| **Relative Humidity** | Indicates potential soiling or condensation. | Affects aerodynamic drag on turbine blades and solar panel cleanliness. |

---

## 📂 Project Structure

```text
.
├── dags/
│   └── weather_etl_dag.py              # Airflow DAG — orchestration entry point
├── src/
│   ├── domain/                         # Pure business logic — zero external dependencies
│   │   ├── entities/
│   │   │   └── forecast.py             # Forecast domain entity
│   │   ├── enums/
│   │   │   └── sources.py              # Energy source types (Wind, Solar)
│   │   └── ports/
│   │       ├── external_integration.py # Port: weather data provider contract
│   │       ├── messaging.py            # Port: event publishing contract
│   │       └── repositories.py        # Port: persistence contract
│   ├── application/                    # Use cases — orchestrates domain logic
│   │   ├── services/
│   │   │   └── extract_weather_data.py
│   │   └── use_cases/
│   │       └── request_forecast.py
│   ├── infrastructure/                 # Adapters — concrete implementations of ports
│   │   ├── integration/
│   │   │   └── open_meteo_client.py    # Adapter: Open-Meteo API
│   │   ├── messaging/
│   │   │   └── kafka_publisher.py      # Adapter: Kafka event publisher
│   │   ├── repository/
│   │   │   └── mongo_repository.py    # Adapter: MongoDB persistence
│   │   └── dependencies.py            # Dependency injection wiring
│   ├── interfaces/                     # Delivery layer — exposes the application
│   │   └── api/v1/
│   │       ├── endpoints.py            # FastAPI route handlers
│   │       └── schemas/
│   │           └── forecast.py         # Pydantic request/response schemas
│   ├── jobs/
│   │   └── spark_etl_forecast.py       # PySpark ETL job (S3 → Delta Lake → MongoDB)
│   └── main.py                         # FastAPI application entry point
├── logstash/                           # ELK pipeline configurations
├── localstack/                         # LocalStack S3 bucket init scripts
├── tests/                              # Unit & Integration (Pytest + mocked infra)
├── .github/workflows/                  # CI/CD automation
├── Dockerfile                          # Custom Airflow image (Java 17 + Spark)
├── docker-compose.yaml                 # Full infrastructure definition
├── Makefile                            # Project automation
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- [Docker & Docker Compose V2](https://docs.docker.com/get-docker/)
- Python 3.11+
- `make` utility (standard on Linux/macOS)

### Quick Start

```bash
# 1. Configure environment variables
cp .env.example .env

# 2. Build the custom Airflow + Java 17 + Spark image
make build

# 3. Provision all services (Airflow, Spark, Postgres, MongoDB, LocalStack, ELK)
make up

# 4. Fix volume permissions (Linux only — prevents Airflow log write errors)
make fix-perm
```

### Access Points

| Service | URL | Credentials |
| :--- | :--- | :--- |
| Airflow UI | http://localhost:8081 | `admin` / `admin` |
| FastAPI Docs | http://localhost:8000/docs | — |
| Kibana (Logs) | http://localhost:5601 | — |
| LocalStack S3 | http://localhost:4566 | — |

Once services are up, unpause the `dag_weather_spark` DAG in the Airflow UI and click **Trigger** to execute the full pipeline.

### Monitoring & Operations

```bash
# Stream real-time execution logs
make logs

# Check the status of all running containers
make ps

# Stop all services
make down

# Full reset — stops services and purges all volumes and data
make clean
```

---

## 🔄 CI/CD Strategy

Every `git push` triggers a GitHub Actions workflow that enforces the full development lifecycle automatically:

| Stage | Tool | What it validates |
| :--- | :--- | :--- |
| **Linter** | `flake8` / `ruff` | PEP8 compliance and code style. |
| **Unit Tests** | `pytest` + mocks | Domain logic and Power Curve calculations — no I/O required. |
| **Integration Tests** | `pytest` + ephemeral containers | Full Spark flow end-to-end against real MongoDB and LocalStack instances. |
| **Security** | `pip-audit` / `safety` | Dependency vulnerability scanning. |

---

## 🧪 Running Tests

```bash
# Unit tests — fast, fully mocked I/O
make test-unit

# Integration tests — requires active containers (make up first)
make test-integration
```

---

> Developed with a focus on **Scalability**, **Reliability**, **Data Governance**, and **Operational Excellence**.