# Renewable Generation & Weather Forecast ETL Pipeline

This project implements a robust ETL (Extract, Transform, Load) pipeline designed to ingest weather forecast data, which is critical for predicting renewable energy generation (Wind and Solar). The architecture leverages a modern data stack to simulate a production-grade cloud environment locally.

## Table of Contents

1. [Project Motivation](#project-motivation)
2. [Architecture & Tech Stack](#architecture--tech-stack)
3. [Decision Records](#decision-records)
4. [Technical Data Explanation](#technical-data-explanation)
5. [Project Structure](#project-structure)
6. [Getting Started](#getting-started)
7. [Running Tests](#running-tests)

---

## Project Motivation

Renewable energy sources like solar and wind are highly dependent on atmospheric conditions. Accurate weather forecasting is the cornerstone of energy grid stability. This project was built to:

- Automate the collection of atmospheric variables (Temperature, Cloud Cover, Wind Speed).
- Provide a scalable processing layer using Apache Spark.
- Store processed data in a NoSQL format for fast retrieval by forecasting models.

---

## Architecture & Tech Stack

The project follows a **Medallion-like Architecture** (simulated locally):

| Layer | Technology |
| :--- | :--- |
| **Orchestration** | Apache Airflow 3.x (Latest features and Task SDK) |
| **Processing** | Apache Spark (PySpark) with Java 17 |
| **Object Storage** | LocalStack (Simulating AWS S3) |
| **NoSQL Storage** | MongoDB (Metadata and processed forecasts) |
| **Relational Storage** | PostgreSQL (Airflow Metadata) |
| **Infrastructure** | Docker & Docker Compose |

---

## Decision Records

### Why Apache Airflow 3.x?

Chosen for its industry-standard status in DAG orchestration. Using version 3.x ensures the pipeline is compatible with the latest asynchronous task executions and provider separations.

### Why Apache Spark?

Spark was chosen over standard Python/Pandas because weather data can scale rapidly. Distributed processing ensures that the pipeline remains performant even when handling years of historical data.

---

## Technical Data Explanation

In the context of Renewable Generation, the weather data collected serves as **physical predictors**:

| Metric | Technical Importance | Impact on Generation |
| :--- | :--- | :--- |
| **Temperature (@2m)** | Affects PV cell efficiency and air density. | High temperatures reduce Solar output; density affects Wind kinetic energy. |
| **Wind Speed (@100m)** | Primary input for Wind Turbines ($P \propto v^3$). | Cubic relationship: small forecast errors lead to massive power discrepancies. |
| **Cloud Cover** | Main driver for Global Horizontal Irradiance (GHI). | Direct correlation with Solar intermittency and ramp events. |
| **Surface Pressure** | Used to calculate Air Density ($\rho$). | Essential for refining the Wind Power curve accuracy. |
| **Relative Humidity** | Indicates potential soiling or condensation. | Affects aerodynamic drag on blades and solar panel cleanliness. |

---

## Project Structure

The project follows a **Hexagonal Architecture (Ports & Adapters)** in the `src/` layer, with a clear separation between domain logic, application use cases, and infrastructure concerns.

```text
.
├── dags/
│   └── weather_etl_dag.py          # Airflow DAG definition (orchestration entry point)
├── src/
│   ├── domain/                     # Core business rules — no external dependencies
│   │   ├── entities/
│   │   │   └── forecast.py         # Forecast domain entity
│   │   ├── enums/
│   │   │   └── sources.py          # Energy source types (Wind, Solar)
│   │   └── ports/
│   │       ├── external_integration.py  # Port: weather data provider contract
│   │       ├── messaging.py             # Port: event publishing contract
│   │       └── repositories.py          # Port: persistence contract
│   ├── application/                # Use cases — orchestrates domain logic
│   │   ├── services/
│   │   │   └── extract_weather_data.py  # Weather extraction service
│   │   └── use_cases/
│   │       └── request_forecast.py      # Forecast request use case
│   ├── infrastructure/             # Adapters — concrete implementations of ports
│   │   ├── integration/
│   │   │   └── open_meteo_client.py     # Adapter: Open-Meteo API client
│   │   ├── messaging/
│   │   │   └── kafka_publisher.py       # Adapter: Kafka event publisher
│   │   ├── repository/
│   │   │   └── mongo_repository.py      # Adapter: MongoDB persistence
│   │   └── dependencies.py              # Dependency injection wiring
│   ├── interfaces/                 # Delivery layer — exposes the application
│   │   └── api/
│   │       └── v1/
│   │           ├── endpoints.py         # FastAPI route handlers
│   │           └── schemas/
│   │               └── forecast.py      # Request/response Pydantic schemas
│   ├── jobs/
│   │   └── spark_etl_forecast.py   # PySpark ETL job (S3 → transform → MongoDB)
│   └── main.py                     # FastAPI application entry point
├── localstack/                     # LocalStack initialization scripts (S3 buckets)
├── scripts/                        # Utility scripts
├── test_pipeline.py                # End-to-end pipeline test
├── Dockerfile                      # Custom Airflow image (Java 17 + Spark)
├── docker-compose.yaml             # Full infrastructure definition
├── Makefile                        # Project automation commands
└── README.md
```

---

## Getting Started

Follow these steps to set up the environment and run the pipeline using the provided `Makefile` commands.

### Prerequisites

- [Docker & Docker Compose](https://docs.docker.com/get-docker/) installed.
- `make` utility installed (standard on Linux/macOS).

### 1. Build the Environment

Build the custom Airflow image that includes Java 17 and Apache Spark:

```bash
make build
```

### 2. Spin Up Services

Start all containers (Airflow, Spark, Postgres, MongoDB, LocalStack) in the background:

```bash
make up
```

### 3. Fix Permissions *(Linux only)*

To avoid `Permission Denied` errors when Airflow tries to write logs or access DAGs:

```bash
make fix-perm
```

### 4. Access and Run

Once all services are up, open the Airflow Web UI:

- **URL:** [http://localhost:8081](http://localhost:8081)
- **Username:** `admin`
- **Password:** `admin`

Unpause the `dag_weather_spark` DAG and click the **Trigger** button to run the pipeline.

### 5. Monitoring & Debugging

Follow execution logs in real-time:

```bash
make logs
```

Check the status of all running containers:

```bash
make ps
```

### 6. Stopping & Cleanup

Stop all services:

```bash
make down
```

Stop all services and delete all data and volumes (full reset):

```bash
make clean
```

### 7. Running Tests
To ensure the transformation logic and integration are working correctly:
```bash
# Run unit tests (Mocked I/O)
make test-unit

# Run integration tests (Requires active containers)
make test-integration