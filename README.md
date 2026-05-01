# CryptoStream 

Real-time crypto data pipeline: **CoinGecko → Kafka → dbt → Neon PostgreSQL**, monitored by Prometheus & Grafana, orchestrated by Airflow.

---

## Architecture

```
CoinGecko API
     │
     ▼
 [ingestor]  ──Avro──►  [kafka]  ──────────────────────────────────┐
     │                     │                                        │
     │ /metrics           [schema-registry]                         │
     ▼                                                              │
[prometheus] ──────► [grafana]                                      │
                                                                    ▼
                                                             [airflow DAGs]
                                                                    │
                                                                    ▼
                                                              [dbt run]
                                                                    │
                                                                    ▼
                                                          Neon PostgreSQL (remote)
```

## Services & Ports

| Service           | Container Name        | Host Port | Purpose                          |
|-------------------|-----------------------|-----------|----------------------------------|
| Kafka             | `cs-kafka`            | `29092`   | Message broker (external access) |
| ZooKeeper         | `cs-zookeeper`        | —         | Kafka coordination               |
| Schema Registry   | `cs-schema-registry`  | `8081`    | Avro schema management           |
| Kafka UI          | `cs-kafka-ui`         | `8082`    | Browse topics & messages         |
| Ingestor          | `cs-ingestor`         | `9464`    | CoinGecko → Kafka + /health      |
| PostgreSQL        | `cs-postgres`         | `5432`    | Airflow metadata DB              |
| Airflow Webserver | `cs-airflow-webserver`| `8080`    | DAG management UI                |
| Airflow Scheduler | `cs-airflow-scheduler`| —         | Runs scheduled DAGs              |
| dbt               | `cs-dbt`              | —         | SQL transformations (one-shot)   |
| Prometheus        | `cs-prometheus`       | `9090`    | Metrics store                    |
| Grafana           | `cs-grafana`          | `3000`    | Dashboards                       |

---

## Quick Start

### 1. Set up environment variables

```bash
cp .env.example .env
# Edit .env — fill in COINGECKO_API_KEY, NEON_DATABASE_URL, passwords, etc.
```

### 2. Generate the Airflow Fernet key (required once)

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output into AIRFLOW_FERNET_KEY in your .env
```

### 3. Start all services

```bash
docker compose up -d
```

> **Note:** First run downloads ~3 GB of images. Subsequent starts are instant.

### 4. Verify everything is running

```bash
docker compose ps
```

All services should show **healthy** or **running**.

---

## Useful Commands

```bash
# ── Logs ─────────────────────────────────────────────────────
docker compose logs -f ingestor          # live ingestor output
docker compose logs -f airflow-scheduler # DAG execution logs
docker compose logs -f kafka             # broker logs

# ── Shell access ──────────────────────────────────────────────
docker compose exec ingestor sh          # into ingestor container
docker compose exec postgres psql -U airflow airflow  # Airflow DB

# ── Restart a single service ──────────────────────────────────
docker compose restart ingestor

# ── Run dbt manually ─────────────────────────────────────────
docker compose run --rm dbt dbt run --profiles-dir /usr/app

# ── Stop everything ───────────────────────────────────────────
docker compose down

# ── Stop + wipe all volumes (⚠ destructive) ──────────────────
docker compose down -v
```

---

## Service UIs

| UI               | URL                         | Default credentials |
|------------------|-----------------------------|---------------------|
| Airflow          | http://localhost:8080        | admin / admin       |
| Kafka UI         | http://localhost:8082        | —                   |
| Grafana          | http://localhost:3000        | admin / admin       |
| Prometheus       | http://localhost:9090        | —                   |
| Schema Registry  | http://localhost:8081/subjects | —                 |
| Ingestor health  | http://localhost:9464/health | —                   |
| Ingestor metrics | http://localhost:9464/metrics| —                   |

> Change default passwords in `.env` before exposing any port externally.

---

## Developing Locally (without Docker)

Run the ingestor directly against the Dockerised Kafka:

```bash
cd ingestor
npm install
# .env must have KAFKA_BOOTSTRAP_SERVERS=localhost:29092
node src/index.js
```

---

## Directory Structure

```
cryptostream/
├── docker-compose.yml          ← all services wired together
├── .env / .env.example         ← environment configuration
├── .dockerignore
│
├── ingestor/                   ← Node.js CoinGecko → Kafka producer
│   ├── Dockerfile
│   ├── .dockerignore
│   ├── package.json
│   └── src/
│       ├── index.js            ← entry point
│       ├── metrics.js          ← Prometheus metrics
│       ├── logger.js           ← Winston logger
│       └── crypto-price.avsc   ← Avro schema
│
├── airflow/
│   ├── dags/                   ← Airflow DAG definitions
│   └── plugins/                ← custom Airflow plugins
│
├── dbt_transform/
│   ├── profiles.yml            ← dbt Neon connection (env-driven)
│   ├── models/                 ← SQL transformation models
│   └── tests/                  ← dbt data quality tests
│
├── prometheus/
│   └── prometheus.yml          ← scrape config
│
└── grafana/
    ├── provisioning/
    │   ├── datasources/        ← auto-connects Prometheus
    │   └── dashboards/         ← auto-loads dashboard JSONs
    └── dashboards/
        └── ingestor-overview.json
```

---

## CI/CD

The project includes a GitHub Actions workflow (`.github/workflows/ci-cd.yml`) that runs on `push` and `pull_request` to the `main` branch.

The workflow performs the following:
1. **Configuration Validation:** Validates the `docker-compose.yml` file.
2. **Docker Build:** Builds the Docker image for the `ingestor` service.
3. **Docker Push:** Automatically pushes the built image to the GitHub Container Registry (GHCR) upon merges or pushes to the `main` branch.
