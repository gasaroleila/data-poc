# Sovereign Data Clean Room & Lakehouse Pipeline: 14-Day Implementation & Learning Plan

> **Project Name:** Sovereign Data Intelligence Platform (Local Proof-of-Concept)
> **Target Goal:** Build a working, lightweight, open-source prototype of a Sovereign Data Clean Room & Lakehouse Pipeline in 14 days using PySpark, Delta Lake, Delta Sharing, Unity Catalog principles, and MLflow.
> **Environment:** Dockerized from Day 1 (docker-compose), deployable to Strettch Cloud `SC Compute` after partnership finalization.

---

## 1. Project Architecture & Technical Objectives

This prototype simulates an end-to-end sovereign data platform designed for regional data privacy compliance (e.g., Rwanda's *Law No. 058/2021*). It proves that an institution can ingest raw operational records, scrub personally identifiable information (PII) at the edge, store data in reliable transactional tables, and share anonymized features securely with third-party AI developers via open protocols.

```
+-----------------------------------------------------------------------------------+
|                                 DATA PROVIDER NODE                                |
|                                                                                   |
|  [ Mock Raw Logs ]                                                                |
|  (JSON/CSV w/ PII)                                                                |
|          |                                                                        |
|          v                                                                        |
|  [ Ingestion & Scrubbing ] --->  [ Bronze Layer ] (Raw Ingestion / Append-Only)   |
|  (Go / PySpark UDFs)                   |                                          |
|                                        v                                          |
|                                  [ Silver Layer ] (Anonymized & Cleaned Delta)   |
|                                        |                                          |
|                                        v                                          |
|                                  [ Gold Layer ]   (AI Feature Store / Delta)      |
|                                        |                                          |
|                                        v                                          |
|                            [ Delta Sharing Server ]                               |
|                            (Custom Compliance Logic / REST API)                   |
+----------------------------------------|------------------------------------------+
                                         | Secure REST Protocol
                                         | (Zero-Copy / Pre-Signed URLs)
                                         v
+-----------------------------------------------------------------------------------+
|                                 DATA CONSUMER NODE                                |
|                                                                                   |
|  [ External AI Developer ] ---> Connects via `delta-sharing` Python SDK           |
|                                Reads Gold Features (No PII visible)               |
|                                Trains Scikit-Learn / XGBoost Model                |
|                                Logs Model Weights & Loss to [ MLflow Server ]     |
+-----------------------------------------------------------------------------------+
```

---

## 2. Directory Structure of the Prototype Repo

```
sovereign-data-poc/
├── docker-compose.yml              # Grows per phase; always a valid deployable unit
├── dockerfiles/
│   ├── spark-jupyter.Dockerfile    # PySpark + JupyterLab + Delta Lake
│   ├── sharing-server.Dockerfile   # Delta Sharing server (JVM)
│   ├── compliance-gateway.Dockerfile  # Multi-stage Go build
│   └── mlflow.Dockerfile           # MLflow tracking server
├── config/
│   ├── delta-sharing-server.yaml
│   └── recipients.json
├── notebooks/                      # Interactive learning (Phases 1-2)
│   ├── 01_pyspark_basics.ipynb
│   ├── 02_delta_lake_intro.ipynb
│   └── 03_medallion_pipeline.ipynb
├── data/
│   ├── raw/                        # Raw simulated inputs
│   └── delta/
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── src/
│   ├── generator/
│   │   └── mock_data.py
│   ├── pipeline/
│   │   ├── 01_ingest_bronze.py
│   │   ├── 02_scrub_silver.py
│   │   └── 03_aggregate_gold.py
│   └── sharing/
│       └── server_config.py
├── gateway/                        # Go compliance gateway
│   ├── main.go                     # HTTP reverse proxy + entry point
│   ├── rules.go                    # Column masking, row filtering, differential privacy
│   ├── audit.go                    # Append-only JSONL audit logger
│   ├── rules_test.go
│   ├── go.mod
│   └── go.sum
├── consumer/
│   ├── client_train.py
│   └── profile.share
├── benchmarks/
│   └── benchmark_test.py
├── logs/                           # Audit trail output
│   └── .gitkeep
└── tests/
    ├── test_generator.py
    └── test_pipeline.py
```

---

## 3. 14-Day Implementation Schedule

### Phase 1 -- Days 1-3: Docker + PySpark Foundations

* **Focus:** Docker-first environment, PySpark fundamentals, Delta Lake introduction.
* **Key Tasks:**
  1. Build `dockerfiles/spark-jupyter.Dockerfile` (PySpark 3.5.x + Delta Spark 3.2.x + JupyterLab).
  2. Create `docker-compose.yml` with single `spark-jupyter` service on port 8888, volume-mounting `./data`, `./notebooks`, `./src`.
  3. Write `src/generator/mock_data.py` to produce 5000 synthetic records with PII fields.
  4. Build `notebooks/01_pyspark_basics.ipynb`: DataFrame ops (select, filter, groupBy, UDFs, joins, explain plans).
  5. Build `notebooks/02_delta_lake_intro.ipynb`: Write/read Delta format, schema enforcement, time travel (`versionAsOf`), ACID updates/deletes, inspect `_delta_log/`.

* **Day-by-day:**
  - **Day 1:** Dockerfile + docker-compose + mock data generator + read JSON with `spark.read.json()` + basic DataFrame operations.
  - **Day 2:** PySpark fundamentals: joins, UDFs (SHA-256 UDF), window functions, explain plans.
  - **Day 3:** Delta Lake -- write as Delta, time travel, schema enforcement errors, ACID operations, understand `_delta_log/` directory.

* **Checkpoint -- do not proceed until you can answer:**
  1. What is the difference between a transformation and an action in Spark? Give three examples of each.
  2. What happens when you `spark.read.format("delta").load(path)` -- what files does Spark actually read?
  3. If you write data, delete a row, then query `versionAsOf(0)`, what do you get and why?
  4. What is inside `_delta_log/` and what does each JSON file represent?

* **Docker state:** 1 service (spark-jupyter).

### Phase 2 -- Days 4-6: Medallion Pipeline

* **Focus:** Build Bronze/Silver/Gold pipeline, understanding each layer's purpose deeply.
* **Key Tasks:**
  1. Build `notebooks/03_medallion_pipeline.ipynb` for interactive development before extracting to scripts.
  2. Build `src/pipeline/01_ingest_bronze.py`: Raw JSON -> Bronze Delta (append mode, `ingested_at` metadata).
  3. Build `src/pipeline/02_scrub_silver.py`: Bronze -> Silver (SHA-256 hash national_id, drop PII columns, compliance flag).
  4. Build `src/pipeline/03_aggregate_gold.py`: Silver -> Gold (per-district aggregations, district integer encoding for ML).

* **Day-by-day:**
  - **Day 4:** Bronze ingestion -- append vs overwrite, partitioning strategy, ingestion metadata.
  - **Day 5:** Silver scrubbing -- SHA-256 hashing, PII removal, verification query asserting zero PII in output.
  - **Day 6:** Gold aggregation + full end-to-end pipeline run through all three layers.

* **Checkpoint:**
  1. Why three layers instead of two? What problem does each layer solve?
  2. If a regulator asks "prove no PII leaks to Gold," how do you demonstrate this programmatically?
  3. What happens if the raw data schema changes (new column)? Walk through each layer.
  4. Why `sha2` instead of just deleting national_id? When would you need the hash?

* **Docker state:** Still 1 service.

### Phase 3 -- Days 7-9: Delta Sharing + Consumer Client

* **Focus:** Deploy Delta Sharing server, build consumer client, verify zero-copy access.
* **Key Tasks:**
  1. Build `dockerfiles/sharing-server.Dockerfile` (Delta Sharing server, JVM-based from `delta-io/delta-sharing`).
  2. Add `sharing-server` service to docker-compose on port 8080, mount `data/delta/` read-only.
  3. Configure `config/delta-sharing-server.yaml` to expose Gold tables.
  4. Generate `consumer/profile.share` with bearer token + endpoint.
  5. Build `consumer/client_train.py`: connect via delta-sharing SDK, load as Pandas, train RandomForest, print R2 score (no MLflow yet).

* **Day-by-day:**
  - **Day 7:** Sharing server in docker-compose, verify with `curl localhost:8080/delta-sharing/shares`.
  - **Day 8:** Profile generation, consumer client loading data as Pandas, verify consumer has no `data/` mount (zero-copy).
  - **Day 9:** Complete ML training in consumer script, prove end-to-end data sharing protocol.

* **Checkpoint:**
  1. What HTTP requests does `delta_sharing.load_as_pandas()` actually make? (Check server logs)
  2. Why is this called "zero-copy"? What would "copy" look like in a traditional setup?
  3. If you revoke the bearer token, what happens to the consumer? Try it.
  4. Draw the network flow: consumer container -> sharing server -> filesystem -> response.

* **Docker state:** 2 services (spark-jupyter, sharing-server).

### Phase 4 -- Days 10-12: Go Compliance Gateway

* **Focus:** Build a Go HTTP gateway in front of the sharing server for compliance enforcement and audit logging.
* **Key Tasks:**
  1. Build `gateway/main.go`: Go HTTP reverse proxy forwarding to the sharing server.
  2. Build `gateway/audit.go`: Append-only JSONL audit logger to `logs/audit.jsonl`.
  3. Build `gateway/rules.go`: Column masking (GPS -> 2km grid), row filtering (min group size 10), differential privacy (Laplace noise on numeric aggregates).
  4. Build `gateway/rules_test.go`.
  5. Build `dockerfiles/compliance-gateway.Dockerfile` (multi-stage Go build).
  6. Add gateway to docker-compose on port 9090, reroute consumer through it.

* **Day-by-day:**
  - **Day 10:** Go module setup, simple HTTP reverse proxy + JSONL audit logging. Learn `net/http`, `httputil.ReverseProxy`, JSON encoding.
  - **Day 11:** Column masking and row filtering rules. Learn Go structs, interfaces, JSON marshaling through the compliance logic.
  - **Day 12:** Laplace noise for differential privacy, `rules_test.go`, reroute consumer through gateway and verify end-to-end.

* **Checkpoint:**
  1. Why put the gateway in front of the sharing server instead of building rules into the pipeline?
  2. What is the Laplace mechanism and what parameter (epsilon) controls the privacy/accuracy tradeoff?
  3. If you set minimum group size to 10, what attack does this prevent?
  4. Show the audit log and explain each field. Why is append-only important?
  5. What happens if the gateway goes down? Does the consumer bypass it or fail?

* **Docker state:** 3 services (spark-jupyter, sharing-server, compliance-gateway).

### Phase 5 -- Days 13-14: MLflow + Benchmarks + Deploy Readiness

* **Focus:** MLflow tracking, performance benchmarks, deployment readiness for Strettch Cloud.
* **Key Tasks:**
  1. Build `dockerfiles/mlflow.Dockerfile`, add MLflow service to docker-compose on port 5000.
  2. Update `consumer/client_train.py` to log params, metrics, and model artifacts to MLflow.
  3. Build `benchmarks/benchmark_test.py`: ingestion throughput, compression ratio, query latency at 10k/100k/1M rows.
  4. Deployment polish: all paths relative, config via env vars, no hardcoded localhost in production paths.

* **Day-by-day:**
  - **Day 13:** MLflow service + updated consumer with experiment tracking + begin benchmarking.
  - **Day 14:** Complete benchmarks, end-to-end validation (`docker-compose up` from clean state), Strettch Cloud deployment readiness.

* **Docker state:** 4 services (spark-jupyter, sharing-server, compliance-gateway, mlflow).

### Fallback: What to Cut if Behind

* **Non-negotiable:** Phases 1-3 (Docker + PySpark + pipeline + sharing).
* **Simplifiable:** Phase 4 -- drop differential privacy, keep column masking + audit logging only.
* **Deferrable:** Phase 5 -- MLflow and benchmarks can be added post-14-days.

---

## 4. Hands-On Code Specifications

### A. Mock Data Generator (`src/generator/mock_data.py`)

Generates realistic telemetry records with embedded PII to test scrubbing logic.

```python
import json
import random
import uuid
from datetime import datetime

def generate_mock_records(count=1000):
    records = []
    districts = ["Gasabo", "Kicukiro", "Nyarugenge", "Musanze", "Huye"]
    for i in range(count):
        record = {
            "record_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "national_id": f"11995800{random.randint(100000, 999999)}",
            "full_name": f"Citizen_{i}",
            "phone_number": f"+25078{random.randint(1000000, 9999999)}",
            "district": random.choice(districts),
            "metric_type": "soil_ph",
            "metric_value": round(random.uniform(4.5, 8.5), 2),
            "yield_estimate_kg": random.randint(100, 5000)
        }
        records.append(record)
    return records

if __name__ == "__main__":
    data = generate_mock_records(5000)
    with open("data/raw/telemetry_batch_1.json", "w") as f:
        json.dump(data, f, indent=2)
    print("Generated 5,000 raw records in data/raw/telemetry_batch_1.json")
```

### B. Bronze to Silver PII Scrubbing (`src/pipeline/02_scrub_silver.py`)

Reads Bronze Delta table, hashes National IDs using SHA-256, redacts personal identity attributes, and writes to Silver Delta storage.

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, lit, current_timestamp
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("SovereignDataEngine-SilverScrub") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read Bronze Table
bronze_df = spark.read.format("delta").load("data/delta/bronze")

# Anonymization Pipeline (Silver)
silver_df = bronze_df \
    .withColumn("hashed_national_id", sha2(col("national_id"), 256)) \
    .drop("national_id", "full_name", "phone_number") \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("compliance_flag", lit("RW_LAW_058_2021_COMPLIANT"))

# Write to Silver Delta Table
silver_df.write.format("delta").mode("overwrite").save("data/delta/silver")
print("Successfully processed Bronze data into Silver Delta Table (PII Redacted).")
```

### C. Delta Sharing Configuration (`config/delta-sharing-server.yaml`)

Configures the open-source Delta Sharing server to expose only Silver and Gold datasets.

```yaml
version: 1
host: "0.0.0.0"
port: 8080
endpoint: "/delta-sharing"
authorization:
  bearerTokens:
    - "rwanda-sovereign-token-2026-secret"
shares:
  - name: "sovereign_data_share"
    schemas:
      - name: "agri_analytics"
        tables:
          - name: "gold_features"
            location: "data/delta/gold"
```

### D. Consumer AI Training Script (`consumer/client_train.py`)

Simulates a third-party developer bringing an ML training script directly to the shared data clean room node.

```python
import delta_sharing
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import mlflow

# Point to profile
profile_file = "consumer/profile.share"
client = delta_sharing.SharingClient(profile_file)

# Fetch shared table via Delta Sharing REST protocol
table_url = f"{profile_file}#sovereign_data_share.agri_analytics.gold_features"
data = delta_sharing.load_as_pandas(table_url)

print(f"Loaded {len(data)} rows from Sovereign Data Clean Room via Delta Sharing.")

# Train ML Model
X = data[["soil_ph_avg", "district_encoded"]]
y = data["yield_estimate_kg"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("Sovereign_Agri_Yield_Prediction")

with mlflow.start_run():
    model = RandomForestRegressor(n_estimators=100)
    model.fit(X_train, y_train)
    score = model.score(X_test, y_test)
  
    mlflow.log_param("n_estimators", 100)
    mlflow.log_metric("r2_score", score)
    mlflow.sklearn.log_model(model, "model")
    print(f"Model trained successfully. R2 Score: {score:.4f}. Logged to MLflow.")
```

---

## 5. Verification & Benchmark Checklist

To support resume claims and YC batch application metrics, record the following measurements during Days 12–14:

* [ ] **Ingestion Throughput:** Measure records per second processed through Go/PySpark PII scrubbing pipeline.
* [ ] **Storage Optimization:** Measure storage savings comparing raw JSON files vs. compressed Silver Delta Parquet files.
* [ ] **Zero-Knowledge Verification:** Inspect Silver and Gold tables to verify 0% leak of raw National IDs, phone numbers, or unhashed identity attributes.
* [ ] **Cross-Node Latency:** Measure round-trip query time using `delta-sharing` Python client over HTTP REST endpoints.
* [ ] **Audit Trail Completeness:** Verify that every query executed by the client generates a log entry recorded in local server logs.
