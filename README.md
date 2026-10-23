# Day 46 — Batch Processing with PySpark

## Overview

This project implements a batch ETL pipeline using **Apache Spark (PySpark)** and **Apache Airflow**. It reads order and order-item data from CSV files, cleans and validates the data, joins both datasets, calculates Gross Merchandise Value (GMV) after discounts, and writes the transformed result in Parquet format.

The pipeline also includes stage-level logging to improve traceability. Logs help track processing progress, measure execution duration, inspect data quality, and diagnose failures through Airflow.

## Objectives

- Build a batch ETL pipeline using PySpark.
- Read and process multiple CSV datasets.
- Clean and validate records before transformation.
- Join order and order-item datasets using `order_id`.
- Calculate GMV after discounts.
- Store transformed data in Parquet format.
- Orchestrate the pipeline with Apache Airflow.
- Log processing stages, durations, row counts, and errors.
- Validate the generated output.

## Tech Stack

| Technology | Purpose |
|---|---|
| Python | ETL logic and file operations |
| Apache Spark / PySpark | Batch data processing |
| Apache Airflow | Workflow orchestration and task monitoring |
| CSV | Input data format |
| Apache Parquet | Output columnar storage format |
| Docker Compose / Podman Compose | Container and service orchestration |

## Project Workflow

```text
orders.csv ─────────┐
                    ├──> Read CSV Files
order_items.csv ────┘
                           |
                           v
                     Data Cleaning
                           |
                           v
                Data Type Standardization
                           |
                           v
                    Data Validation
                           |
                           v
                Join using order_id
                           |
                           v
                    Calculate GMV
                           |
                           v
                  Write Parquet Output
                           |
                           v
                   Validate Output
                           |
                           v
             Copy Output to Airflow Data
```

## Input Data

The pipeline processes two CSV files.

### 1. `orders.csv`

Contains order-level information, including the order identifier, order date, payment method, and order status. The `internal_notes` column is removed because it is not required for this transformation.

### 2. `order_items.csv`

Contains order-item information, including product category, quantity, price, and discount rate.

The datasets are joined using `order_id`. The exact input schema depends on the supplied CSV files.

## Data Processing

### 1. Read CSV Files

PySpark reads both CSV files with headers enabled and automatic schema inference disabled.

### 2. Data Cleaning

Missing values are filled with default values:

| Dataset | Column | Default Value |
|---|---|---|
| Orders | `payment_method` | `UNKNOWN` |
| Orders | `order_status` | `UNKNOWN` |
| Order items | `product_category` | `UNKNOWN` |
| Order items | `quantity` | `0` |
| Order items | `price` | `0` |
| Order items | `discount_rate` | `0` |

The `internal_notes` column is removed from the orders dataset.

### 3. Data Type Standardization

- `order_date` is converted to a timestamp.
- `quantity` is converted to an integer.
- `price` is converted to a double.
- `discount_rate` is converted to a double.

### 4. Data Validation

Invalid records are filtered out before the join.

**Orders validation**
- `order_id` must not be null.
- `order_date` must parse successfully and must not be null.

**Order-items validation**
- `order_id` must not be null.
- `quantity` must not be null and must be greater than zero.
- `price` must not be null and must be greater than or equal to zero.
- `discount_rate` must be between zero and one, inclusive.

### 5. Data Joining

The datasets are joined with an **inner join** on `order_id`. Only records with matching order identifiers are retained.

### 6. GMV Calculation

The pipeline creates a `gmv` column to calculate the item value after discount:

\[
\text{GMV} = \text{quantity} \times \text{price} \times (1 - \text{discount\_rate})
\]

The result is rounded to two decimal places.

## Output

The transformed dataset is written in Parquet format.

**Primary output path inside the containers:**

```text
/opt/airflow/parquet_output/orders_joined_parquet
```

**Output path inside the Airflow container:**

```text
/opt/airflow/data/output/orders_joined_parquet
```

The secondary output corresponds to the project’s `airflow/data/output` directory when that directory is mounted as configured in Compose.

A typical output directory contains Parquet part files and a `_SUCCESS` marker:

```text
orders_joined_parquet/
├── part-....snappy.parquet
└── _SUCCESS
```

The number and names of part files depend on Spark execution and partitioning.

## Logging and Traceability

The pipeline uses Python's `logging` module to record execution progress. Logs can include:

- Start and successful completion of each processing stage.
- Execution duration for each stage.
- Input column names.
- Row counts before and after validation.
- Spark application ID.
- Output path and number of copied files.
- Error messages and stack traces when a failure occurs.

Example log format (illustrative only):

```text
2026-10-23 08:30:01 | INFO | START | Read orders.csv
2026-10-23 08:30:03 | INFO | SUCCESS | Read orders.csv | duration=2.15 seconds
2026-10-23 08:30:05 | INFO | START | Validate orders data
2026-10-23 08:30:08 | INFO | SUCCESS | Validate orders data | duration=3.20 seconds
2026-10-23 08:30:14 | INFO | PIPELINE SUCCESS | TransformOrders completed
```

The timestamps and durations above are examples, not measured benchmark results. When the script is run through Airflow, inspect the task logs in the Airflow UI.

## Run the Project

### Prerequisites

- Docker with Docker Compose, or Podman with a compatible Compose provider.
- Git.
- The project repository and required input CSV files.

### 1. Open the Project Directory

Run the commands from the project root:

```bash
cd day-46
```

If you have already opened a terminal in the `day-46` directory, you can skip this step.

### 2. Build and Start Airflow and Spark

Use **one** of the following commands.

**Docker Compose:**

```bash
docker compose --profile airflow --profile spark up -d --build
```

**Podman Compose:**

```bash
podman compose --profile airflow --profile spark up -d --build
```

These commands build the configured images when needed and start the services enabled by the `airflow` and `spark` profiles. If the Compose configuration requires additional supporting services, ensure they are started as well.

### 3. Check the Services

For Docker:

```bash
docker compose ps
```

For Podman:

```bash
podman compose ps
```

Wait until the required Airflow and Spark services are running before triggering the pipeline.

### 4. Monitor Spark Master and Workers

After the Spark services are running, open these URLs in your browser to monitor the Spark cluster:

| Component | Monitoring URL |
|---|---|
| Spark Master | [http://localhost:10010](http://localhost:10010) |
| Spark Worker 1 | [http://localhost:10011](http://localhost:10011) |
| Spark Worker 2 | [http://localhost:10012](http://localhost:10012) |

The Spark Master UI shows cluster-level information, including registered workers and running or completed applications. Each Worker UI shows information about that worker and the tasks or executors running on it.

These addresses assume the corresponding container ports are published to the host using the listed ports in the Compose configuration. If a page does not open, verify the service status and port mappings with `docker compose ps` or `podman compose ps`, and inspect the relevant service logs.

### 5. Open Airflow

Open the Airflow UI in your browser:

**URL:** [http://localhost:8080](http://localhost:8080)

Use the configured local credentials:

| Field | Value |
|---|---|
| Username | `airflow` |
| Password | `airflow` |

> These are local development credentials. Do not use default credentials for a publicly exposed or production deployment.

### 5. Unpause and Trigger the DAG

1. In the Airflow UI, find the DAG named `orders_etl_pipeline`.
2. If the DAG is paused, unpause it.
3. Trigger the DAG.
4. Open the `transform_orders` task to monitor its status and logs.
5. If the task fails, inspect the logs to identify the stage and error.

The pipeline reads the CSV files, cleans and validates the data, joins the datasets, calculates GMV, writes Parquet output, validates the result, and copies the output to the configured Airflow data directory.

### 6. Check the Parquet Output

After the DAG completes successfully, check the project output directory:

```text
airflow/data/output/orders_joined_parquet/
```

The equivalent path inside the Airflow container is:

```text
/opt/airflow/data/output/orders_joined_parquet/
```

The directory should contain the generated Parquet files. The exact location on the host depends on the volume or bind-mount configuration in the Compose file.

## Available Compose Profiles

The repository also provides commands for starting other services independently. Run these from the project root.

| Service | Command |
|---|---|
| Airflow | `docker compose --profile airflow up -d` |
| Grafana | `docker compose --profile grafana up -d` |
| PostgreSQL | `docker compose --profile postgres up -d` |
| MySQL | `docker compose --profile mysql up -d` |
| MySQL and PostgreSQL | `docker compose --profile db up -d` |
| Hive | `docker compose --profile hive up -d` |
| Kafka | `docker compose --profile kafka up -d` |
| Spark | `docker compose --profile spark up -d` |

For Podman, replace `docker compose` with `podman compose`, provided your Compose provider supports the project configuration.

## Managing Services

### View Running Services

```bash
docker compose ps
```

Or with Podman:

```bash
podman compose ps
```

### View Logs

```bash
docker compose logs -f
```

To inspect a specific service:

```bash
docker compose logs -f <service_name>
```

For Podman, use `podman compose logs -f` with the same arguments.

### Stop Services

```bash
docker compose --profile {profile_name} down -v
```

Or:

```bash
podman compose --profile {profile_name} down -v
```

These commands stop and remove the Compose-managed containers and networks while normally preserving named volumes.

> **Important:** Avoid `docker compose down -v` or `podman compose down -v` unless you intentionally want to remove the associated named volumes. Volumes may contain generated Parquet output or other persistent project data.

## Troubleshooting

- **A service fails to start:** inspect its logs with `docker compose logs <service_name>` or `podman compose logs <service_name>`.
- **Input file not found:** verify the CSV paths and the volume mounts.
- **Spark cannot write Parquet:** check ownership and write permissions on the shared output volume for the Spark workers.
- **Airflow task fails:** open the task logs in the Airflow UI and locate the failing ETL stage.
- **A profile does not start the expected service:** check the `profiles` and `depends_on` settings in the Compose file.
- **Output does not appear in the host folder:** verify that `airflow/data/output` is mounted to `/opt/airflow/data/output` in the Compose configuration.

## Key Learning Outcomes

- Implement batch ETL with PySpark.
- Clean and validate structured data.
- Join multiple datasets using a common key.
- Apply business calculations during data transformation.
- Write and validate Parquet datasets.
- Orchestrate data processing with Airflow.
- Implement logging and execution-time monitoring.
- Manage shared storage and permissions in containers.

## Future Improvements

- Add automated data quality checks and assertions.
- Record input and output row counts in a dedicated audit table.
- Make input and output paths configurable.
- Add retry handling and alert notifications in Airflow.
- Optimize Spark execution by reducing repeated data scans.
- Add unit tests for transformation and validation logic.
- Add data partitioning where appropriate for larger datasets.

## Conclusion

This project demonstrates a containerized batch ETL workflow that combines PySpark for data transformation and Apache Airflow for orchestration. It processes order and order-item data, validates records, calculates GMV after discounts, and produces Parquet output. Stage-level logging improves observability and helps identify processing failures, execution bottlenecks, and data quality issues.
