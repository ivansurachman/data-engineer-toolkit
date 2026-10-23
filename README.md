# Day 46 — Batch Processing with PySpark

## Overview

This project implements a batch data processing pipeline using **Apache Spark (PySpark)** and **Apache Airflow**. The pipeline reads order and order item data from CSV files, performs data cleaning and validation, joins the datasets, calculates Gross Merchandise Value (GMV), and stores the transformed data in Parquet format.

The project also includes process-level logging to improve traceability, making it easier to monitor execution progress, measure processing duration, identify failures, and validate output data.

## Objectives

- Build a batch ETL pipeline using PySpark.
- Read and process multiple CSV datasets.
- Clean and validate data before transformation.
- Join order data with order item data.
- Calculate GMV after discounts.
- Store transformed data in Parquet format.
- Integrate data processing with Apache Airflow.
- Add logging to monitor each processing stage.
- Validate the generated output.

## Tech Stack

| Technology | Purpose |
|---|---|
| Python | ETL implementation and file operations |
| Apache Spark / PySpark | Distributed data processing |
| Apache Airflow | Workflow orchestration and task monitoring |
| CSV | Input data format |
| Apache Parquet | Output columnar storage format |
| Podman Compose | Container and service orchestration |

## Project Workflow

```text
orders.csv ─────────┐
                    │
                    ├──> Read CSV Files
                    │
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
                 Join Using order_id
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

The pipeline processes two CSV datasets.

### 1. `orders.csv`

Contains order-level information, including the order identifier, order date, payment method, and order status.

The `internal_notes` column is removed because it is not required for the transformation.

### 2. `order_items.csv`

Contains order item information, including product category, quantity, price, and discount rate.

The datasets are joined using the `order_id` column.

> The exact input schema depends on the source CSV files.

## Data Processing

### 1. Read CSV Files

The pipeline reads both CSV files using PySpark with headers enabled and automatic schema inference disabled.

### 2. Data Cleaning

Missing values are handled using default values.

| Dataset | Column | Default Value |
|---|---|---|
| Orders | `payment_method` | `UNKNOWN` |
| Orders | `order_status` | `UNKNOWN` |
| Order Items | `product_category` | `UNKNOWN` |
| Order Items | `quantity` | `0` |
| Order Items | `price` | `0` |
| Order Items | `discount_rate` | `0` |

The `internal_notes` column is removed from the orders dataset.

### 3. Data Type Standardization

The pipeline converts columns to appropriate data types:

- `order_date` to timestamp.
- `quantity` to integer.
- `price` to double.
- `discount_rate` to double.

### 4. Data Validation

The pipeline filters invalid records before joining the datasets.

**Orders validation:**
- `order_id` must not be null.
- `order_date` must be a valid timestamp and must not be null.

**Order items validation:**
- `order_id` must not be null.
- `quantity` must not be null and must be greater than zero.
- `price` must not be null and must be greater than or equal to zero.
- `discount_rate` must be between zero and one, inclusive.

### 5. Data Joining

The two datasets are joined using an **inner join** on `order_id`. Only matching records from both datasets are retained.

### 6. GMV Calculation

The pipeline creates a new column named `gmv` to calculate the item value after discount.

Formula:

\[
\text{GMV} = \text{quantity} \times \text{price} \times (1 - \text{discount\_rate})
\]

The result is rounded to two decimal places.

## Output

The transformed dataset is written in Apache Parquet format.

**Primary output path:**

```text
/opt/airflow/parquet_output/orders_joined_parquet
```

**Secondary output path:**

```text
/opt/airflow/data/output/orders_joined_parquet
```

The primary output is written by Spark. The pipeline then reads the Parquet output again to validate the resulting row count and schema before copying the generated files to the secondary directory.

Typical Parquet output files include:

```text
orders_joined_parquet/
├── part-....snappy.parquet
└── _SUCCESS
```

The number and names of part files depend on Spark execution and partitioning.

## Logging and Traceability

The pipeline uses Python's `logging` module to record execution progress.

The logs include:

- Start and successful completion of processing stages.
- Execution duration for each stage.
- Input column names.
- Row counts before and after data validation.
- Spark application ID.
- Output location and number of copied files.
- Error messages and stack traces when an exception occurs.

Example log format:

```text
2026-10-23 08:30:01 | INFO | START | 02 - Read orders.csv
2026-10-23 08:30:03 | INFO | SUCCESS | 02 - Read orders.csv | duration=2.15 seconds
2026-10-23 08:30:05 | INFO | START | 07 - Validate orders data
2026-10-23 08:30:08 | INFO | SUCCESS | 07 - Validate orders data | duration=3.20 seconds
2026-10-23 08:30:14 | INFO | PIPELINE SUCCESS | TransformOrders completed
```

The timestamps and durations above are illustrative examples, not actual benchmark results.

When the script is executed through an Airflow task, its logs can be inspected through the Airflow UI, provided that the task execution setup captures the Spark process output.

## Apache Airflow Integration

The pipeline is orchestrated through an Airflow DAG named:

```text
orders_etl_pipeline
```

The transformation task is named:

```text
transform_orders
```

Airflow is used to trigger the processing task and inspect execution logs and task status.

The pipeline raises exceptions when processing fails, allowing Airflow to identify failed executions rather than reporting a false success.

## Containerization

The project uses Podman Compose to manage the containerized environment.

A shared named volume, `parquet_output`, is used to make the primary Parquet output available to the relevant Airflow and Spark containers.

Because the containers run under different user IDs, volume ownership and group permissions must be configured so Spark workers can write output and Airflow can access the results.

## How to Run

1. Start the required services using Podman Compose.
2. Ensure the input CSV files are available at the configured paths.
3. Verify that the Spark containers can access the shared input and output directories.
4. Open the Airflow UI.
5. Locate the `orders_etl_pipeline` DAG.
6. Trigger the DAG or rerun the `transform_orders` task.
7. Monitor the task logs for processing progress and errors.
8. Verify that the Parquet output has been generated successfully.

The exact startup commands and Airflow UI address depend on the local Compose configuration.

## Key Learning Outcomes

- Implementing batch ETL with PySpark.
- Cleaning and validating structured data.
- Joining multiple datasets using a common key.
- Applying business calculations during data transformation.
- Writing and validating Parquet datasets.
- Orchestrating data processing with Airflow.
- Implementing logging and execution-time monitoring.
- Managing shared storage permissions in a containerized environment.

## Future Improvements

- Add automated data quality checks and assertions.
- Record input and output row counts in a dedicated audit table.
- Add configurable input and output paths.
- Introduce retry handling and alert notifications in Airflow.
- Optimize Spark execution by reducing repeated data scans.
- Add unit tests for transformation and validation logic.
- Add data partitioning where appropriate for larger datasets.

## Conclusion

This project demonstrates a containerized batch ETL workflow that combines PySpark for data transformation and Apache Airflow for orchestration. It processes order and order item data, validates records, calculates GMV after discounts, and produces Parquet output.

The addition of stage-level logging improves pipeline observability and helps identify processing failures, execution bottlenecks, and data quality issues.


## Getting Started

### Prerequisites

Make sure the following tools are installed:

- [Podman](https://podman.io/) or Docker
- Docker Compose or a compatible Compose provider
- Git
- Access to the project repository

> This project uses Compose profiles to start individual services. The commands below use `docker compose`, as configured in the project instructions. If you use Podman, you can generally use `podman compose` with a compatible Compose provider.

### 1. Clone the Repository

```bash
git clone https://github.com/ivansurachman/data-engineer-toolkit.git
cd data-engineer-toolkit
```

### 2. Configure the Environment

Review the project files before starting the services:

- `docker-compose.yaml` or `compose.yaml`
- `.env`, if required
- Input data files and mounted directories
- Service-specific configuration files

Make sure the required environment variables, volume mounts, and network settings are configured.

### 3. Start the Required Services

Start the services needed for your workflow using the available Compose profiles.

## Available Services

### Airflow

Start Apache Airflow for workflow orchestration and task monitoring.

```bash
docker compose --profile airflow up -d
```

### Grafana

Start Grafana for monitoring and dashboard visualization.

```bash
docker compose --profile grafana up -d
```

### PostgreSQL

Start PostgreSQL.

```bash
docker compose --profile postgres up -d
```

Alternatively, start both MySQL and PostgreSQL using the `db` profile:

```bash
docker compose --profile db up -d
```

### MySQL

Start MySQL.

```bash
docker compose --profile mysql up -d
```

Alternatively, start both MySQL and PostgreSQL using the `db` profile:

```bash
docker compose --profile db up -d
```

### Hive

Start the Hive services.

```bash
docker compose --profile hive up -d
```

### Kafka

Start Kafka for event streaming and message processing.

```bash
docker compose --profile kafka up -d
```

### Spark

Start the Spark services for batch data processing.

```bash
docker compose --profile spark up -d
```

> **Note:** The commands above start the services associated with each profile. Dependencies, service health, and access ports depend on the Compose configuration.

## Running the PySpark ETL Pipeline

This workflow processes order data using PySpark and orchestrates the execution through Airflow.

### 1. Start the Required Services

Start Spark:

```bash
docker compose --profile spark up -d
```

Start Airflow:

```bash
docker compose --profile airflow up -d
```

If the Compose configuration requires a database or other supporting services, start those profiles as well.

### 2. Verify the Containers

```bash
docker compose ps
```

Check that the required Airflow and Spark containers are running.

### 3. Verify the Input Data

Make sure these files are available inside the relevant containers:

```text
/opt/airflow/data/orders.csv
/opt/airflow/data/order_items.csv
```

### 4. Open Airflow

Open the Airflow web interface using the port configured in the Compose file.

Locate the DAG:

```text
orders_etl_pipeline
```

Trigger the DAG from the Airflow UI.

### 5. Monitor the Transformation Task

Open the `transform_orders` task and inspect its logs.

The logs provide information about:

- Reading input CSV files
- Cleaning and validating records
- Standardizing data types
- Joining the datasets
- Calculating GMV
- Writing Parquet output
- Validating the output
- Copying output files
- Errors and execution duration

### 6. Verify the Output

The primary output directory is:

```text
/opt/airflow/parquet_output/orders_joined_parquet
```

The secondary output directory is:

```text
/opt/airflow/data/output/orders_joined_parquet
```

The output is stored in Parquet format and typically includes part files and a `_SUCCESS` marker.

## Managing Services

### View Running Containers

```bash
docker compose ps
```

### View Service Logs

```bash
docker compose logs -f
```

To inspect a specific service, use its service name from the Compose configuration:

```bash
docker compose logs -f <service_name>
```

### Stop Services

Stop and remove containers and networks while preserving named volumes:

```bash
docker compose down
```

> Avoid `docker compose down -v` unless you intentionally want to remove the associated named volumes. Volumes may contain generated Parquet data or other persistent project data.

## Troubleshooting

- **Service fails to start:** inspect its logs with `docker compose logs <service_name>`.
- **Input file not found:** verify the file paths and volume mounts.
- **Spark cannot write Parquet:** verify that the Spark worker has write permission on the output volume.
- **Airflow task fails:** inspect the task logs in the Airflow UI and identify the failing ETL stage.
- **Profile does not start the expected service:** check the `profiles` and `depends_on` settings in the Compose file.

