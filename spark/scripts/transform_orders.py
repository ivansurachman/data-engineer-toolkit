import os
import shutil
import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round

# ============================================================
# 0. KONFIGURASI LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("TransformOrders")


def log_step_start(step_name):
    """Mencatat awal proses dan mengembalikan waktu mulai."""
    logger.info("START | %s", step_name)
    return time.perf_counter()


def log_step_end(step_name, start_time, detail=""):
    """Mencatat proses selesai dan durasinya."""
    duration = time.perf_counter() - start_time
    logger.info(
        "SUCCESS | %s | duration=%.2f seconds | %s",
        step_name,
        duration,
        detail
    )


# ============================================================
# 1. MEMBUAT SPARK SESSION
# ============================================================

step = log_step_start("01 - Create Spark Session")

spark = (
    SparkSession.builder
    .appName("TransformOrders")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

logger.info("Spark application ID: %s", spark.sparkContext.applicationId)

log_step_end("01 - Create Spark Session", step)


orders_path = "/opt/airflow/data/orders.csv"
items_path = "/opt/airflow/data/order_items.csv"
output_path = "/opt/airflow/parquet_output/orders_joined_parquet"
destination_path = "/opt/airflow/data/output/orders_joined_parquet"


try:
    # ========================================================
    # 2. MEMBACA DATA CSV
    # ========================================================

    step = log_step_start("02 - Read orders.csv")

    orders_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(orders_path)
    )

    logger.info("orders.csv columns: %s", orders_df.columns)
    log_step_end("02 - Read orders.csv", step)

    step = log_step_start("03 - Read order_items.csv")

    items_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(items_path)
    )

    logger.info("order_items.csv columns: %s", items_df.columns)
    log_step_end("03 - Read order_items.csv", step)


    # ========================================================
    # 3. MEMBERSIHKAN DATA
    # ========================================================

    step = log_step_start("04 - Clean orders data")

    orders_df = orders_df.drop("internal_notes")

    orders_df = orders_df.fillna({
        "payment_method": "UNKNOWN",
        "order_status": "UNKNOWN"
    })

    log_step_end(
        "04 - Clean orders data",
        step,
        "Dropped internal_notes and filled missing values"
    )

    step = log_step_start("05 - Clean order_items data")

    items_df = items_df.fillna({
        "product_category": "UNKNOWN",
        "quantity": "0",
        "price": "0",
        "discount_rate": "0"
    })

    log_step_end(
        "05 - Clean order_items data",
        step,
        "Filled missing values"
    )


    # ========================================================
    # 4. STANDARDISASI TIPE DATA
    # ========================================================

    step = log_step_start("06 - Standardize data types")

    orders_df = orders_df.withColumn(
        "order_date",
        to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")
    )

    items_df = (
        items_df
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("discount_rate", col("discount_rate").cast("double"))
    )

    log_step_end("06 - Standardize data types", step)


    # ========================================================
    # 5. VALIDASI DAN FILTER DATA
    # ========================================================

    step = log_step_start("07 - Validate orders data")

    orders_before = orders_df.count()

    orders_df = orders_df.filter(
        col("order_id").isNotNull() &
        col("order_date").isNotNull()
    )

    orders_after = orders_df.count()

    log_step_end(
        "07 - Validate orders data",
        step,
        f"Rows before={orders_before}, after={orders_after}, "
        f"removed={orders_before - orders_after}"
    )

    step = log_step_start("08 - Validate order_items data")

    items_before = items_df.count()

    items_df = items_df.filter(
        col("order_id").isNotNull() &
        col("quantity").isNotNull() &
        (col("quantity") > 0) &
        col("price").isNotNull() &
        (col("price") >= 0) &
        col("discount_rate").between(0, 1)
    )

    items_after = items_df.count()

    log_step_end(
        "08 - Validate order_items data",
        step,
        f"Rows before={items_before}, after={items_after}, "
        f"removed={items_before - items_after}"
    )


    # ========================================================
    # 6. JOIN DATA
    # ========================================================

    step = log_step_start("09 - Join orders and order_items")

    joined_df = orders_df.join(
        items_df,
        on="order_id",
        how="inner"
    )

    log_step_end(
        "09 - Join orders and order_items",
        step,
        "Join type=inner, key=order_id"
    )


    # ========================================================
    # 7. MENGHITUNG GMV
    # ========================================================

    step = log_step_start("10 - Calculate GMV")

    transformed_df = joined_df.withColumn(
        "gmv",
        round(
            col("quantity") *
            col("price") *
            (1 - col("discount_rate")),
            2
        )
    )

    log_step_end(
        "10 - Calculate GMV",
        step,
        "GMV = quantity * price * (1 - discount_rate)"
    )


    # ========================================================
    # 8. MENYIMPAN HASIL KE PARQUET
    # ========================================================

    step = log_step_start("11 - Write Parquet")

    (
        transformed_df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    log_step_end(
        "11 - Write Parquet",
        step,
        f"Output path={output_path}"
    )


    # ========================================================
    # 9. MEMBACA KEMBALI DAN VALIDASI OUTPUT
    # ========================================================

    step = log_step_start("12 - Validate Parquet output")

    result_df = spark.read.parquet(output_path)
    result_count = result_df.count()

    logger.info("Output row count: %s", result_count)
    logger.info("Output schema:")
    result_df.printSchema()

    log_step_end(
        "12 - Validate Parquet output",
        step,
        f"Rows={result_count}"
    )

    result_df.show(10, truncate=False)


    # ========================================================
    # 10. MENYALIN OUTPUT KE FOLDER AIRFLOW
    # ========================================================

    step = log_step_start("13 - Copy Parquet to Airflow output")

    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    if os.path.exists(destination_path):
        shutil.rmtree(destination_path)

    os.makedirs(destination_path, exist_ok=True)

    copied_files = 0

    for filename in os.listdir(source_path := output_path):
        source_file = os.path.join(source_path, filename)
        destination_file = os.path.join(destination_path, filename)

        if os.path.isfile(source_file):
            shutil.copyfile(source_file, destination_file)
            copied_files += 1

    log_step_end(
        "13 - Copy Parquet to Airflow output",
        step,
        f"Files copied={copied_files}, destination={destination_path}"
    )

    logger.info("PIPELINE SUCCESS | TransformOrders completed")


except Exception:
    logger.exception("PIPELINE FAILED | TransformOrders encountered an error")
    raise

finally:
    logger.info("Stopping Spark session")
    spark.stop()
    logger.info("Spark session stopped")
