import os

from pyspark.sql import SparkSession


def init_spark() -> SparkSession:
    warehouse = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "warehouse"))

    spark = (
        SparkSession.builder
        .appName("PySpark Iceberg Local Example")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", f"file:{warehouse}")
        .config("spark.sql.warehouse.dir", f"file:{warehouse}/_spark_sql_warehouse")
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0")
        .getOrCreate()
    )

    print("✅ SparkSession initialized, version: ", spark.version)
    return spark
