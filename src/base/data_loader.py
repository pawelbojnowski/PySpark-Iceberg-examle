import os

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

NAMESPACE = "tutorial"

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data")

SCHEMA_ADDRESS = StructType([
    StructField("id", IntegerType(), False),
    StructField("client_id", IntegerType(), False),
    StructField("address", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

SCHEMA_CLIENTS = StructType([
    StructField("id", IntegerType(), False),
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("country_code", StringType(), True)
])


def recreate_namespace(spark: SparkSession) -> None:
    if spark.catalog.databaseExists(NAMESPACE):
        tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {NAMESPACE}").collect()]
        for t in tables:
            spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{t}")
        spark.sql(f"DROP NAMESPACE IF EXISTS {NAMESPACE} CASCADE")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")


def create_table(spark: SparkSession, input_file: str, schema: StructType, table_name: str, partition_by: str) -> None:
    df = (spark.read
          .option("multiLine", True)
          .schema(schema)
          .json(input_file))

    (df.writeTo(f"{NAMESPACE}.{table_name}")
     .using("iceberg")
     .tableProperty("format-version", "3")
     .partitionedBy(partition_by)
     .create()
     )


def create_clients_table(spark: SparkSession, table_name: str = "clients") -> None:
    create_table(spark, os.path.join(_DATA_DIR, "clients.json"), SCHEMA_CLIENTS, table_name, "country_code")


def create_address_table(spark: SparkSession, table_name: str = "address") -> None:
    create_table(spark, os.path.join(_DATA_DIR, "address.json"), SCHEMA_ADDRESS, table_name, "created_at")
