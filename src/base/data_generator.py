import os

from faker import Faker
from pyspark.sql.functions import col, explode
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType, TimestampType

from src.base.print import print_header
from src.base.spark import init_spark

DATA_GENERATOR_SCHEMA_ADDRESS = StructType([
    StructField("address_id", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

DATA_GENERATOR_SCHEMA_CLIENTS = StructType([
    StructField("client_id", IntegerType(), False),
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("addresses", ArrayType(DATA_GENERATOR_SCHEMA_ADDRESS), True)
])

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data")


def save_to_json_file(base_path: str, df, file: str) -> None:
    (df.toPandas()
     .to_json(
         os.path.join(base_path, f"{file}.json"),
         orient="records",
         force_ascii=False
     ))


def generate_fake_data_for_tutorial(spark, number_of_rows: int) -> None:
    fake = Faker()
    data = []
    last_address_id = 0
    for i in range(number_of_rows):
        data.append(
            (
                i + 1,
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                fake.phone_number(),
                fake.country_code(),
                [
                    {
                        "address_id": (last_address_id := last_address_id + 1),
                        "address": fake.address().replace("\n", ", "),
                        "created_at": fake.date_time()
                    }
                    for _ in range(fake.random_int(min=2, max=3))
                ]
            )
        )

    df = spark.createDataFrame(data, DATA_GENERATOR_SCHEMA_CLIENTS)
    os.makedirs(_DATA_DIR, exist_ok=True)

    clients = df.select(col("client_id").alias("id"),
                        col("firstname"),
                        col("lastname"),
                        col("email"),
                        col("phone_number"),
                        col("country_code"))
    save_to_json_file(_DATA_DIR, clients, "clients")

    address = (df.select(
        col("client_id"),
        explode("addresses").alias("address")
    ))
    address = (address.select(
        col("address.address_id").alias("id"),
        col("client_id"),
        col("address.address"),
        col("address.created_at"),
    ))
    save_to_json_file(_DATA_DIR, address, "address")


# ===================================================================================================================
print_header("Generating data files...")
spark = init_spark()
generate_fake_data_for_tutorial(spark, 10)
# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
