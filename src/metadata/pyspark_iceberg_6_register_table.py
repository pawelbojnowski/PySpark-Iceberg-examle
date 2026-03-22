import random

from faker import Faker

from src.base.data_loader import recreate_namespace, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
fake = Faker()
data = []
for j in range(1, 10):
    country = random.choice(["PL", "US"])
    data.append((j,
                 fake.first_name(),
                 fake.last_name(),
                 fake.email(),
                 fake.phone_number(),
                 country))
(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients")
 .using("iceberg")
 .tableProperty("format-version", "3")
 .partitionedBy("country_code")
 .create()
 )

# ===================================================================================================================
print_header("Register table tutorial.clients")
(spark.sql("""
           CALL local.system.register_table(
            table => 'tutorial.clients_2',
            metadata_file => '../../warehouse/tutorial/clients/metadata/v1.metadata.json'
            );
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Register table tutorial.clients")
(spark.sql("""
           SELECT *
           FROM tutorial.clients_2.partitions
           """)
 .show(truncate=False))

(spark.sql("""
           SELECT *
           FROM tutorial.clients_2
           """)
 .show(truncate=False))

print_header("✅ Done.")
spark.stop()
