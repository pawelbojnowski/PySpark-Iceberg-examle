import random

from faker import Faker

from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark, table_name="example_for_maintenance")

print("Data generation in progress...")
fake = Faker()
for i in range(1, 10):
    data = []
    for j in range(1, 2):
        country = random.choice(["PL", "US"])
        data.append(((1000 * i) + j,
                     fake.first_name(),
                     fake.last_name(),
                     fake.email(),
                     fake.phone_number(),
                     country))
    (spark.createDataFrame(data, SCHEMA_CLIENTS)
     .writeTo("tutorial.example_for_maintenance")
     .append()
     )

# ===================================================================================================================
print_header("Metadata BEFORE expire snapshots")
(spark.sql("""
           SELECT *
           FROM tutorial.example_for_maintenance.snapshots
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Expire snapshots")
# retains only the last snapshot, removes all older ones
spark.sql("""
    CALL local.system.expire_snapshots(
        table => 'tutorial.example_for_maintenance',
        retain_last => 1
    )
 """).show(truncate=False)

# ===================================================================================================================
print_header("Metadata AFTER expire snapshots")
(spark.sql("""
           SELECT *
           FROM tutorial.example_for_maintenance.snapshots
           """)
 .show(truncate=False))

print_header("✅ Done.")
spark.stop()
