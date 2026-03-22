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
print_header("Metadata BEFORE remove orphan files")
(spark.sql("""
           SELECT *
           FROM tutorial.example_for_maintenance.snapshots
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Remove orphan files")
# removes files not referenced by any snapshot, older than the given timestamp
spark.sql("""
    CALL local.system.remove_orphan_files(
        table => 'tutorial.example_for_maintenance',
        older_than => TIMESTAMP '2026-03-16 23:00:00'
    )
 """).show(truncate=False)

# ===================================================================================================================
print_header("Metadata AFTER remove orphan files")
(spark.sql("""
           SELECT *
           FROM tutorial.example_for_maintenance.snapshots
           """)
 .show(truncate=False))

print_header("✅ Done.")
spark.stop()
