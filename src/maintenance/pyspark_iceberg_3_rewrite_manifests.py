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
print_header("Metadata BEFORE compaction")
(spark.sql("""
           SELECT 'tutorial.example_for_maintenance.manifests', count(*) as count
           FROM tutorial.example_for_maintenance.snapshots
           UNION ALL
           SELECT 'tutorial.example_for_maintenance.files', count(*) as count
           FROM tutorial.example_for_maintenance.files
           """)
 .show(truncate=False))
print("Files...")
(spark.sql("""
           SELECT partition, file_path
           FROM tutorial.example_for_maintenance.files
           """)
 .show(truncate=False))
print("Manifests...")

# ===================================================================================================================
print_header("Rewrite manifests")
spark.sql("""
    CALL local.system.rewrite_manifests(
      table => 'tutorial.example_for_maintenance'
    )
 """).show(truncate=False)

# ===================================================================================================================
print_header("Metadata AFTER compaction")
(spark.sql("""
           SELECT 'tutorial.example_for_maintenance.manifests', count(*) as count
           FROM tutorial.example_for_maintenance.snapshots
           UNION ALL
           SELECT 'tutorial.example_for_maintenance.files', count(*) as count
           FROM tutorial.example_for_maintenance.files
           """)
 .show(truncate=False))
print("Check 'partition' order...")
(spark.sql("""
           SELECT partition, file_path
           FROM tutorial.example_for_maintenance.files
           """)
 .show(truncate=False))
print("Check manifests row count...")
(spark.sql("""
           SELECT *
           FROM tutorial.example_for_maintenance.manifests
           """)
 .show(truncate=False))

print_header("✅ Done.")
spark.stop()
