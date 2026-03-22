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
for i in range(1, 100):
    data = []
    for j in range(1, 100):
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

(spark.sql("""
           DELETE
           FROM tutorial.example_for_maintenance
           WHERE country_code = 'US'
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Metadata BEFORE compaction")
(spark.sql("""
           SELECT 'tutorial.example_for_maintenance.snapshots', count(*) as count
           FROM tutorial.example_for_maintenance.snapshots
           UNION ALL
           SELECT 'tutorial.example_for_maintenance.files', count(*) as count
           FROM tutorial.example_for_maintenance.files
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Compaction with rewrite_data_files procedure")
spark.sql("""
CALL local.system.rewrite_position_delete_files(
  table => 'tutorial.example_for_maintenance',
  options => map(
    'min-input-files', '5',
    'target-file-size-bytes', '1073741824'
  )
)
""").show(truncate=False)

# ===================================================================================================================
print_header("Metadata AFTER compaction")

(spark.sql("""
           SELECT 'tutorial.example_for_maintenance.snapshots', count(*) as count
           FROM tutorial.example_for_maintenance.snapshots
           UNION ALL
           SELECT 'tutorial.example_for_maintenance.files', count(*) as count
           FROM tutorial.example_for_maintenance.files
           """)
 .show(truncate=False))

(spark.sql("""
           SELECT file_size_in_bytes, record_count, file_path
           FROM tutorial.example_for_maintenance.files
           """)
 .show(truncate=False))

print_header("✅ Done.")
spark.stop()
