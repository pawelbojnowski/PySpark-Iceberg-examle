from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show manifests info for tutorial.clients.manifests")
(spark.sql("""
           SELECT *
           FROM tutorial.clients.manifests
           """)
 .show(truncate=False))

data = [
    (1000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (1000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]
print("Add new data: ", data)
spark.createDataFrame(data, SCHEMA_CLIENTS).writeTo("tutorial.clients").append()
spark.sql("SELECT * FROM tutorial.clients.manifests").show(truncate=False)

# ===================================================================================================================

data = [
    (2000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (2000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

print("Add another data: ", data)
spark.createDataFrame(data, SCHEMA_CLIENTS).writeTo("tutorial.clients").append()
spark.sql("SELECT * FROM tutorial.clients.manifests").show(truncate=False)

# ===================================================================================================================
print_header("Example manifests information")

manifests = spark.sql("SELECT * FROM tutorial.clients.manifests").first()

print("content: ", manifests['content'])
print("path: ", manifests['path'])
print("length: ", manifests['length'])
print("partition_spec_id: ", manifests['partition_spec_id'])
print("added_snapshot_id: ", manifests['added_snapshot_id'])
print("added_data_files_count: ", manifests['added_data_files_count'])
print("existing_data_files_count: ", manifests['existing_data_files_count'])
print("deleted_data_files_count: ", manifests['deleted_data_files_count'])
print("added_delete_files_count: ", manifests['added_delete_files_count'])
print("existing_delete_files_count: ", manifests['existing_delete_files_count'])
print("deleted_delete_files_count: ", manifests['deleted_delete_files_count'])
print("partition_summaries: ", manifests['partition_summaries'])

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
