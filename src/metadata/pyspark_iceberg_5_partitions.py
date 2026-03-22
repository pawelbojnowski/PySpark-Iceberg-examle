from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show partitions info for tutorial.clients.partitions")
(spark.sql("""
           SELECT *
           FROM tutorial.clients.partitions
           """)
 .show(truncate=False))

data = [
    (1000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (1000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]
print("Add new data: ", data)
spark.createDataFrame(data, SCHEMA_CLIENTS).writeTo("tutorial.clients").append()
spark.sql("SELECT * FROM tutorial.clients.partitions").show(truncate=False)

# ===================================================================================================================

data = [
    (2000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (2000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

print("Add another data: ", data)
spark.createDataFrame(data, SCHEMA_CLIENTS).writeTo("tutorial.clients").append()
spark.sql("SELECT * FROM tutorial.clients.partitions").show(truncate=False)

# ===================================================================================================================
print_header("Example partitions information")

partitions = spark.sql("SELECT * FROM tutorial.clients.partitions").first()

print("partition: ", partitions['partition'])
print("spec_id: ", partitions['spec_id'])
print("record_count: ", partitions['record_count'])
print("file_count: ", partitions['file_count'])
print("total_data_file_size_in_bytes: ", partitions['total_data_file_size_in_bytes'])
print("position_delete_record_count: ", partitions['position_delete_record_count'])
print("position_delete_file_count: ", partitions['position_delete_file_count'])
print("equality_delete_record_count: ", partitions['equality_delete_record_count'])
print("equality_delete_file_count: ", partitions['equality_delete_file_count'])
print("last_updated_at: ", partitions['last_updated_at'])
print("last_updated_snapshot_id: ", partitions['last_updated_snapshot_id'])

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
