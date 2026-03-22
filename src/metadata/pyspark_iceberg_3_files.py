import json

from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show files info for tutorial.clients.files")
(spark.sql("""
           SELECT *
           FROM tutorial.clients.files
           """)
 .show(truncate=False))

data = [
    (1000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (1000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]
print("Add new data: ", data)
(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients")
 .append()
 )

(spark.sql("""
           SELECT *
           FROM tutorial.clients.files
           """)
 .show(truncate=False))

# ===================================================================================================================

data = [
    (2000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (2000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

print("Add another data: ", data)
(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients")
 .append()
 )

(spark.sql("""
           SELECT *
           FROM tutorial.clients.files
           """)
 .show(truncate=False))

print_header("Example files information")

files = (spark.sql("""
                   SELECT *
                   FROM tutorial.clients.files
                   """)
         .first())

print("content: ", files['content'])
print("file_path: ", files['file_path'])
print("file_format: ", files['file_format'])
print("spec_id: ", files['spec_id'])
print("partition: ", files['partition'])
print("record_count: ", files['record_count'])
print("file_size_in_bytes: ", files['file_size_in_bytes'])
print("column_sizes: ", files['column_sizes'])
print("value_counts: ", files['value_counts'])
print("null_value_counts: ", files['null_value_counts'])
print("nan_value_counts: ", files['nan_value_counts'])
print("lower_bounds: ", files['lower_bounds'])
print("upper_bounds: ", files['upper_bounds'])
print("key_metadata: ", files['key_metadata'])
print("split_offsets: ", files['split_offsets'])
print("equality_ids: ", files['equality_ids'])
print("sort_order_id: ", files['sort_order_id'])
print("first_row_id: ", files['first_row_id'])
print("referenced_data_file: ", files['referenced_data_file'])
print("content_offset: ", files['content_offset'])
print("content_size_in_bytes: ", files['content_size_in_bytes'])
print("readable_metrics: ", json.dumps(files['readable_metrics'].asDict(recursive=True), indent=10))

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
