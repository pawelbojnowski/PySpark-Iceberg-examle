from datetime import datetime

from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show tables in namespaces: tutorial.clients")
spark.sql("SELECT * FROM tutorial.clients").show(truncate=False)

current_timestamp = datetime.now()
print(f"Current timestamp: {current_timestamp}")

# ===================================================================================================================
print_header("Append new data: tutorial.clients")
data = [
    (1000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (1000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

new_data = spark.createDataFrame(data, SCHEMA_CLIENTS)

new_data.show(truncate=False)

(new_data
 .writeTo("tutorial.clients")
 .append())

# ===================================================================================================================
print_header("Select current and previous state")

print("Current state: tutorial.clients")
spark.sql("SELECT * FROM tutorial.clients order by id").show(truncate=False)

print(f"State before appending new records: tutorial.clients for timestamp {current_timestamp}")
spark.sql(f"SELECT * FROM tutorial.clients TIMESTAMP AS OF '{current_timestamp}'").show(truncate=False)

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
