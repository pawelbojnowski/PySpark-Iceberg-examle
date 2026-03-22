import json

from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show snapshot info for tutorial.clients.snapshots")
(spark.sql("""
           SELECT *
           FROM tutorial.clients.snapshots
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
           FROM tutorial.clients.snapshots
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
           FROM tutorial.clients.snapshots
           """)
 .show(truncate=False))

print_header("Example snapshot information")

snapshot = (spark.sql("""
                      SELECT *
                      FROM tutorial.clients.snapshots
                      where parent_id is not null
                      """)
            .first())

print("committed_at: ", snapshot['committed_at'])
print("snapshot_id: ", snapshot['snapshot_id'])
print("parent_id: ", snapshot['parent_id'])
print("operation: ", snapshot['operation'])
print("manifest_list: ", snapshot['manifest_list'])
print("summary: ", json.dumps(snapshot['summary'], indent=10))

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
