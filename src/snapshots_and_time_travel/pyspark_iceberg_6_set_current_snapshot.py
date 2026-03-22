from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Get current snapshot id for: tutorial.clients")

snapshots = spark.sql("""
                      SELECT snapshot_id
                      FROM tutorial.clients.history
                      ORDER BY made_current_at DESC
                          LIMIT 1;
""")
snapshot_id = snapshots.first()["snapshot_id"]
print("snapshot_id", snapshot_id)

# ===================================================================================================================
print_header("Append new data: 'tutorial.clients'")
data = [
    (1000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (1000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

print("Data: ", data)

(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients")
 .append()
 )

# ===================================================================================================================
print_header("Change current snapshot to snapshot_id: " + str(snapshot_id))
spark.sql(f"""
        CALL local.system.set_current_snapshot(
        table => 'tutorial.clients',
        snapshot_id => {snapshot_id}
);
""").show(truncate=False)

# ===================================================================================================================
print_header("Show data from: tutorial.clients")

spark.sql("SELECT * FROM tutorial.clients order by id").show(truncate=False)
spark.sql("SELECT * FROM tutorial.clients.snapshots").show(truncate=False)
spark.sql("SELECT * FROM tutorial.clients.history").show(truncate=False)

print_header("✅ Done.")
spark.stop()
