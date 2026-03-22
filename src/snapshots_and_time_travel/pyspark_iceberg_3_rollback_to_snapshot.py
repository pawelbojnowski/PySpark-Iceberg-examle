from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark


def show_tutorial_clients_details(spark):
    spark.sql("SELECT * FROM tutorial.clients order by id").show(100, truncate=False)
    spark.sql("SELECT * FROM tutorial.clients.snapshots").show(truncate=False)


spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show tables in namespaces: tutorial.clients")

show_tutorial_clients_details(spark)

# ===================================================================================================================
print_header("Get snapshot id for: tutorial.clients")

snapshots = spark.sql("SELECT * FROM tutorial.clients.snapshots")
snapshot_id = snapshots.first()["snapshot_id"]
print("snapshot_id", snapshot_id)

# ===================================================================================================================
print_header("Append new data: tutorial.clients")
data = [
    (1000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (1000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients")
 .append())

show_tutorial_clients_details(spark)

# ===================================================================================================================
print_header("Rollback to snapshot")

(spark.sql(f"""CALL local.system.rollback_to_snapshot(
                 table => 'tutorial.clients',
                 snapshot_id => {snapshot_id}
                 );""")).show()

# ===================================================================================================================
print_header("Show current state of table")
show_tutorial_clients_details(spark)

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
