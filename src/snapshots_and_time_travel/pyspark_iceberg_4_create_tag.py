from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show tables in namespaces: tutorial.clients")

spark.sql("SELECT * FROM tutorial.clients order by id").show(100, truncate=False)

# ===================================================================================================================
print_header("Create tag 'v1' for current set of data")

spark.sql("""
          ALTER TABLE tutorial.clients
          CREATE
          TAG v1
          """)

spark.sql("""
          SELECT *
          FROM tutorial.clients.refs
          """).show(truncate=False)

# ===================================================================================================================
print_header("Append new data: tutorial.clients")
data = [
    (1000000, "Donald", "Patt", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (1000001, "Jerry", "Doki", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients")
 .append())

spark.sql("SELECT * FROM tutorial.clients order by id").show(100, truncate=False)

# ===================================================================================================================
print_header("Show data current ")

spark.sql("SELECT * FROM tutorial.clients order by id").show(100, truncate=False)
spark.sql(" SELECT * FROM tutorial.clients VERSION AS OF 'v1' order by id ").show(100, truncate=False)

print_header("✅ Done.")
spark.stop()
