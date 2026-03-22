from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show data from: tutorial.clients")

spark.sql("SELECT * FROM tutorial.clients order by id").show(100, truncate=False)

# ===================================================================================================================
print_header("Create branch 'dev_branch' from current set of data")

spark.sql("""
          ALTER TABLE tutorial.clients
          CREATE
          BRANCH dev_branch
          """)

spark.sql("""
          SELECT *
          FROM tutorial.clients.refs
          """).show(truncate=False)

# ===================================================================================================================
print_header("Append new data: 'tutorial.clients' to 'main' branch")
data = [
    (1000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (1000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients")
 .append()
 )

print("Show data from 'main' branch")
spark.sql("SELECT * FROM tutorial.clients order by id").show(100, truncate=False)

print("Show data from 'dev_branch' branch")
spark.sql("SELECT * FROM tutorial.clients VERSION AS OF 'dev_branch' order by id ").show(100, truncate=False)

# ===================================================================================================================
print_header("Append new data to 'tutorial.clients' to branch: 'branch_dev_branch'")
data = [
    (2000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (2000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]

(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients.branch_dev_branch")
 .append())

print("Show data from 'main' branch")
spark.sql("SELECT * FROM tutorial.clients order by id").show(100, truncate=False)

print("Show data from 'dev_branch' branch")
spark.sql("SELECT * FROM tutorial.clients VERSION AS OF 'dev_branch' order by id ").show(100, truncate=False)

print_header("✅ Done.")
spark.stop()
