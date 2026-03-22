from src.base.data_loader import recreate_namespace, create_clients_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)
#
# ===================================================================================================================
print_header("Select data from: tutorial.clients")
spark.sql("SELECT * FROM tutorial.clients").show(truncate=False)
spark.sql("DESCRIBE EXTENDED tutorial.clients").show(truncate=False)

# ===================================================================================================================
print_header("Rename table form 'tutorial.clients' to 'tutorial.external_clients'")
spark.sql("ALTER TABLE tutorial.clients RENAME TO tutorial.external_clients").show(
    truncate=False)

# ===================================================================================================================
print_header("Select data from: tutorial.external_clients ")
spark.sql("SELECT * FROM tutorial.external_clients").show(truncate=False)
spark.sql("DESCRIBE TABLE tutorial.external_clients").show(truncate=False)

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
