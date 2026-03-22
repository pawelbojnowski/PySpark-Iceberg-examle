from src.base.data_loader import create_clients_table, recreate_namespace
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark, "table_clients")
create_clients_table(spark, "table_clients_to_drop")

# ===================================================================================================================
print_header("Show tables in namespaces: tutorial")
spark.sql("SHOW TABLES IN tutorial").show(truncate=False)

# ===================================================================================================================
print_header("Drop table: tutorial.table_clients_to_drop")
spark.sql("DROP TABLE tutorial.table_clients_to_drop")

# ===================================================================================================================
print_header("Show tables in namespaces: tutorial")
spark.sql("SHOW TABLES IN tutorial").show(truncate=False)
# Table "table_to_drop" should not be present in result

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
