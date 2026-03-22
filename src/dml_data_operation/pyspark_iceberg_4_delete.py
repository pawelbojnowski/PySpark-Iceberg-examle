from src.base.data_loader import recreate_namespace, create_clients_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show records before delete")
print("Before delete")
spark.sql("SELECT * FROM tutorial.clients ORDER BY id ").show(truncate=False)

# ===================================================================================================================
delete_query = "DELETE FROM tutorial.clients WHERE id = 1"
print("Query: ")
print(delete_query)
spark.sql(delete_query)

# ===================================================================================================================
print("\nAfter delete")
spark.sql("SELECT * FROM tutorial.clients ORDER BY id").show(truncate=False)

print_header("✅ Done.")
spark.stop()
