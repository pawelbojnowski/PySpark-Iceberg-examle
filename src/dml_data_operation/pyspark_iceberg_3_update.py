from src.base.data_loader import recreate_namespace, create_clients_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show record before update")
print("Before update")
spark.sql("SELECT * FROM tutorial.clients WHERE id=1 ORDER BY id ").show(truncate=False)

# ===================================================================================================================
update_query = """UPDATE tutorial.clients
                  SET phone_number = "999 999 999 999",
                      country_code = "US"
                  WHERE id = 1"""
print("Query: ")
print(update_query)
spark.sql(update_query)

# ===================================================================================================================
print("\nAfter update")
spark.sql("SELECT * FROM tutorial.clients WHERE id=1  ORDER BY id").show(truncate=False)

print_header("✅ Done.")
spark.stop()
