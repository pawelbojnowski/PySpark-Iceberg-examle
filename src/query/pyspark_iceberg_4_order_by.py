from src.base.data_loader import recreate_namespace, create_clients_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show all records")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Show records ordered by country_code")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           order by country_code
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Show records ordered by country_code, country_code")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           order by country_code, country_code
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
