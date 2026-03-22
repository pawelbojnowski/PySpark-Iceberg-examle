from src.base.data_loader import recreate_namespace, create_clients_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Show all records form tutorial.clients")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Show records where the country_code is equals 'PL'")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           where country_code = 'PL'
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Show records where the address contains '+1'")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           where phone_number like "%+1%"
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Show records where the email contain 'example.org' and the country_code is equals 'NZ")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           where email like "%example.org%"
             and country_code = 'GQ'
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Show records where the firstname is equals one of value ('Lauren', 'Jeremy', 'Lauren', 'Kenneth')")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           where firstname in ('Lauren', 'Jeremy', 'Lauren', 'Kenneth')
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
