from src.base.data_loader import recreate_namespace, create_clients_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Select all columns of tutorial.clients")
spark.sql("SELECT * FROM tutorial.clients").show(truncate=False)

# ===================================================================================================================
print_header("Select only needed columns of tutorial.clients")
(spark.sql("SELECT firstname, lastname, phone_number FROM tutorial.clients")
 .show(truncate=False))

# ===================================================================================================================
print_header("Select add modify columns of tutorial.clients")
(spark.sql("SELECT UPPER(firstname), UPPER(lastname), phone_number FROM tutorial.clients")
 .show(truncate=False))

# ===================================================================================================================
print_header("Select add modify and set new name for columns of tutorial.clients")
(spark.sql("SELECT UPPER(firstname) as firstname, UPPER(lastname) as lastname, phone_number FROM tutorial.clients")
 .show(truncate=False))

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
