from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)
print("Data generation in progress...")
# ===================================================================================================================
print_header("Data BEFORE adding file")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           """)
 .show(truncate=False))

# ===================================================================================================================
print_header("Add File with add_files procedure")
(spark.sql("""
           CALL local.system.add_files(
              table => 'tutorial.clients',
              source_table => '`parquet`.`../../data/data_for_add_files`'
            );
""")
 .show(truncate=False))

# ===================================================================================================================
print_header("Data AFTER adding file")
(spark.sql("""
           SELECT *
           FROM tutorial.clients
           """)
 .show(truncate=False))

print_header("✅ Done.")
spark.stop()
