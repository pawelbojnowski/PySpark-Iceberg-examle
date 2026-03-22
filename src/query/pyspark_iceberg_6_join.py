from src.base.data_loader import recreate_namespace, create_clients_table, create_address_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)
create_address_table(spark)

# ===================================================================================================================
print_header("Select all columns of tutorial.clients")
spark.sql("SELECT * FROM tutorial.clients").show(truncate=False)

# ===================================================================================================================
print_header("Select all columns of tutorial.address")
spark.sql("SELECT * FROM tutorial.address").show(truncate=False)

# ===================================================================================================================
print_header("(INNER) JOIN")
(spark.sql("""
           SELECT c.*, " -> ", a.*
           FROM tutorial.clients c
                    join tutorial.address a on c.id = a.client_id
           ORDER BY c.id, a.id
           """)
 .show(100, truncate=False))

# ===================================================================================================================
print_header("LEFT")
(spark.sql("""
           SELECT c.*, " -> ", a.*
           FROM tutorial.clients c
                    left join tutorial.address a on c.id = a.client_id
           ORDER BY c.id, a.id
           """)
 .show(100, truncate=False))

# ===================================================================================================================
print_header("RIGHT")
(spark.sql("""
           SELECT c.*, " -> ", a.*
           FROM tutorial.clients c
                    right join tutorial.address a on c.id = a.client_id
           ORDER BY c.id, a.id
           """)
 .show(100, truncate=False))

# ===================================================================================================================
print_header("FULL OUTER JOIN")
(spark.sql("""
           SELECT c.*, " -> ", a.*
           FROM tutorial.clients c
                    FULL OUTER JOIN tutorial.address a on c.id = a.client_id
           ORDER BY c.id, a.id
           """)
 .show(100, truncate=False))

# ===================================================================================================================
print_header("CROSS JOIN")
(spark.sql("""
           SELECT c.*, " -> ", a.*
           FROM tutorial.clients c
                    CROSS JOIN tutorial.address a ON TRUE
           ORDER BY c.id, a.id
           """)
 .show(100, truncate=False))
# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
