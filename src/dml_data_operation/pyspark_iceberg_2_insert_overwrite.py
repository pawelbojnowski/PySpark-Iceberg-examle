from src.base.data_loader import recreate_namespace, create_clients_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("INSERT INTO with Spark SQL (in Iceberg)")
insert_query = """INSERT OVERWRITE tutorial.clients
                  VALUES (1000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
                         (1000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ") \
               """
print("Query: ", insert_query)

spark.sql(insert_query)

(spark.sql("SELECT *  FROM tutorial.clients ORDER BY id ").show(truncate=False))

# ===================================================================================================================
print_header("INSERT INTO with PySpark")
data = [
    (2000000, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
    (2000001, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
]
print("Data: ", data)
columns = ["id", "firstname", "lastname", "email", "phone_number", "country_code"]
print("Columns: ", columns)

(spark.createDataFrame(data, columns)
 .writeTo("tutorial.clients")
 .overwritePartitions())

(spark.sql("SELECT *  FROM tutorial.clients ORDER BY id ").show(truncate=False))

print_header("✅ Done.")
spark.stop()
