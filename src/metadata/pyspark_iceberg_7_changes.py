from faker import Faker

from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

fake = Faker()
spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Update new data: tutorial.clients")

for _ in range(1, 10):
    data = [(1, "Jeremy", "Washington", "tkennedy@example.net", fake.phone_number(), "AU")]
    new_data = spark.createDataFrame(data, SCHEMA_CLIENTS)
    new_data.createOrReplaceTempView("updates")
    spark.sql("""
        MERGE INTO tutorial.clients t
        USING updates s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *""")

# ===================================================================================================================
print_header("Select current and previous state")
spark.sql(f"""SELECT * FROM tutorial.clients.changes  where id = 1 order by _change_ordinal""").show(truncate=False)

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
