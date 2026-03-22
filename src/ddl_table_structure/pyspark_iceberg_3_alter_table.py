from typing import List

from src.base.data_loader import recreate_namespace, create_clients_table
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()


def run_query(description: str, query: str, additional_queries: List[str] = []):
    print_header(description)
    spark.sql(query)
    spark.sql("SELECT * FROM tutorial.clients").show(truncate=False)
    spark.sql("DESCRIBE TABLE EXTENDED tutorial.clients").show(truncate=False)
    for other_query in additional_queries:
        (spark.sql(other_query).show(truncate=False))

    print("---------------------------------------------------------------------------------------------------\n")


# Table initialization
recreate_namespace(spark)
create_clients_table(spark)

# ===================================================================================================================
print_header("Select data from: tutorial.clients before adding column")
spark.sql("SELECT * FROM tutorial.clients").show(truncate=False)
spark.sql("DESCRIBE TABLE tutorial.clients").show(truncate=False)

# https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table


run_query("Alter table: tutorial.clients by adding column\n",
          """ALTER TABLE tutorial.clients
              ADD COLUMN is_active BOOLEAN COMMENT 'active users'""");

run_query("Alter table: tutorial.clients by dropping column",
          """ALTER TABLE tutorial.clients DROP COLUMN email""")

run_query("Alter table: tutorial.clients by renaming column",
          """ALTER TABLE tutorial.clients RENAME COLUMN phone_number TO phone""")

run_query("Alter table: tutorial.clients by altering column comment",
          """ALTER TABLE tutorial.clients ALTER COLUMN id COMMENT 'Unique identifier'""")

run_query("Alter table: tutorial.clients by updating table properties",
          """ALTER TABLE tutorial.clients
              SET TBLPROPERTIES (
              'read.split.target-size'='268435456'
              );""",
          ["SHOW TBLPROPERTIES tutorial.clients;"]
          )

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
