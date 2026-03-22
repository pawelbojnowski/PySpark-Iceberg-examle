from src.base.data_loader import recreate_namespace, create_clients_table, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Table initialization
recreate_namespace(spark)
create_clients_table(spark)


def insert_or_update_all_columns_example():
    print_header("MERGE INTO - insert or update all columns")
    # ===================================================================================================================
    print("Before MERGE INTO:")
    spark.sql("SELECT * FROM tutorial.clients ORDER BY id ").show(truncate=False)

    # ===================================================================================================================

    data = [
        (1, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
        (1000, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
    ]
    new_data = spark.createDataFrame(data, SCHEMA_CLIENTS)
    new_data.createOrReplaceTempView("updates")
    print("New data:")
    new_data.show(truncate=False)

    spark.sql("""
        MERGE INTO tutorial.clients t
        USING updates s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *""")

    # ===================================================================================================================
    print("After MERGE INTO:")
    spark.sql("SELECT * FROM tutorial.clients ORDER BY id").show(truncate=False)


def insert_or_update_selected_columns_example():
    print_header("MERGE INTO - insert or update selected columns")
    # ===================================================================================================================
    print("Before MERGE INTO:")
    spark.sql("SELECT * FROM tutorial.clients ORDER BY id ").show(truncate=False)

    # ===================================================================================================================

    data = [
        (1, "Alex", "Gottard", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
        (100, "Max", "Pass", "beasleylarry@example.org", "730-750-7042x8804", "NZ")
    ]
    new_data = spark.createDataFrame(data, SCHEMA_CLIENTS)
    new_data.createOrReplaceTempView("updates")
    print("New data:")
    new_data.show(truncate=False)

    spark.sql("""
        MERGE INTO tutorial.clients t
        USING updates s
        ON t.id = s.id
        WHEN MATCHED THEN
          UPDATE SET 
            t.firstname = s.firstname,
            t.lastname = s.lastname
        WHEN NOT MATCHED THEN
          INSERT (id, firstname, lastname, email, phone_number, country_code)
          VALUES (s.id, s.firstname, s.lastname, s.email, s.phone_number, s.country_code)
          """)

    # ===================================================================================================================
    print("After MERGE INTO:")
    spark.sql("SELECT * FROM tutorial.clients ORDER BY id").show(truncate=False)


def insert_or_update_or_delete():
    print_header("MERGE INTO - insert or update or delete")
    # ===================================================================================================================
    print("Before MERGE INTO:")
    spark.sql("SELECT * FROM tutorial.clients ORDER BY id ").show(truncate=False)

    # ===================================================================================================================

    data = [
        (1, "Jeremy", "Washington", "tkennedy@example.net", "001-609-623-4714x41897", "AU"),
        (2, "Scott", "Anderson", "beasleylarry@example.org", "730-750-7042x8804", None)
    ]
    new_data = spark.createDataFrame(data, SCHEMA_CLIENTS)
    new_data.createOrReplaceTempView("updates")
    print("New data:")
    new_data.show(truncate=False)

    spark.sql("""
    MERGE INTO tutorial.clients t
    USING updates s
    ON t.id = s.id
    WHEN MATCHED AND s.country_code IS NULL
    THEN DELETE
    WHEN MATCHED THEN
    UPDATE SET *
    WHEN NOT MATCHED THEN
    INSERT *""")

    # ===================================================================================================================
    print("After MERGE INTO:")
    spark.sql("SELECT * FROM tutorial.clients ORDER BY id").show(truncate=False)


insert_or_update_all_columns_example()
insert_or_update_selected_columns_example()
insert_or_update_or_delete()

print_header("✅ Done.")
spark.stop()
