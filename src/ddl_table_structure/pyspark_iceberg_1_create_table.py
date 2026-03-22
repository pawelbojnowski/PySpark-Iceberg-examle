from src.base.data_loader import recreate_namespace
from src.base.print import print_header
from src.base.spark import init_spark

spark = init_spark()

# Namespace initialization
recreate_namespace(spark)

# ===================================================================================================================
print_header("Create DataFrame")
data = [
    ("Anna", "Smith", "12 Maple Street, New York", "+1 202 555 0101", "US"),
    ("Oliver", "Brown", "221B Baker Street, London", "+44 20 7946 0958", "UK"),
    ("Lucas", "Martin", "10 Rue de Rivoli, Paris", "+33 1 42 68 53 00", "FR"),
    ("Sofia", "Rossi", "Via Roma 15, Milan", "+39 02 1234 5678", "IT"),
    ("Liam", "Murphy", "8 O'Connell Street, Dublin", "+353 1 234 5678", "IE"),
    ("Mia", "Garcia", "Calle Gran Via 45, Madrid", "+34 91 123 4567", "ES"),
    ("Noah", "Müller", "Alexanderplatz 3, Berlin", "+49 30 123456", "DE"),
    ("Emma", "Dubois", "Place Royale 1, Brussels", "+32 2 123 45 67", "BE"),
    ("William", "Silva", "Av. Paulista 1000, São Paulo", "+55 11 91234 5678", "BR"),
    ("Ava", "Kowalski", "ul. Marszałkowska 10, Warsaw", "+48 22 123 45 67", "PL"),
]
df = spark.createDataFrame(data, ["firstname", "lastname", "address", "phone", "country_code"])

# ===================================================================================================================
print_header("Show schema")
df.printSchema()

# ===================================================================================================================
print_header("Show data")
df.show(truncate=False)

# ===================================================================================================================
print_header("Save data to Table in local/init_table/clients")
(
    df.writeTo("init_table.clients")
    .using("iceberg")
    .tableProperty("format-version", "3")
    .partitionedBy("country_code")
    .createOrReplace()
)

# ===================================================================================================================
print_header("Show namespaces")
spark.sql("SHOW NAMESPACES IN local.init_table").show(truncate=False)

# ===================================================================================================================
print_header("Show tables in namespaces: init_table")
spark.sql("SHOW TABLES IN init_table").show(truncate=False)

# ===================================================================================================================
print_header("Select data from: init_table.clients")
spark.sql("SELECT * FROM init_table.clients").show(truncate=False)

# ===================================================================================================================
print_header("✅ Done.")
spark.stop()
