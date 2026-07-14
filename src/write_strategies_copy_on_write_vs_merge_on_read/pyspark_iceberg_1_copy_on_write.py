from src.base.data_loader import recreate_namespace, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

# =============================================================================
# COPY-ON-WRITE (CoW)
#
# Strategy: When a record is updated or deleted, Iceberg rewrites the entire
# data file that contains the affected rows and produces a new snapshot.
# No separate delete files are ever created.
#
# Table properties:
#   write.delete.mode = copy-on-write  (default)
#   write.update.mode = copy-on-write  (default)
#   write.merge.mode  = copy-on-write  (default)
#
# Trade-offs:
#   Reads  – fast: queries scan only clean data files, no merging needed.
#   Writes – slow: every write rewrites full Parquet files even for one-row
#            changes; amplifies I/O on large partitions.
#
# Best for: read-heavy workloads where write throughput is not a bottleneck.
# =============================================================================

spark = init_spark()

recreate_namespace(spark)


def show_files(table: str) -> None:
    # content = 0 → DATA file
    # content = 1 → POSITION_DELETES file
    # content = 2 → EQUALITY_DELETES file
    spark.sql(f"""
        SELECT content,
               CASE content WHEN 0 THEN 'DATA'
                            WHEN 1 THEN 'POSITION_DELETES'
                            WHEN 2 THEN 'EQUALITY_DELETES'
                            ELSE        'OTHER'
               END AS content_type,
               record_count,
               file_size_in_bytes,
               file_path
        FROM   {table}.files
        ORDER  BY content, file_path
    """).show(truncate=False)

    data_files   = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}.files WHERE content = 0").first()["cnt"]
    delete_files = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}.files WHERE content IN (1, 2)").first()["cnt"]
    print(f"  data files:   {data_files}")
    print(f"  delete files: {delete_files}  ← CoW never produces delete files")


# ---------------------------------------------------------------------------
print_header("1. Create table with Copy-on-Write mode")
# ---------------------------------------------------------------------------

spark.sql("""
    CREATE TABLE tutorial.clients_cow (
        id           INT,
        firstname    STRING,
        lastname     STRING,
        email        STRING,
        phone_number STRING,
        country_code STRING
    )
    USING iceberg
    PARTITIONED BY (country_code)
    TBLPROPERTIES (
        'format-version'     = '2',
        'write.delete.mode'  = 'copy-on-write',
        'write.update.mode'  = 'copy-on-write',
        'write.merge.mode'   = 'copy-on-write'
    )
""")

spark.sql("SHOW TBLPROPERTIES tutorial.clients_cow").show(truncate=False)

# ---------------------------------------------------------------------------
print_header("2. Insert initial data")
# ---------------------------------------------------------------------------

data = [
    (1, "Alice",   "Smith",   "alice@example.com",   "111-111-1111", "US"),
    (2, "Bob",     "Jones",   "bob@example.com",     "222-222-2222", "US"),
    (3, "Charlie", "Brown",   "charlie@example.com", "333-333-3333", "DE"),
    (4, "Diana",   "Prince",  "diana@example.com",   "444-444-4444", "DE"),
    (5, "Eve",     "Taylor",  "eve@example.com",     "555-555-5555", "FR"),
]

(spark.createDataFrame(data, SCHEMA_CLIENTS)
 .writeTo("tutorial.clients_cow")
 .append())

spark.sql("SELECT * FROM tutorial.clients_cow ORDER BY id").show(truncate=False)

# ---------------------------------------------------------------------------
print_header("3. Files BEFORE any mutation")
# ---------------------------------------------------------------------------

show_files("tutorial.clients_cow")

# ---------------------------------------------------------------------------
print_header("4. UPDATE id=1  (phone_number change)")
# ---------------------------------------------------------------------------
# CoW rewrites the entire data file of the US partition.
# The old file is replaced; a new data file appears in the next snapshot.

print("SQL: UPDATE tutorial.clients_cow SET phone_number = '999-000-0001' WHERE id = 1\n")
spark.sql("""
    UPDATE tutorial.clients_cow
    SET    phone_number = '999-000-0001'
    WHERE  id = 1
""")

# ---------------------------------------------------------------------------
print_header("5. Files AFTER UPDATE  →  only DATA files, no delete files")
# ---------------------------------------------------------------------------
# Key: content=1 (POSITION_DELETES) does NOT appear.
# The US-partition data file was REWRITTEN with the new phone number.

show_files("tutorial.clients_cow")

# ---------------------------------------------------------------------------
print_header("6. DELETE id=3")
# ---------------------------------------------------------------------------
# CoW again rewrites the DE-partition data file, removing id=3 physically.

print("SQL: DELETE FROM tutorial.clients_cow WHERE id = 3\n")
spark.sql("DELETE FROM tutorial.clients_cow WHERE id = 3")

# ---------------------------------------------------------------------------
print_header("7. Files AFTER DELETE  →  still only DATA files, no delete files")
# ---------------------------------------------------------------------------

show_files("tutorial.clients_cow")

# ---------------------------------------------------------------------------
print_header("8. Final data (SELECT *)")
# ---------------------------------------------------------------------------

spark.sql("SELECT * FROM tutorial.clients_cow ORDER BY id").show(truncate=False)

# ---------------------------------------------------------------------------
print_header("9. Snapshots")
# ---------------------------------------------------------------------------

spark.sql("""
    SELECT snapshot_id, committed_at, operation, summary
    FROM   tutorial.clients_cow.snapshots
    ORDER  BY committed_at
""").show(truncate=False)

# ---------------------------------------------------------------------------
print_header("10. REWRITE_DATA_FILES — compact small files  (optional for CoW)")
# ---------------------------------------------------------------------------
# CoW has no delete files to eliminate, so REWRITE_DATA_FILES serves a
# different purpose here: merging many small data files into fewer larger ones
# (e.g. after many small inserts).  The result is the same clean data files
# as before — just fewer and larger.

print("Running REWRITE_DATA_FILES with binpack strategy...\n")
spark.sql("""
    CALL local.system.rewrite_data_files(
        table    => 'tutorial.clients_cow',
        strategy => 'binpack'
    )
""").show(truncate=False)

print("\nFiles after compaction:")
show_files("tutorial.clients_cow")

# ---------------------------------------------------------------------------
print_header("Summary: Copy-on-Write")
# ---------------------------------------------------------------------------
print("""
CoW — what happened under the hood:
  Step 2  INSERT → wrote 3 data files (one per partition: US, DE, FR).
  Step 4  UPDATE → US-partition data file REWRITTEN with updated phone_number.
                   Old file retired in manifest.  No delete files.
  Step 6  DELETE → DE-partition data file REWRITTEN without id=3.
                   No delete files.
  Step 10 REWRITE → merges small data files into larger ones.
                    No delete files to eliminate — only file-size optimisation.

At every snapshot:
  • Files table shows ONLY content=0 (DATA).
  • Zero delete files ever exist.
  • Reads are always clean — no merging required.
  • Cost: full file rewrite even for a single-row change (write amplification).
  • REWRITE_DATA_FILES on CoW = small-file compaction only, not delete cleanup.
""")

print_header("✅ Done.")
spark.stop()
