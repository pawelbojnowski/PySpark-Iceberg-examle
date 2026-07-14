from src.base.data_loader import recreate_namespace, SCHEMA_CLIENTS
from src.base.print import print_header
from src.base.spark import init_spark

# =============================================================================
# MERGE-ON-READ (MoR)
#
# Strategy: When a record is updated or deleted, Iceberg does NOT rewrite the
# original data file.  Instead it appends a small *position-delete file*
# (content=1) that records which row positions are logically removed.
# On read, the engine merges the data file with its delete files on the fly.
#
# Table properties:
#   write.delete.mode = merge-on-read
#   write.update.mode = merge-on-read
#   write.merge.mode  = merge-on-read
#
# Trade-offs:
#   Writes – fast: only a tiny delete file is written; original data is kept.
#   Reads  – slower over time: the engine must merge an increasing number of
#            delete files with data files.  Run REWRITE_DATA_FILES to compact.
#
# Best for: write-heavy / streaming workloads; CDC pipelines; cases where
#           updates are frequent but full-table scans are less common.
# =============================================================================

spark = init_spark()

recreate_namespace(spark)


def show_files(table: str) -> None:
    # content = 0 → DATA file
    # content = 1 → POSITION_DELETES file  ← appears after MoR mutations
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
    print(f"  delete files: {delete_files}  ← MoR accumulates delete files until compaction")


# ---------------------------------------------------------------------------
print_header("1. Create table with Merge-on-Read mode")
# ---------------------------------------------------------------------------

spark.sql("""
    CREATE TABLE tutorial.clients_mor (
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
        'write.delete.mode'  = 'merge-on-read',
        'write.update.mode'  = 'merge-on-read',
        'write.merge.mode'   = 'merge-on-read'
    )
""")

spark.sql("SHOW TBLPROPERTIES tutorial.clients_mor").show(truncate=False)

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
 .writeTo("tutorial.clients_mor")
 .append())

spark.sql("SELECT * FROM tutorial.clients_mor ORDER BY id").show(truncate=False)

# ---------------------------------------------------------------------------
print_header("3. Files BEFORE any mutation")
# ---------------------------------------------------------------------------

show_files("tutorial.clients_mor")

# ---------------------------------------------------------------------------
print_header("4. UPDATE id=1  (phone_number change)")
# ---------------------------------------------------------------------------
# MoR does NOT rewrite the data file.
# It writes a POSITION_DELETES file marking the old row as deleted,
# then appends a new data file with the updated row.

print("SQL: UPDATE tutorial.clients_mor SET phone_number = '999-000-0001' WHERE id = 1\n")
spark.sql("""
    UPDATE tutorial.clients_mor
    SET    phone_number = '999-000-0001'
    WHERE  id = 1
""")

# ---------------------------------------------------------------------------
print_header("5. Files AFTER UPDATE  →  DATA + POSITION_DELETES files")
# ---------------------------------------------------------------------------
# Key: content=1 (POSITION_DELETES) NOW EXISTS.
# Original US data file is UNCHANGED (same file_path as before).
# A new DATA file with the updated row was appended alongside it.

show_files("tutorial.clients_mor")

# ---------------------------------------------------------------------------
print_header("6. DELETE id=3")
# ---------------------------------------------------------------------------
# MoR writes another POSITION_DELETES file for the DE partition.
# Original DE data file is still intact.

print("SQL: DELETE FROM tutorial.clients_mor WHERE id = 3\n")
spark.sql("DELETE FROM tutorial.clients_mor WHERE id = 3")

# ---------------------------------------------------------------------------
print_header("7. Files AFTER DELETE  →  more POSITION_DELETES accumulate")
# ---------------------------------------------------------------------------

show_files("tutorial.clients_mor")

# ---------------------------------------------------------------------------
print_header("8. Final data (SELECT *)")
# ---------------------------------------------------------------------------
# Even though raw files still contain deleted/old rows, SELECT returns the
# correct logical view by applying all position-delete files at read time.

spark.sql("SELECT * FROM tutorial.clients_mor ORDER BY id").show(truncate=False)

# ---------------------------------------------------------------------------
print_header("9. Snapshots")
# ---------------------------------------------------------------------------

spark.sql("""
    SELECT snapshot_id, committed_at, operation, summary
    FROM   tutorial.clients_mor.snapshots
    ORDER  BY committed_at
""").show(truncate=False)

# ---------------------------------------------------------------------------
print_header("10. REWRITE_DATA_FILES — compact delete files away  (MoR only)")
# ---------------------------------------------------------------------------
# Merges data files with their delete files into fresh data files.
# After compaction the storage layout is identical to CoW.
# Use strategy 'binpack' (default) — no sort order required.

print("Running REWRITE_DATA_FILES with binpack strategy...\n")
spark.sql("""
    CALL local.system.rewrite_data_files(
        table    => 'tutorial.clients_mor',
        strategy => 'binpack'
    )
""").show(truncate=False)

print("\nFiles after compaction:")
show_files("tutorial.clients_mor")

# ---------------------------------------------------------------------------
print_header("Summary: Merge-on-Read")
# ---------------------------------------------------------------------------
print("""
MoR — what happened under the hood:
  Step 2  INSERT → wrote 3 data files (one per partition: US, DE, FR).
  Step 4  UPDATE → US data file NOT touched.
                   POSITION_DELETES file written (marks old row as removed).
                   New data file with updated row appended.
  Step 6  DELETE → DE data file NOT touched.
                   POSITION_DELETES file written (marks id=3 as removed).
  Step 10 REWRITE → data + delete files merged into new clean data files.
                    POSITION_DELETES files removed.

Before compaction (steps 5–9):
  • Files table shows content=0 (DATA) AND content=1 (POSITION_DELETES).
  • Reads are more expensive — engine merges both on the fly.
  • Writes are cheap: only tiny delete files written per mutation.

After compaction (step 10):
  • Only content=0 (DATA) files remain — same as CoW.
  • Schedule periodic REWRITE_DATA_FILES to keep read performance healthy.
""")

print_header("✅ Done.")
spark.stop()
