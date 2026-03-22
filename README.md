# 🧊 Operation on Apache Iceberg table

### **📦 DDL – table structure**

* [CREATE TABLE](src/ddl_table_structure/pyspark_iceberg_1_create_table.py)
* [DROP TABLE](src/ddl_table_structure/pyspark_iceberg_2_drop_table.py)
* [ALTER TABLE](src/ddl_table_structure/pyspark_iceberg_3_alter_table.py)
* [RENAME TABLE](src/ddl_table_structure/pyspark_iceberg_4_rename_table.py)
* [ALTER_PARTITION](src/ddl_table_structure/pyspark_iceberg_5_alter_partition.py)[RENAME TABLE](src/ddl_table_structure/pyspark_iceberg_4_rename_table.py)

### **✏️ Query**

* [SELECT](src/query/pyspark_iceberg_6_join.py)
* [WHERE](src/query/pyspark_iceberg_2_where.py)
* [GROUP BY](src/query/pyspark_iceberg_3_group_by.py)
* [ORDER BY](src/query/pyspark_iceberg_4_order_by.py)
* [JOIN](src/query/pyspark_iceberg_6_join.py)

### **✏️ DML – data operation**

* [INSERT INTO](src/dml_data_operation/pyspark_iceberg_1_insert_into.py)
* [INSERT OVERWRITE](src/dml_data_operation/pyspark_iceberg_2_insert_overwrite.py)
* [UPDATE](src/dml_data_operation/pyspark_iceberg_3_update.py)
* [DELETE](src/dml_data_operation/pyspark_iceberg_4_delete.py)
* [MERGE INTO](src/dml_data_operation/pyspark_iceberg_5_merge_into.py)

### **🕒 Snapshots and Time Travel**

* [SELECT WITH VERSION](src/snapshots_and_time_travel/pyspark_iceberg_2_select_with_version.py)
* [SELECT WITH TIMESTAMP](src/snapshots_and_time_travel/pyspark_iceberg_1_select_with_timestamp.py)
* [ROLLBACK TO SNAPSHOT](src/snapshots_and_time_travel/pyspark_iceberg_3_rollback_to_snapshot.py)
* [CREATE TAG](src/snapshots_and_time_travel/pyspark_iceberg_4_create_tag.py)
* [CREATE BRANCH](src/snapshots_and_time_travel/pyspark_iceberg_5_create_branch.py)
* [SET_CURRENT_SNAPSHOT](src/snapshots_and_time_travel/pyspark_iceberg_6_set_current_snapshot.py)

### **🔧 Maintenance**

* [REWRITE_DATA_FILES](src/maintenance/pyspark_iceberg_1_rewrite_data_files.py)
* [REWRITE_MANIFESTS](src/maintenance/pyspark_iceberg_3_rewrite_manifests.py)
* [EXPIRE_SNAPSHOTS](src/maintenance/pyspark_iceberg_4_expire_snapshots.py)
* [REMOVE_ORPHAN_FILES](src/maintenance/pyspark_iceberg_5_remove_orphan_files.py)
* [REWRITE_POSITION_DELETE_FILES](src/maintenance/pyspark_iceberg_2_rewrite_position_delete_files.py)
* [ADD_FILES](src/maintenance/pyspark_iceberg_6_add_files.py)

### **🔎 Metadata**

* [SNAPSHOTS](src/metadata/pyspark_iceberg_1_snapshots.py)
* [HISTORY](src/metadata/pyspark_iceberg_2_history.py)
* [FILES](src/metadata/pyspark_iceberg_3_files.py)
* [MANIFESTS](src/metadata/pyspark_iceberg_4_manifests.py)
* [PARTITIONS](src/metadata/pyspark_iceberg_5_partitions.py)
* [REGISTER_TABLE](src/metadata/pyspark_iceberg_6_register_table.py)

### more:

# 🧩 Project Setup & Usage Guide

Setup Environment

Run the initialization script:

```bash
sh initScript.sh
```

Run Scripts
Step 1: Navigate to the scripts directory

```bash
cd scripts
```

Step 2: Execute one of the available Python scripts

For example:

```bash
python3 scripts/query/pyspark_iceberg_1_select.py
```

or

```bash
python3 scripts/query/pyspark_iceberg_1_select.py
```

---

**Learn and have fun! 🎉**

