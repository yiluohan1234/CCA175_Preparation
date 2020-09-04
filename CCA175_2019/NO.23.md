## NO.23 CORRECT TEXT

Problem Scenario 6 : You have been given following mysql database details as well as other info.

```
user=retail_dba 
password=cloudera 
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
sqoop import-all-tables --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera -m 3 --compress --compression-codecorg.apache.hadoop.io.compress.SnappyCodec --hive-import --hive-overwrite --create-hive-table --outdir java_output 
```

Compression Codec : org.apache.hadoop.io.compress.SnappyCodec Please accomplish following.

1. Import entire database such that it can be used as a hive tables, it must be created in default schema.
2. Also make sure each tables file is partitioned in 3 files e.g. part-00000, part-00002, part-
   00003
3. Store all the Java files in a directory called java_output to evalute the further

**Answer:**

```
sqoop import-all-tables --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera -m 3 --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-import  --hive-overwrite --create-hive-table --outdir java_output
```

it can be used as a hive tables, it must be created in default schema.这个就要求

```
--hive-import --hive-overwrite --create-hive-table
```

进行压缩前首先要有压缩

```
--compress
```

