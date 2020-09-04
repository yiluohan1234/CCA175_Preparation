## NO.72 CORRECT TEXT

Problem Scenario 19 : You have been given following mysql database details as well as other info.

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

 Now accomplish following activities.

1. Import departments table from mysql to hdfs as textfile in departments_text directory.
2. Import departments table from mysql to hdfs as sequncefile in departments_sequence directory.
3. Import departments table from mysql to hdfs as avro file in departments_avro directory.
4. Import departments table from mysql to hdfs as parquet file in departments_parquet directory.

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_text --as-textfile -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_sequence --as-sequencefile -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_avro --as-avrodatafile -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_text --as-parquetfile -m 1
```

注：存储文件的不同