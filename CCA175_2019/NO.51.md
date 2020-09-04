## NO.51 CORRECT TEXT

Problem Scenario 5 : You have been given following mysql database details. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. List all the tables using sqoop command from retail_db
2. Write simple sqoop eval command to check whether you have permission to read database tables or not.
3. Import all the tables as avro files in /user/hive/warehouse/retail_cca175.db
4. Import departments table as a text file in /user/cloudera/departments.

**Answer:**

sqoop import-all-tables --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --hive-import --as-avrodatafile --hive-home /user/hive/warehouse --hive-database retail_cca174.db

```
sqoop list-tables --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera

sqoop eval --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --query "select * from orders limit 10"

sqoop import-all-tables --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --as-avrodatafile --warehouse-dir /user/hive/warehouse/retail_cca175.db -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --as-textfile --target-dir /user/cloudera/departments -m 1
```

注：

--target-dir和--warehouse-dir的区别

- -target-dir将会在当前路径下生成结果目录，具有更改表名的功能表名

- --warehouse-dir后面的目录是一个父目录，下面可以存放多张表