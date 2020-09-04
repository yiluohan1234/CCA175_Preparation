## NO.37 CORRECT TEXT

Problem Scenario 16 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --hive-import --hive-overwrite --hive-table departments_hive -m 1
```

Please accomplish below assignment.

1. Create a table in hive as below.
   create table departments_hive(department_id int, department_name string);
2. Now import data from mysql table departments to this hive table. Please make sure that data should be visible using below hive command, select" from departments_hive

**Answer:**

```
#hive
create table departments_hive(department_id int, department_name string);
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --hive-import --hive-home /user/hive/warehouse --hive-overwrite --fields-terminated-by '\001' --hive-table departments_hive -m 1
```

注：

```
--fields-terminated-by '\001'
--hive-home /user/hive/warehouse
```

