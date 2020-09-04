## NO.21 CORRECT TEXT

Problem Scenario 10 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Create a database named hadoopexam and then create a table named departments in it, with following fields. department_id int, department_name string e.g. location should be hdfs://quickstart.cloudera:8020/user/hive/warehouse/hadoopexam.db/departments
2. Please import data in existing table created above from retaidb.departments into hive table
   hadoopexam.departments.
3. Please import data in a non-existing table, means while importing create hive table named hadoopexam.departments_new

**Answer:**

```
#hive
create database hadoopexam;
use hadoopexam;
create table departments(department_id int, department_name string);
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --hive-overwrite --hive-home /user/hive/warehouse --hive-import --hive-table hadoopexam.departments
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --hive-overwrite --hive-home /user/hive/warehouse --hive-import --hive-table hadoopexam.departments_new --create-hive-table
```

可以使用命令行命令查询具体的使用参数

```
sqoop help import
```

