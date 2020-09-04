## NO.74 CORRECT TEXT

Problem Scenario 18 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Now accomplish following activities.

1. Create mysql table as below.
   mysql --user=retail_dba -password=cloudera
     use retail_db
     CREATE TABLE IF NOT EXISTS departments_hive02(id int, department_name varchar(45), avg_salary int);
     show tables;
2. Now export data from hive table departments_hive01 in departments_hive02. While exporting, please note following. wherever there is a empty string it should be loaded as a null value in mysql. wherever there is -999 value for int field, it should be created as null value.

**Answer:**

```
#mysql
create table if not exists departments_hive02(id int, department_name varchar(45), avg_salary int);
sqoop export --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments_hive02 --input-null-string "" --input-null-non-string -999 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --export-dir /user/hive/warehouse/departments_hive01 -m 1
```

注：

-null-string含义是 string类型的字段，当Value是NULL，替换成指定的字符，该例子中为@@@

--null-non-string 含义是非string类型的字段，当Value是NULL，替换成指定字符，该例子中为###