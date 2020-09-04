## NO.85 CORRECT TEXT

Problem Scenario 12 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

 Please accomplish following.

1. Create a table in retailedb with following definition.
   CREATE table departments_new (department_id int(11), department_name varchar(45),
   created_date T1MESTAMP DEFAULT NOW());
   2 . Now insert records from departments table to departments_new
   3 . Now import data from departments_new table to hdfs.
   4 . Insert following 5 records in departmentsnew table. Insert into departments_new values(110, "Civil" , null); Insert into departments_new values(111, "Mechanical" , null);
   Insert into departments_new values(112, "Automobile" , null); Insert into departments_new values(113, "Pharma" , null);
   Insert into departments_new values(114, "Social Engineering" , null);
2. Now do the incremental import based on created_date column.

**Answer:**

```
create table departments_new(department_id int(11), department_name varchar(45), created_date TIMESTAMP default now());
insert into departments_new select *, null from departments;
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments_new --target-dir p83_target -m 1
insert into departments_new values(110, "Civil", null);
insert into departments_new values(111, "Mechanical", null);
insert into departments_new values(112, "Automobile", null);
insert into departments_new values(113, "Pharma", null);
insert into departments_new values(114, "Engineering", null);
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments_new --check-column created_date --incremental append --last-value '2019-06-12 22:20:53' --target-dir p83_target -m 1

```

注：`--check-column `、`--incremental`、`--last-value`