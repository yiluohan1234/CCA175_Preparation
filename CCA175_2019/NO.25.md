## NO.25 CORRECT TEXT

Problem Scenario 17 : You have been given following mysql database details as well as other info.

```
user=retail_dba 
password=cloudera 
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table department_hive01 --hive-import --input-null-string "" 
```

Please accomplish below assignment.

1. Create a table in hive as below, create table departments_hive01(department_id int,
   department_name string, avg_salary int);

2. Create another table in mysql using below statement CREATE TABLE IF NOT EXISTS
   departments_hive01(id int, department_name varchar(45), avg_salary int);

3. Copy all the data from departments table to departments_hive01 using insert into departments_hive01 select a.*, null from departments a;
   Also insert following records as below

   ```
   insert into departments_hive01 values(777, "Not known",1000);
   insert into departments_hive01 values(8888, null,1000);
   insert into departments_hive01 values(666, null,1100);
   ```

4. Now import data from mysql table departments_hive01 to this hive table. Please make sure that data should be visible using below hive command. Also, while importing if null value found for department_name column replace it with "" (empty string) and for id column with -999 select * from departments_hive;

**Answer:**

```
#hive
create table departments_hive01(department_id int, department_name string, avg_salary int);
#mysql
create table if not exists departments_hive01(id int, department_name varchar(45), avg_salary int);
insert into departments_hive01 select a.*, null from departments a;
insert into departments_hive01 values(777, "Not known", 1000);
insert into departments_hive01 values(8888, null, 1000);
insert into departments_hive01 values(666, null, 1100);
#sqoop
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments_hive01 --hive-home /user/hive/warehouse --hive-import --hive-table departments_hive01 --fields-terminated-by '\001' --null-string "" --null-non-string "-999"  --split-by "id" -m 1
```

注：

假设有一张表test，sqoop命令中--split-by 'id'，-m 10，会发生怎样奇特的事情。首先呢，sqoop会去查表的元数据等等，重点说一下sqoop是如何根据--split-by进行分区的。首先sqoop会向关系型数据库比如mysql发送一个命令:select max(id),min(id) from test。然后会把max、min之间的区间平均分为10分，最后10个并行的map去找数据库，导数据就正式开始啦！66666~

注意点：1.--split-by对非数字类型的字段支持不好。一般用于主键及数字类型的字段