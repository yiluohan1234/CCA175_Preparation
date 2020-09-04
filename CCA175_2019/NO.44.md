## NO.44 CORRECT TEXT

Problem Scenario 11 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Import departments table in a directory called departments.
2. Once import is done, please insert following 5 records in departments mysql table.
   Insert into departments(10, physics); Insert into departments(11, Chemistry); Insert into departments(12, Maths); Insert into departments(13, Science); Insert into departments(14, Engineering);
3. Now import only new inserted records and append to existring directory . which has been created in first step.

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments -target-dir /user/cloudera/departments -m 1
#mysql
Insert into departments values(10, 'physics');
Insert into departments values(11, 'Chemistry');
Insert into departments values(12, 'Maths');
Insert into departments values(13, 'Science');
Insert into departments values(14, 'Engineering');
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments -target-dir /user/cloudera/departments --check-column "department_id" --incremental append --last-value  1000 -m 1
```

