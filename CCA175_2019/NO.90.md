## NO.90 CORRECT TEXT

Problem Scenario 7 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Import department tables using your custom boundary query, which import departments between 1 to 25.
2. Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
3. Also make sure you have imported only two columns from table, which are department_id,department_name

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --boundary-query "select 1, 25 from departments" --target-dir /user/cloudera/departments --columns department_id,department_name -m 2
```

