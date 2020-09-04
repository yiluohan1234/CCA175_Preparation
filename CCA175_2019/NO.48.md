## NO.48 CORRECT TEXT

Problem Scenario 20 : You have been given MySQL DB with following details.

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. Write a Sqoop Job which will import "retaildb.categories" table to hdfs, in a directory name "categories_targetJob".

**Answer:**

```
sqoop job --create sqoopjob -- import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --target-dir /user/cuiyufei/cca/categories_targetJob --fields-terminated-by '|' --lines-terminated-by '\n' -m 1
sqoop job --list
sqoop job --show sqoopjob
sqoop job --delete sqoopjob
sqoop job --exec sqoopjob(需要输入密码，mysql密码)
```

sqoop job --**create sqoopjob** -- import --connect 

