## NO.66 CORRECT TEXT

Problem Scenario 9 : You have been given following mysql database details as well as other info.

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Import departments table in a directory.
2. Again import departments table same directory (However, directory already exist hence it should not overrride and append the results)
3. Also make sure your results fields are terminated by '|' and lines terminated by '\n\

Answer:

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir /user/cuiyufei/cca/spark9_result --fields-terminated-by '|' --lines-terminated-by '\n' -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir /user/cuiyufei/cca/spark9_result --fields-terminated-by '|' --lines-terminated-by '\n' --append -m 1
```

