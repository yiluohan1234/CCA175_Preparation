## NO.80 CORRECT TEXT 

Problem Scenario 1:

You have been given MySQL DB with following details.

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

 Please accomplish following activities.
1 . Connect MySQL DB and check the content of the tables.
2 . Copy "retaildb.categories" table to hdfs, without specifying directory name.
3 . Copy "retaildb.categories" table to hdfs, in a directory name "categories_target".
4 . Copy "retaildb.categories" table to hdfs, in a warehouse directory name "categories_warehouse".
**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories -m 1
#hdfs dfs -ls categories
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --target-dir categories_target -m 1
#hdfs dfs -ls categories_target
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --warehouse-dir categories_warehouse -m 1
```

## 