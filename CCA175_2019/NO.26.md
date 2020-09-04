## NO.26 CORRECT TEXT

Problem Scenario 14 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. Create a csv file named updated_departments.csv with the following contents in local file system.
   updated_departments.csv

   ```
   2,fitness
   3,footwear
   12,fathematics
   13,fcience
   14,engineering
   1000,management
   ```

2. Upload this csv file to hdfs filesystem,

3. Now export this data from hdfs to mysql retaildb.departments table. During upload make sure
   existing department will just updated and new departments needs to be inserted.

4. Now update updated_departments.csv file with below content.

   ```
   2,Fitness
   3,Footwear
   12,Fathematics
   13,Science
   14,Engineering
   1000,Management
   2000,Quality Check
   ```

5. Now upload this file to hdfs.

6. Now export this data from hdfs to mysql retail_db.departments table. During upload make sure existing department will just updated and no new departments needs to be inserted.

**Answer:**

```
vi updated_departments.csv
sqoop export --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --export-dir /user/cuiyufei/cca/updated_departments.csv --batch -m 1 --update-key 'department_id' --update-mode allowinsert

sqoop export --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --export-dir /user/cuiyufei/cca/updated_departments01.csv --batch -m 1 --update-key 'department_id' --update-mode updateonly
```

```
allowinsert
updateonly
```

注：

官方给出的查询文档进行查询或者`sqoop help`进行查询