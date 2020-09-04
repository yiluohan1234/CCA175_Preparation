## NO.70 CORRECT TEXT

Problem Scenario 3: You have been given MySQL DB with following details. 

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. Import data from categories table, where category=22 (**Data should be stored in categories subset**)
2. Import data from categories table, where category>22 (Data should be stored in categories_subset_2)
3. Import data from categories table, where category between 1 and 22 (Data should be stored in categories_subset_3)
4. While importing catagories data change the delimiter to '|' (Data should be stored in
   categories_subset_S)
5. Importing data from catagories table and restrict the import to category_name,category id columns only with delimiter as '|'
6. Add null values in the table using below SQL statement ALTER TABLE categories modify category_department_id int(11); INSERT INTO categories values
   (eO.NULL.'TESTING');
7. Importing data from catagories table (In categories_subset_17 directory) using '|' delimiter and
   categoryjd between 1 and 61 and encode null values for both string and non string columns.
8. Import entire schema retail_db in a directory categories_subset_all_tables

Answer:

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --warehouse-dir categories_subset --where "category_id = 22" -m l
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --warehouse_dir categories_subset2 --where "category > 22" -m l
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --warehouse_dir categories_subset3 --where "category between 1 and 22" -m l
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --fields-terminated-by '|' --warehouse_dir categories_subset --where "category between 1 and 22" -m l
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --pasword cloudera --table=categories --warehouse-dir=categories_subset_8 --columns category_name,category_id --fields-terminated-by '|' -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --pasword cloudera --table=categories --warehouse-dir=categories_subset_17  --where "cagegory between 1 and 61" --fields-terminated-by '|' --null-string null --null-non-string null -m 1
#mysql 
alter table categories modify category_department_id int(11);
insert into categories values(NULL, 'TESTING');

sqoop import-all-tables --connect jdbc:mysql://quickstart:3306/retail_db --username=retail_dba --password=cloudera --warehouse-dir=categories_subset_all_tables
```

