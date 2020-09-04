## NO.39 CORRECT TEXT

Problem Scenario 80 : You have been given MySQL DB with following details.

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.products
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Columns of products table : (product_id | product_category_id | product_name |
product_description | product_price | product_image ) Please accomplish following activities.

1. Copy "retaildb.products" table to hdfs in a directory p93_products
2. Now sort the products data sorted by `product_price` per category, use `product_category_id` colunm to group by category

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table products --target-dir p93_products -m 1

```

