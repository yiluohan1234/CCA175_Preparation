## NO.83 CORRECT TEXT

Problem Scenario 8 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Import joined result of orders and order_items table join on orders.order_id = order_items.order_item_order_id.
2. Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
3. Also make sure you use orderid columns for sqoop to use for boundary conditions.

**Answer:**

```
sqoop import --connect jdbc:mysql://localhost:3306/cca --username retail_dba --password cloudera --query "select * from orders join order_items on orders.order_id =order_items.order_item_order_id where $CONDITIONS" --target-dir p83_result --split-by order_id -m 2
```

注：在官网搜索conditions