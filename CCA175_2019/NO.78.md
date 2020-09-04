## NO.78 CORRECT TEXT

Problem Scenario 75 : You have been given MySQL DB with following details.

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. Copy "retail_db.order_items" table to hdfs in respective directory p90_order_items .
2. Do the summation of entire revenue in this table using pyspark.
3. Find the maximum and minimum revenue as well.
4. Calculate average revenue
   Columns of ordeMtems table : (order_item_id , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_ item_subtotal,order_item_product_price)

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table order_items --target-dir p90_order_items -m 1
#pyspark
data = sc.textFile("p90_order_items")
data.first()
revenue = data.map(lambda line:float(line.split(",")[4]))
totalRevenue = revenue.reduce(lambda x,y: x + y)
maxRevenue = revenue.reduce(lambda a,b:(a if a>=b else b))
minRevenue = revenue.reduce(lambda a,b:(a if a<=b else b))
count=revenue.count()
averageRev=totalRevenue/count
```

