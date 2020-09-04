## NO.64 CORRECT TEXT

Problem Scenario 74 : You have been given MySQL DB with following details.

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Columns of order table : (orderid , order_date , ordercustomerid, order status} Columns of orderjtems table : (order_item_td , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
Please accomplish following activities.

1. Copy "retaildb.orders" and "retaildb.order_tems" table to hdfs in respective directory p89_orders and p89_order_items .
2. Join these data using orderid in Spark and Python
3. Now fetch selected columns from joined data Orderld, Order date and amount collected on this order.
4. Calculate total order placed for each date, and produced the output sorted by date.

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table orders --target-dir p89_orders
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table order_items --target-dir p89_order_items
order = sc.textFile("p89_orders").map(lambda x:(int(x.split(",")[0]), x))
order_items = sc.textFile("p89_order_items").map(lambda x:(int(x.split(",")[1]), x))

joinedRDD = order_items.join(order)
#(32768, (u'81958,32768,1073,1,199.99,199.99', u'32768,2014-02-12 00:00:00.0,1900,PENDING_PAYMENT'))
distinctOrdersDate = joinedRDD.map(lambda line:line[1][1].split(",")[1] + "," + line[1][1].split(",")[0]).distinct()
newLineTuple = distinctOrdersDate.map(lambda line: (line.split(",")[0], 1))
totalOrdersPerDate = newLineTuple.reduceByKey(lambda x,y:x+y)

```

