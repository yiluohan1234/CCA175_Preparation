## NO.63 CORRECT TEXT

Problem Scenario 78 : You have been given MySQL DB with following details.

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Columns of order table : (orderid , order_date , order_customer_id, order_status) Columns of ordeMtems table : (order_item_td , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
Please accomplish following activities.

1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory
   p92_orders and p92_order_items .
2. Join these data using order_id in Spark and Python
3. Calculate total revenue per day and per customer
4. Calculate maximum revenue customer

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table orders --target-dir p92_orders
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table order_items --target-dir p92_order_items

order = sc.textFile("p92_orders").map(lambda x:(int(x.split(",")[0]), x))
order_items = sc.textFile("p92_order_items").map(lambda x:(int(x.split(",")[1]), x))

joinedRDD = order_items.join(order)

ordersPerDatePerCustomer = joinedRDD.map(lambda line:((line[1][1].split(",")[1], line[1][1].split(",")[2]), float(line[1][0].split(",")[4])))
amountCollectedPerDayPerCustomer = ordersPerDatePerCustomer.reduceByKey(lambda runningSum, amount: runningSum + amount)
revenuePerDatePerCustomerRDD = amountCollectedPerDayPerCustomer.map(lambda threeElementTuple: (threeElementTuple[0][0], (threeElementTuple[0][1],threeElementTuple[1])))

perDateMaxAmountCollectedByCustomer = revenuePerDatePerCustomerRDD.reduceByKey(lambda runningAmountTuple, newAmountTuple: (runningAmountTuple if runningAmountTuple[1] >= newAmountTuple[1] else newAmountTuple))
```

注：要读懂题意