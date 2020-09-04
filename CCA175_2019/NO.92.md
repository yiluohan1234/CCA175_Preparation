## NO.92 CORRECT TEXT

Problem Scenario 77 : You have been given MySQL DB with following details. 

```
user=retail_dba
password=cloudera database=retail_db table=retail_db.orders table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Columns of order table : (orderid , order_date , order_customer_id, order_status) Columns of ordeMtems table : (order_item_id , order_item_order_ld , order_item_product_id, order_item_quantity,order_item_subtotal,order_ item_product_price)
Please accomplish following activities.

1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory
   p92_orders and p92_order_items 
2. Join these data using orderid in Spark and Python
3. Calculate total revenue per day and per order
4. Calculate total and average revenue for each date. - combineByKey -aggregateByKey

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table orders --target-dir p92_orders -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table order_items --target-dir p92_order_items -m 1
#pyspark
orders = sc.textFile("p92_orders")
orderItems = sc.textFile("p92_order_items")
ordersKeyValue = orders.map(lambda x:(x.split(",")[0],x))
orderItemsKeyValue = orderItems.map(lambda x:(x.split(",")[1],x))
joinedData = orderItemsKeyValue.join(ordersKeyValue)
//Retruned row will contain ((order_date,order_id),amout_collected)
revenuePerDayPerOrder = joinedData.map(lambda row: ((row[1][1].split(",")[1],row[0]),float(row[1][0].split(",")[4])))
totalRevenuePerDayPerOrder = revenuePerDayPerOrder.reduceByKey(lambda runningSum, value: runningSum + value).sortByKey()

//(date, amount_collected)
dateAndRevenueTuple = totalRevenuePerDayPerOrder.map(lambda line: (line[0][0], line[1]))
//(Date, Total Revenue for date, total_number_of_dates)
totalRevenueAndTotalCount = dateAndRevenueTuple.combineByKey(lambda revenue: (revenue, 1), lambda revenueSumTuple, amount: (revenueSumTuple[0] + amount, revenueSumTuple[1] + 1), lambda tuple1, tuple2: (round(tuple1[0] + tuple2[0], 2), tuple1[1] + tuple2[1]))
averageRevenuePerDate = totalRevenueAndTotalCount.map(lambda threeElements:(threeElements[0], threeElements[1][0]/threeElements[1][1]))
totalRevenueAndTotalCount = dateAndRevenueTuple.aggregateByKey((0,0), lambda runningRevenueSumTuple, revenue: (runningRevenueSumTuple[0] + revenue, runningRevenueSumTuple[1] + 1), lambda tupleOneRevenueAndCount, tupleTwoRevenueAndCount:(tupleOneRevenueAndCount[0] + tupleTwoRevenueAndCount[0],tupleOneRevenueAndCount[1] + tupleTwoRevenueAndCount[1]))
averageRevenuePerDate = totalRevenueAndTotalCount.map(lambda threeElements: (threeElements[0], threeElements[1][0]/threeElements[1][1]))
```

