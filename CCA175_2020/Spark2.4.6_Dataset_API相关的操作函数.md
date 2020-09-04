# Spark2.4.6 Dataset API相关的操作函数

Spark SQL中的DataFrame类似于一张关系型数据表。在关系型数据库中对单表或进行的查询操作，在DataFrame中都可以通过调用其API接口来实现。可以参考，Scala提供的[DataSet API](http://spark.apache.org/docs/2.4.6/api/scala/index.html#org.apache.spark.sql.Dataset)。

　　本文中的代码基于Spark-2.4.6的文档实现。

# 一、DataFrame对象的生成

　　Spark-SQL可以以其他RDD对象、parquet文件、json文件、hive表，以及通过JDBC连接到其他关系型数据库作为数据源来生成DataFrame对象。本文将以MySQL数据库为数据源，生成DataFrame对象后进行相关的DataFame之上的操作。
　　文中生成DataFrame的代码如下：

```
val orders = spark.read.format( "jdbc" ).options(
      Map( "url" -> "jdbc:mysql://localhost:3306",
        "user" -> "root",
        "password" -> "199037",
        "dbtable" -> "retail_db.orders" )).load()
val order_items = spark.read.format( "jdbc" ).options(
      Map("url" -> "jdbc:mysql://localhost:3306" ,
        "user" -> "root",
        "password" -> "199037",
        "dbtable" -> "retail_db.order_items" )).load()
```

# 二、DataFrame对象上Action操作

## 1、`show`：展示数据

　　以表格的形式在输出中展示`jdbcDF`中的数据，类似于`select * from orders`的功能。
　　`show`方法有四种调用方式，分别为，
**（1）show**
　　只显示前20条记录。
　　示例：

```
orders.show
```

结果略。

**（2）show(numRows: Int)**
　　显示`numRows`条
　　示例：

```
orders.show(3)
```

结果:

```
scala> orders.show(3)
+--------+-------------------+-----------------+---------------+
|order_id|         order_date|order_customer_id|   order_status|
+--------+-------------------+-----------------+---------------+
|       1|2013-07-25 00:00:00|            11599|         CLOSED|
|       2|2013-07-25 00:00:00|              256|PENDING_PAYMENT|
|       3|2013-07-25 00:00:00|            12111|       COMPLETE|
+--------+-------------------+-----------------+---------------+
only showing top 3 rows
```

**（3）show(truncate: Boolean)**
　　是否最多只显示20个字符，默认为`true`。
　　示例：

```
orders.show(true)
```

结果略。

**（4）show(numRows: Int, truncate: Boolean)**
　　综合前面的显示记录条数，以及对过长字符串的显示格式。
　　示例：

```
orders.show(3, false)
```

结果：

```
scala> orders.show(3, false)
+--------+-------------------+-----------------+---------------+
|order_id|order_date         |order_customer_id|order_status   |
+--------+-------------------+-----------------+---------------+
|1       |2013-07-25 00:00:00|11599            |CLOSED         |
|2       |2013-07-25 00:00:00|256              |PENDING_PAYMENT|
|3       |2013-07-25 00:00:00|12111            |COMPLETE       |
+--------+-------------------+-----------------+---------------+
only showing top 3 rows
```

## 2、`collect`：获取所有数据到数组

　　不同于前面的`show`方法，这里的`collect`方法会将`orders`中的所有数据都获取到，并返回一个`Array`对象。

```
orders.collect()
```

　　结果如下，结果数组包含了`orders`的每一条记录，每一条记录由一个`GenericRowWithSchema`对象来表示，可以存储字段名及字段值。

```
scala>   orders.collect()
res4: Array[org.apache.spark.sql.Row] = Array([1,2013-07-25 00:00:00.0,11599,CLOSED], [2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT], [3,2013-07-25 00:00:00.0,12111,COMPLETE], [4,2013-07-25 00:00:00.0,8827,CLOSED], [5,2013-07-25 00:00:00.0,11318,COMPLETE], [6,2013-07-25 00:00:00.0,7130,COMPLETE], [7,2013-07-25 00:00:00.0,4530,COMPLETE], [8,2013-07-25 00:00:00.0,2911,PROCESSING], [9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT], [10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT], [11,2013-07-25 00:00:00.0,918,PAYMENT_REVIEW], [12,2013-07-25 00:00:00.0,1837,CLOSED], [13,2013-07-25 00:00:00.0,9149,PENDING_PAYMENT], [14,2013-07-25 00:00:00.0,9842,PROCESSING], [15,2013-07-25 00:00:00.0,2568,COMPLETE], [16,2013-07-25 00:00:00.0,7276,PENDING_PAYMENT], [17,2013-07-25 00:00:00.0,2667,COMPLETE], [18,20...
```

## 3、`collectAsList`：获取所有数据到List

　　功能和`collect`类似，只不过将返回结构变成了`List`对象，使用方法如下

```
orders.collectAsList()
```

结果如下:

```
res5: java.util.List[org.apache.spark.sql.Row] = [[1,2013-07-25 00:00:00.0,11599,CLOSED], [2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT], [3,2013-07-25 00:00:00.0,12111,COMPLETE], [4,2013-07-25 00:00:00.0,8827,CLOSED], [5,2013-07-25 00:00:00.0,11318,COMPLETE], [6,2013-07-25 00:00:00.0,7130,COMPLETE], [7,2013-07-25 00:00:00.0,4530,COMPLETE], [8,2013-07-25 00:00:00.0,2911,PROCESSING], [9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT], [10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT], [11,2013-07-25 00:00:00.0,918,PAYMENT_REVIEW], [12,2013-07-25 00:00:00.0,1837,CLOSED], [13,2013-07-25 00:00:00.0,9149,PENDING_PAYMENT], [14,2013-07-25 00:00:00.0,9842,PROCESSING], [15,2013-07-25 00:00:00.0,2568,COMPLETE], [16,2013-07-25 00:00:00.0,7276,PENDING_PAYMENT], [17,2013-07-25 00:00:00.0,2667,COMPLETE], [1...
```

## 4、`describe(cols: String*)`：获取指定字段的统计信息

　　这个方法可以动态的传入一个或多个`String`类型的字段名，结果仍然为`DataFrame`对象，用于统计数值类型字段的统计值，比如`count, mean, stddev, min, max`等。
　　使用方法如下，其中`order_id`字段为整型，`order_date`字段为字符类型

```
orders.describe("order_id", "").show()
```

　　结果如下:

```
+-------+------------------+
|summary|          order_id|
+-------+------------------+
|  count|             68883|
|   mean|           34442.0|
| stddev|19884.953633337947|
|    min|                 1|
|    max|             68883|
+-------+------------------+
```

## 5、`first, head, take, takeAsList`：获取若干行记录

　　这里列出的四个方法比较类似，其中
　　（1）`first`获取第一行记录
　　（2）`head`获取第一行记录，`head(n: Int)`获取前n行记录
　　（3）`take(n: Int)`获取前n行数据
　　（4）`takeAsList(n: Int)`获取前n行数据，并以`List`的形式展现
　　以`Row`或者`Array[Row]`的形式返回一行或多行数据。`first`和`head`功能相同。
　　`take`和`takeAsList`方法会将获得到的数据返回到Driver端，所以，使用这两个方法时需要注意数据量，以免Driver发生`OutOfMemoryError`

　　使用和结果略。

# 二、DataFrame对象上的条件查询和join等操作

　　以下返回为DataFrame类型的方法，可以连续调用。

## 1、where条件相关

**（1）where(conditionExpr: String)：SQL语言中where关键字后的条件**
　　传入筛选条件表达式，可以用`and`和`or`。得到DataFrame类型的返回结果，
　　示例：

```
orders.where("order_status ='CLOSED' or order_id = '1'").show(3)
```

　　结果:

```
+--------+-------------------+-----------------+------------+
|order_id|         order_date|order_customer_id|order_status|
+--------+-------------------+-----------------+------------+
|       1|2013-07-25 00:00:00|            11599|      CLOSED|
|       4|2013-07-25 00:00:00|             8827|      CLOSED|
|      12|2013-07-25 00:00:00|             1837|      CLOSED|
+--------+-------------------+-----------------+------------+
only showing top 3 rows
```

**（2）filter：根据字段进行筛选**
　　传入筛选条件表达式，得到DataFrame类型的返回结果。和`where`使用条件相同
　　示例：

```
orders.filter("order_status ='CLOSED' or order_id = '1'").show(3)
```

　　结果略。

## 2、查询指定字段

**（1）select：获取指定字段值**
　　根据传入的`String`类型字段名，获取指定字段的值，以DataFrame类型返回
　　示例：

```
orders.select( "order_id" , "order_status" ).show(3)
```

　　结果：

```
+--------+---------------+
|order_id|   order_status|
+--------+---------------+
|       1|         CLOSED|
|       2|PENDING_PAYMENT|
|       3|       COMPLETE|
+--------+---------------+
only showing top 3 rows
```

还有一个重载的`select`方法，不是传入`String`类型参数，而是传入`Column`类型参数。可以实现`select order_id, order_id+1 from orders`这种逻辑。

```
orders.select(orders( "order_id" ), orders( "order_id") + 1 ).show(3)
orders.select($"order_id", $"order_id" + 1).show(3)
```

　　结果：

```
+--------+--------------+
|order_id|(order_id + 1)|
+--------+--------------+
|       1|             2|
|       2|             3|
|       3|             4|
+--------+--------------+
only showing top 3 rows
```

能得到`Column`类型的方法是`apply`以及`col`方法，一般用`apply`方法更简便。

**（2）selectExpr：可以对指定字段进行特殊处理**
　　可以直接对指定字段调用UDF函数，或者指定别名等。传入`String`类型参数，得到DataFrame对象。
　　示例，查询order_id`字段，order_date`字段取别名`time`：

```
orders .selectExpr("order_id" , "order_date as time").show(3)
```

　　结果:

```
+--------+-------------------+
|order_id|               time|
+--------+-------------------+
|       1|2013-07-25 00:00:00|
|       2|2013-07-25 00:00:00|
|       3|2013-07-25 00:00:00|
+--------+-------------------+
only showing top 3 rows
```



**（3）col：获取指定字段**
　　只能获取一个字段，返回对象为Column类型。
　　```val order_col = orders.col("order_id")```

​		结果略。

**（4）apply：获取指定字段**
　　只能获取一个字段，返回对象为Column类型
　　示例：

```
val idCol1 = orders.apply("order_id")
val idCol2 = orders("order_id")
```

　　结果略。

**（5）drop：去除指定字段，保留其他字段**
　　返回一个新的DataFrame对象，其中不包含去除的字段，一次只能去除一个字段。
　　示例：

```
orders.drop("order_id")
orders.drop(orders("order_id"))
```

　　结果略。

## 3、limit

　　`limit`方法获取指定DataFrame的前n行记录，得到一个新的DataFrame对象。和`take`与`head`不同的是，`limit`方法不是Action操作。

```
orders.limit(3).show(3)
```

　　结果:

```
+--------+-------------------+-----------------+---------------+                
|order_id|         order_date|order_customer_id|   order_status|
+--------+-------------------+-----------------+---------------+
|       1|2013-07-25 00:00:00|            11599|         CLOSED|
|       2|2013-07-25 00:00:00|              256|PENDING_PAYMENT|
|       3|2013-07-25 00:00:00|            12111|       COMPLETE|
+--------+-------------------+-----------------+---------------+
```

## 4、order by

（1）`orderBy`和`sort`：按指定字段排序，默认为升序
　　示例1，按指定字段排序。加个`-`表示降序排序。`sort`和`orderBy`使用方法相同

```
order_items.orderBy(- order_items("order_item_subtotal")).show(3)
// 或者
order_items.orderBy(desc("order_item_subtotal")).show(3)
```

　　结果:

```
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|       171806|              68722|                  208|                  1|            1999.99|                 1999.99|
|       171837|              68736|                  208|                  1|            1999.99|                 1999.99|
|       171765|              68703|                  208|                  1|            1999.99|                 1999.99|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
```

**（2）sortWithinPartitions**
　　和上面的`sort`方法功能类似，区别在于`sortWithinPartitions`方法返回的是按Partition排好序的DataFrame对象。

## 5、group by

（1）`groupBy`：根据字段进行`group by`操作
　　`groupBy`方法有两种调用方式，可以传入`String`类型的字段名，也可传入`Column`类型的对象。
　　使用方法如下，

```
order_items.groupBy("order_item_product_id")
order_items.groupBy( order_items( "order_item_product_id"))
```

**（2）cube和rollup：group by的扩展**
　　功能类似于`SQL`中的`group by cube/rollup`，略。

**（3）GroupedData对象**
　　该方法得到的是`GroupedData`类型对象，在`GroupedData`的API中提供了`group by`之后的操作，比如，

- `max(colNames: String*)`方法，获取分组中指定字段或者所有的数字类型字段的最大值，只能作用于数字型字段
- `min(colNames: String*)`方法，获取分组中指定字段或者所有的数字类型字段的最小值，只能作用于数字型字段
- `mean(colNames: String*)`方法，获取分组中指定字段或者所有的数字类型字段的平均值，只能作用于数字型字段
- `sum(colNames: String*)`方法，获取分组中指定字段或者所有的数字类型字段的和值，只能作用于数字型字段
- `count()`方法，获取分组中的元素个数

- 这里面比较复杂的是以下两个方法，`agg`，该方法和下面介绍的类似，可以用于对指定字段进行聚合操作。

## 6、distinct

**（1）distinct：返回一个不包含重复记录的DataFrame**
　　返回当前DataFrame中不重复的Row记录。该方法和接下来的`dropDuplicates()`方法不传入指定字段时的结果相同。
　　示例：

```
orders.distinct()
```

　　结果略。

**（2）dropDuplicates：根据指定字段去重**
　　根据指定字段去重。类似于`select distinct a, b`操作
　　示例：

```
orders.dropDuplicates(Seq("order_id"))
```

　结果略。

## 7、聚合

　　聚合操作调用的是`agg`方法，该方法有多种调用方式。一般与`groupBy`方法配合使用。
　　以下示例其中最简单直观的一种用法，对`id`字段求最大值，对`c4`字段求和。

```
order_items.agg("order_item_product_price" -> "max", "order_item_subtotal" -> "sum")
```

　　结果：

```
+-----------------------------+------------------------+
|max(order_item_product_price)|sum(order_item_subtotal)|
+-----------------------------+------------------------+
|                      1999.99|      3.43226199299836E7|
+-----------------------------+------------------------+
```

## 8、union

　　`unionAll`方法：对两个DataFrame进行组合
　　类似于`SQL`中的`UNION ALL`操作。
　　示例：

```
orders.unionALL(orders.limit(1))1
```

　　结果略。

## 9、join

　　重点来了。在`SQL`语言中用得很多的就是`join`操作，DataFrame中同样也提供了`join`的功能。
　　接下来隆重介绍`join`方法。在DataFrame中提供了六个重载的`join`方法。
**（1）、笛卡尔积**

```
orders.join(order_items)
```

**（2）、using一个字段形式**
　　下面这种join类似于`a join b using column1`的形式，需要两个DataFrame中有相同的一个列名，

```
joinDF1.join(joinDF2, "id")
```

　　`joinDF1`和`joinDF2`根据字段`id`进行`join`操作，结果如下，`using`字段只显示一次。

**（3）、using多个字段形式**
　　除了上面这种`using`一个字段的情况外，还可以`using`多个字段，如下

```
joinDF1.join(joinDF2, Seq("id", "name")）
```

**（4）、指定join类型**
　　两个DataFrame的`join`操作有`inner, outer, left_outer, right_outer, leftsemi`类型。在上面的`using`多个字段的join情况下，可以写第三个`String`类型参数，指定`join`的类型，如下所示

```
joinDF1.join(joinDF2, Seq("id", "name"), "inner"）
```

**（5）、使用Column类型来join**
　　如果不用`using`模式，灵活指定`join`字段的话，可以使用如下形式

```
orders.join(order_items, orders("order_id") === order_items("order_item_order_id"))
```

**（6）、在指定join字段同时指定join类型**
　　如下所示

```
scala> orders.join(order_items, orders("order_id") === order_items("order_item_order_id"), "inner").select("order_id", "order_item_order_id").show(3)
+--------+-------------------+
|order_id|order_item_order_id|
+--------+-------------------+
|     148|                148|
|     148|                148|
|     148|                148|
+--------+-------------------+
only showing top 3 rows
```

## 10、获取指定字段统计信息

　　`stat`方法可以用于计算指定字段或指定字段之间的统计信息，比如方差，协方差等。这个方法返回一个`DataFramesStatFunctions`类型对象。
　　下面代码演示根据`c4`字段，统计该字段值出现频率在`30%`以上的内容。在`jdbcDF`中字段`c1`的内容为`"a, b, a, c, d, b"`。其中`a`和`b`出现的频率为`2 / 6`，大于`0.3`

```
jdbcDF.stat.freqItems(Seq ("c1") , 0.3).show()
```

　　结果如下：

## 11、获取两个DataFrame中共有的记录

　　`intersect`方法可以计算出两个DataFrame中相同的记录。

```
scala>   orders.intersect(orders.limit(1)).show(3)
+--------+-------------------+-----------------+------------+                   
|order_id|         order_date|order_customer_id|order_status|
+--------+-------------------+-----------------+------------+
|       1|2013-07-25 00:00:00|            11599|      CLOSED|
+--------+-------------------+-----------------+------------+
```

## 12、获取一个DataFrame中有另一个DataFrame中没有的记录

　　示例：

```
jorders.except(orders.limit(1)).show(3)
```

## 13、操作字段名

**（1）withColumnRenamed：重命名DataFrame中的指定字段名**
　　如果指定的字段名不存在，不进行任何操作。下面示例中将`orders`中的`order_id`字段重命名为`id`。

```scala
scala>  orders.withColumnRenamed("order_id", "id").show(3)
+---+-------------------+-----------------+---------------+
| id|         order_date|order_customer_id|   order_status|
+---+-------------------+-----------------+---------------+
|  1|2013-07-25 00:00:00|            11599|         CLOSED|
|  2|2013-07-25 00:00:00|              256|PENDING_PAYMENT|
|  3|2013-07-25 00:00:00|            12111|       COMPLETE|
+---+-------------------+-----------------+---------------+
only showing top 3 rows
```

**（2）withColumn：往当前DataFrame中新增一列**
　　`whtiColumn(colName: String , col: Column)`方法根据指定`colName`往DataFrame中新增一列，如果`colName`已存在，则会覆盖当前列。

​		增加列的几种方式：

```
scala> orders.withColumn("test_lit", lit(12)).show(3)
+--------+-------------------+-----------------+---------------+--------+
|order_id|         order_date|order_customer_id|   order_status|test_lit|
+--------+-------------------+-----------------+---------------+--------+
|       1|2013-07-25 00:00:00|            11599|         CLOSED|      12|
|       2|2013-07-25 00:00:00|              256|PENDING_PAYMENT|      12|
|       3|2013-07-25 00:00:00|            12111|       COMPLETE|      12|
+--------+-------------------+-----------------+---------------+--------+
only showing top 3 rows
```

```
scala>   orders.withColumn("order_id_2", $"order_id"*2).show(3)
+--------+-------------------+-----------------+---------------+----------+
|order_id|         order_date|order_customer_id|   order_status|order_id_2|
+--------+-------------------+-----------------+---------------+----------+
|       1|2013-07-25 00:00:00|            11599|         CLOSED|         2|
|       2|2013-07-25 00:00:00|              256|PENDING_PAYMENT|         4|
|       3|2013-07-25 00:00:00|            12111|       COMPLETE|         6|
+--------+-------------------+-----------------+---------------+----------+
only showing top 3 rows
```

```
scala> orders.withColumn("order_id_4", expr("order_id*4")).show(3)
+--------+-------------------+-----------------+---------------+----------+
|order_id|         order_date|order_customer_id|   order_status|order_id_4|
+--------+-------------------+-----------------+---------------+----------+
|       1|2013-07-25 00:00:00|            11599|         CLOSED|         4|
|       2|2013-07-25 00:00:00|              256|PENDING_PAYMENT|         8|
|       3|2013-07-25 00:00:00|            12111|       COMPLETE|        12|
+--------+-------------------+-----------------+---------------+----------+
only showing top 3 rows
```

```
orders.withColumn("full name", concat($"order_id", lit(" "), $"order_status")).show(3)
```

　　使用lit()增加常量（固定值）,**lit()**是spark自带的函数，需要**import org.apache.spark.sql.functions**

## 14、行转列

　　有时候需要根据某个字段内容进行分割，然后生成多行，这时可以使用`explode`方法
　　下面代码中，根据`c3`字段中的空格将字段内容进行分割，分割的内容存储在新的字段`c3_`中，如下所示

```
jdbcDF.explode( "c3" , "c3_" ){time: String => time.split( " " )}
```

　　结果如下，

## 15、其他操作

　　API中还有`na, randomSplit, repartition, alias, as`方法，待后续补充。

## 三、Built-in functions

```python
import pyspark.sql.functions as F
help(F)
```

Better use full notation when mentioning the column names

**1.substring**

```
scala> orders.select(substring($"order_date",1,7).alias("Month")).show(3)
[Stage 0:>                                                          (0                                                                        +-------+
|  Month|
+-------+
|2013-07|
|2013-07|
|2013-07|
+-------+
only showing top 3 rows
```

```
scala> orders.select($"order_date".substr(1,7).alias("Month")).show(3)
+-------+
|  Month|
+-------+
|2013-07|
|2013-07|
|2013-07|
+-------+
only showing top 3 rows
```

**2.substring_index**

```
scala> orders.select(substring_index($"order_date","-",2)).show(3)
+---------------------------------+
|substring_index(order_date, -, 2)|
+---------------------------------+
|                          2013-07|
|                          2013-07|
|                          2013-07|
+---------------------------------+
only showing top 3 rows
```

**3.instr**

```
scala> orders.select(instr($"order_date","07")).show(3)
+---------------------+
|instr(order_date, 07)|
+---------------------+
|                    6|
|                    6|
|                    6|
+---------------------+
only showing top 3 rows
```

**4.split**

```
scala> orders.select(split($"order_date","-")(0).alias("Year")).show(3)
+----+
|Year|
+----+
|2013|
|2013|
|2013|
+----+
only showing top 3 rows
```

**5.concat**

```
scala> orders.select(concat($"order_status",lit(","),$"order_id")).show(3)
+---------------------------------+
|concat(order_status, ,, order_id)|
+---------------------------------+
|                         CLOSED,1|
|                PENDING_PAYMENT,2|
|                       COMPLETE,3|
+---------------------------------+
only showing top 3 rows
```

**6.concat_ws()**

is very important for concatenating the columns with field delimiter

```
scala> orders.select(concat_ws("!",$"order_status",$"order_id",$"order_customer_id")).show(3)
+-------------------------------------------------------+
|concat_ws(!, order_status, order_id, order_customer_id)|
+-------------------------------------------------------+
|                                         CLOSED!1!11599|
|                                   PENDING_PAYMENT!2...|
|                                       COMPLETE!3!12111|
+-------------------------------------------------------+
only showing top 3 rows
```

**7.reverse**

```
scala> orders.select(reverse($"order_status")).show(3)
+---------------------+
|reverse(order_status)|
+---------------------+
|               DESOLC|
|      TNEMYAP_GNIDNEP|
|             ETELPMOC|
+---------------------+
only showing top 3 rows
```

**8.length**

```
scala>  orders.select(length($"order_id")).show(3)
+----------------+
|length(order_id)|
+----------------+
|               1|
|               1|
|               1|
+----------------+
only showing top 3 rows
```

**9.when**

```
scala> orders.select($"order_id",when($"order_id" % 2 === 0, "EVEN").otherwise("ODD")).show(3)
+--------+-----------------------------------------------------+
|order_id|CASE WHEN ((order_id % 2) = 0) THEN EVEN ELSE ODD END|
+--------+-----------------------------------------------------+
|       1|                                                  ODD|
|       2|                                                 EVEN|
|       3|                                                  ODD|
+--------+-----------------------------------------------------+
only showing top 3 rows
```

**10.repeat**

```
scala> orders.select(repeat($"order_id",10)).show(3)
+--------------------+
|repeat(order_id, 10)|
+--------------------+
|          1111111111|
|          2222222222|
|          3333333333|
+--------------------+
only showing top 3 rows
```

**11.rpad**

```
scala> orders.select(rpad($"order_id",3,"*")).show(3)
+--------------------+
|rpad(order_id, 3, *)|
+--------------------+
|                 1**|
|                 2**|
|                 3**|
+--------------------+
only showing top 3 rows
```

**12.lpad**

```
scala> orders.select(lpad($"order_id",3,"*")).show(3)
+--------------------+
|lpad(order_id, 3, *)|
+--------------------+
|                 **1|
|                 **2|
|                 **3|
+--------------------+
only showing top 3 rows

```

**13.lower**

```
scala> orders.select(lower($"order_status")).show(3)
+-------------------+
|lower(order_status)|
+-------------------+
|             closed|
|    pending_payment|
|           complete|
+-------------------+
only showing top 3 rows
```

**14.upper**

```
scala>  orders.select(upper($"order_status")).show(3)
+-------------------+
|upper(order_status)|
+-------------------+
|             CLOSED|
|    PENDING_PAYMENT|
|           COMPLETE|
+-------------------+
only showing top 3 rows
```

**15.initcap**

```
scala> orders.select(initcap($"order_status")).show(3)
+---------------------+
|initcap(order_status)|
+---------------------+
|               Closed|
|      Pending_payment|
|             Complete|
+---------------------+
only showing top 3 rows
```

**16.lit**

```
scala> orders.select(lit(0),lit("Anything")).show(5)
+---+--------+
|  0|Anything|
+---+--------+
|  0|Anything|
|  0|Anything|
|  0|Anything|
|  0|Anything|
|  0|Anything|
+---+--------+
only showing top 5 rows
```

**17.like**

```
scala>  orders.filter($"order_id".like("44%")).show(3)
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|      44|2013-07-25 00:00:...|            10500|        PENDING|
|     440|2013-07-27 00:00:...|             7290|       COMPLETE|
|     441|2013-07-27 00:00:...|             5239|PENDING_PAYMENT|
+--------+--------------------+-----------------+---------------+
only showing top 3 rows
```

```
scala> orders.filter($"order_id".like("%44")).show(3)
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|      44|2013-07-25 00:00:...|            10500|        PENDING|
|     144|2013-07-26 00:00:...|             2158|     PROCESSING|
|     244|2013-07-26 00:00:...|             6910|PENDING_PAYMENT|
+--------+--------------------+-----------------+---------------+
only showing top 3 rows
```

```
scala>   orders.filter($"order_id".like("____4")).show(3)
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|   10004|2013-09-25 00:00:...|             7768|         CLOSED|
|   10014|2013-09-25 00:00:...|            10864|SUSPECTED_FRAUD|
|   10024|2013-09-25 00:00:...|             9678|PENDING_PAYMENT|
+--------+--------------------+-----------------+---------------+
only showing top 3 rows
```

**18.contains**

```
scala> orders.filter($"order_id".contains("444")).show(3)
+--------+--------------------+-----------------+------------+
|order_id|          order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|     444|2013-07-27 00:00:...|            10004|    COMPLETE|
|    1444|2013-08-01 00:00:...|             8302|    COMPLETE|
|    2444|2013-08-07 00:00:...|             9714|  PROCESSING|
+--------+--------------------+-----------------+------------+
only showing top 3 rows
```

**19.endswith**

```
scala>  orders.filter($"order_id".endsWith("444")).show(3)
+--------+--------------------+-----------------+------------+
|order_id|          order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|     444|2013-07-27 00:00:...|            10004|    COMPLETE|
|    1444|2013-08-01 00:00:...|             8302|    COMPLETE|
|    2444|2013-08-07 00:00:...|             9714|  PROCESSING|
+--------+--------------------+-----------------+------------+
only showing top 3 rows
```

**20.startswith**

```
scala>  orders.filter($"order_id".startsWith("444")).show(3)
+--------+--------------------+-----------------+------------+
|order_id|          order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|     444|2013-07-27 00:00:...|            10004|    COMPLETE|
|    4440|2013-08-20 00:00:...|             5434|    COMPLETE|
|    4441|2013-08-20 00:00:...|            10735|      CLOSED|
+--------+--------------------+-----------------+------------+
only showing top 3 rows
```

**21.isNotNull**

```
scala> orders.filter($"order_id".isNotNull).show(3)
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|       1|2013-07-25 00:00:...|            11599|         CLOSED|
|       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
|       3|2013-07-25 00:00:...|            12111|       COMPLETE|
+--------+--------------------+-----------------+---------------+
only showing top 3 rows
```

**22.isNull**

```
scala> orders.filter($"order_id".isNull).show(3)
20/08/28 22:32:23 WARN lazy.LazyStruct: Extra bytes detected at the end of the row! Ignoring similar problems.
+--------+----------+-----------------+------------+
|order_id|order_date|order_customer_id|order_status|
+--------+----------+-----------------+------------+
+--------+----------+-----------------+------------+

```

**23.isin**

```
scala>  orders.filter($"order_id".isin("1","2","3333",798)).show(3)
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|       1|2013-07-25 00:00:...|            11599|         CLOSED|
|       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
|     798|2013-07-29 00:00:...|             8709|       COMPLETE|
+--------+--------------------+-----------------+---------------+
only showing top 3 rows
```



## current_date()

In [ ]:

```
df =spark.createDataFrame([(1,)])
```

In [ ]:

```
df.select(current_date()).show()
```

## 四、Date Functions in Data Frames

## current_timestamp()

```
df.select(current_timestamp()).show()
```

```
df.select(current_timestamp()).take(1)
```

**1.date_format()**

Converts Date/Timestamp to String Format

```
scala> orders.select(date_format($"order_date", "yyyy-mm-dd").alias("date")).show(3)
[Stage 0:>                                                          (0                                                                      +----------+
|      date|
+----------+
|2013-00-25|
|2013-00-25|
|2013-00-25|
+----------+
only showing top 3 rows

```

```
df.select(date_format(current_timestamp(),'yyyy-MM-dd a HH:hh:mm:ss.S').alias('date')).take(1)
```



**2.Extraction in date**

```
scala>   orders.select(year($"order_date").alias("date_year")).show(3) 
+---------+
|date_year|
+---------+
|     2013|
|     2013|
|     2013|
+---------+
only showing top 3 rows
```

```
scala>   orders.select(month($"order_date").alias("date_month")).show(3)
+----------+
|date_month|
+----------+
|         7|
|         7|
|         7|
+----------+
only showing top 3 rows
```

**Options for extraction are**

year(), quarter(), month(), dayofmonth(), dayofweek(), dayofyear(), hour(), minute(), second(), weekofyear()

**3.date_add**

```
scala>   orders.select($"order_date", date_add($"order_date",1)).show(3)
+--------------------+-----------------------+
|          order_date|date_add(order_date, 1)|
+--------------------+-----------------------+
|2013-07-25 00:00:...|             2013-07-26|
|2013-07-25 00:00:...|             2013-07-26|
|2013-07-25 00:00:...|             2013-07-26|
+--------------------+-----------------------+
only showing top 3 rows
```

```
orders.select($"order_date", date_add($"order_date",-1)).show(3)
#Negative parameter works
```

**4.date_sub**

```
scala>   orders.select($"order_date", date_sub($"order_date",1)).show(3)
+--------------------+-----------------------+
|          order_date|date_sub(order_date, 1)|
+--------------------+-----------------------+
|2013-07-25 00:00:...|             2013-07-24|
|2013-07-25 00:00:...|             2013-07-24|
|2013-07-25 00:00:...|             2013-07-24|
+--------------------+-----------------------+
only showing top 3 rows
```

```
orders.select($"order_date", date_sub($"order_date",-1)).show(3)
#Negative parameter works
```

**5.date_diff**

```
scala>  orders.select(datediff($"order_date",date_sub($"order_date",1))).show(3)
+---------------------------------------------+
|datediff(order_date, date_sub(order_date, 1))|
+---------------------------------------------+
|                                            1|
|                                            1|
|                                            1|
+---------------------------------------------+
only showing top 3 rows
```

**6.add_months**

```
scala>  orders.select($"order_date", add_months($"order_date",1)).show(3)
+--------------------+-------------------------+
|          order_date|add_months(order_date, 1)|
+--------------------+-------------------------+
|2013-07-25 00:00:...|               2013-08-25|
|2013-07-25 00:00:...|               2013-08-25|
|2013-07-25 00:00:...|               2013-08-25|
+--------------------+-------------------------+
only showing top 3 rows
```

```
orders.select($"order_date", add_months($"order_date",-1)).show(3)
#Negative parameter works
```

**7.months_between**

```
scala> orders.select(months_between(date_add(add_months($"order_date",1),10), $"order_date").alias("months_btwn")).show(3)
+-----------+
|months_btwn|
+-----------+
| 1.32258065|
| 1.32258065|
| 1.32258065|
+-----------+
only showing top 3 rows
```

**8.next_day**

Gets the date of the next upcoming day

```
scala>  orders.select($"order_date", next_day($"order_date","sun").alias("Next_Sunday")).show(3)
+--------------------+-----------+
|          order_date|Next_Sunday|
+--------------------+-----------+
|2013-07-25 00:00:...| 2013-07-28|
|2013-07-25 00:00:...| 2013-07-28|
|2013-07-25 00:00:...| 2013-07-28|
+--------------------+-----------+
only showing top 3 rows
```

**9.last_day**

Gets the last day of the month

```
scala>  orders.select($"order_date", last_day($"order_date").alias("Last_day_of_the_month")).show(3)
+--------------------+---------------------+
|          order_date|Last_day_of_the_month|
+--------------------+---------------------+
|2013-07-25 00:00:...|           2013-07-31|
|2013-07-25 00:00:...|           2013-07-31|
|2013-07-25 00:00:...|           2013-07-31|
+--------------------+---------------------+
only showing top 3 rows
```

**10.trunc**

```scala
scala>   orders.select(trunc($"order_date","year"),trunc($"order_date","month")).show(3)
+-----------------------+------------------------+
|trunc(order_date, year)|trunc(order_date, month)|
+-----------------------+------------------------+
|             2013-01-01|              2013-07-01|
|             2013-01-01|              2013-07-01|
|             2013-01-01|              2013-07-01|
+-----------------------+------------------------+
only showing top 3 rows
```

**Unix Date/Time**

**13.from_unixtime**

Converts seconds after epoch date (1, Jan, 1970) to timestamp

```
time_df = spark.createDataFrame([(1428476400,)], ['unix_time'])
time_df.select(from_unixtime('unix_time', format="yyyy~MM~dd HH:mm:ss.SSSSSS").alias('ts')).collect()
```

```python
time_df.select(from_unixtime('unix_time', format="yyyy~MM~dd HH:mm:ss.SSSSSS").alias('ts')).show()
```

**14.unix_timestamp**

Converts date/timestamp into seconds after epoch date

```scala
scala>  orders.select(unix_timestamp($"order_date")).show(3)
+-----------------------------------------------+
|unix_timestamp(order_date, yyyy-MM-dd HH:mm:ss)|
+-----------------------------------------------+
|                                     1374681600|
|                                     1374681600|
|                                     1374681600|
+-----------------------------------------------+
only showing top 3 rows
```