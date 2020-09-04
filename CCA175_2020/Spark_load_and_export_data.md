# Spark load and export data 

## 一、Spark 从jdbc、parquet、orc、json、csv读取数据

### 1.Jdbc，例如mysql

```scala
import java.util.Properties
val connectionProperties = new Properties()
connectionProperties.put("user", "root")
connectionProperties.put("password", "199037")
val ordersDF = spark.read.jdbc("jdbc:mysql://localhost:3306", "retail_db.orders", connectionProperties)
//or
val ordersDF = spark.read.format( "jdbc" ).options(
      Map( "url" -> "jdbc:mysql://localhost:3306",
        "user" -> "root",
        "password" -> "199037",
        "dbtable" -> "retail_db.orders" )).load()
//or
val ordersDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306").option("driver","com.mysql.jdbc.Driver").option("dbtable", "retail_db.orders").option("user", "root").option("password", "199037").load()
//export data 
ordersDF.createOrReplaceTempView("orders")
val orderIdDF = spark.sql("select order_id from orders")
orderIdDF.show()
// save as parquet(default snappy compression)
ordersDF.write.parquet("/user/cuiyufei/sample-data/parquet")
// save as orc(default snappy compression)
ordersDF.write.orc("/user/cuiyufei/sample-data/orc")
// save as json
ordersDF.write.json("/user/cuiyufei/sample-data/json")
// save as csv
ordersDF.write.csv("/user/cuiyufei/sample-data/csv")
// save as avro
// spark-shell --packages org.apache.spark:spark-avro_2.11:2.4.0
ordersDF.write.format("avro").save("/user/cuiyufei/sample-data/avro")
```

### 2.Hive

```
val orders = spark.read.table('db_name.table_name')
```

### 3.Parquet

```scala
spark.read.parquet(path)
//or
spark.read.format("parquet").load(path)
eg:
val parq = spark.read.parquet("/user/cuiyufei/sample-data/parquet/")
//or
val parq = spark.read.format("parquet").load("/user/cuiyufei/sample-data/parquet/")
parq.printSchema()
```

### 4.Orc.

```scala
spark.read.orc(path)
//or
spark.read.format("orc").load(path)
eg:
val orcDF = spark.read.orc("file:/root/CCA175/sample-data/orc/")
//or
val orcDF = spark.read.format("orc").load("/user/cuiyufei/sample-data/orc")
orcDF.printSchema()
```

### 5.Json

```scala
spark.read.json(path)
//or
spark.read.format("json").load(path)
eg:
val jDF= spark.read.json("/user/cuiyufei/sample-data/json")
/or
val jDF = spark.read.format("json").load("/user/cuiyufei/sample-data/json")
jDF.printSchema()
jDF.createOrReplaceTempView("colors")
```

### 6.Avro.

```scala
spark.read.format("avro").load(path)
eg:
//spark-shell --packages org.apache.spark:spark-avro_2.11:2.4.0
val aFile = spark.read.format("avro").load("/user/cuiyufei/sample-data/avro/")
aFile.printSchema()
```

### 7.Csv

```scala
//推断头信息，省略首行的头信息
spark.read.option("inferSchema", "true").option("header", "true").csv(path)
//or
spark.read.format("csv").option("inferSchema", "true").option("header", true).load(path)
eg:
val cDF = spark.read.csv("/user/cuiyufei/sample-data/csv")
or
//
val cDF = spark.read.format("csv").load("/user/cuiyufei/sample-data/csv")
cDF.show
```

### 8.Text

```
spark.read.text(path)
//or
spark.read.format("text").load(path)
```

## 9.自定义分隔符

  有时候我们的输入数据分隔符是自定义的，我们可以使用读取csv的格式来读，虽然文件不是csv格式，但是通过csv的api可以指定分隔符，从而通过指定分隔符来读取数据。csv的option有如下选择：我们可以设置seq参数来设置自定义分隔符。

```
val dataFrame: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("encoding", "gbk2312")
      .load(path)
```

`spark`读取`csv`的时候，如果`inferSchema`开启，`spark`只会输入一行数据，推测它的表结构类型，避免遍历一次所有的数，禁用`inferSchema`参数的时候，或者直接指定`schema`。

| 参数                        | 解释                                                         |
| --------------------------- | ------------------------------------------------------------ |
| `sep`                       | 默认是`,` 指定单个字符分割字段和值                           |
| `encoding`                  | 默认是`uft-8`通过给定的编码类型进行解码                      |
| `quote`                     | 默认是`“`，其中分隔符可以是值的一部分，设置用于转义带引号的值的单个字符。如果您想关闭引号，则需要设置一个空字符串，而不是`null`。 |
| `escape`                    | 默认(`\`)设置单个字符用于在引号里面转义引号                  |
| `charToEscapeQuoteEscaping` | 默认是转义字符（上面的`escape`）或者`\0`，当转义字符和引号(`quote`)字符不同的时候，默认是转义字符(escape)，否则为`\0` |
| `comment`                   | 默认是空值，设置用于跳过行的单个字符，以该字符开头。默认情况下，它是禁用的 |
| `header`                    | 默认是`false`，将第一行作为列名                              |
| `enforceSchema`             | 默认是`true`， 如果将其设置为`true`，则指定或推断的模式将强制应用于数据源文件，而`CSV`文件中的标头将被忽略。 如果选项设置为`false`，则在`header`选项设置为`true`的情况下，将针对CSV文件中的所有标题验证模式。模式中的字段名称和CSV标头中的列名称是根据它们的位置检查的，并考虑了*`spark.sql.caseSensitive`。虽然默认值为`true`，但是建议禁用 `enforceSchema`选项，以避免产生错误的结果 |
| `inferSchema`               | inferSchema`（默认为`false`）：从数据自动推断输入模式。 *需要对数据进行一次额外的传递 |
| `samplingRatio`             | 默认为`1.0`,定义用于模式推断的行的分数                       |
| `ignoreLeadingWhiteSpace`   | 默认为`false`,一个标志，指示是否应跳过正在读取的值中的前导空格 |
| `ignoreTrailingWhiteSpace`  | 默认为`false`一个标志，指示是否应跳过正在读取的值的结尾空格  |
| `nullValue`                 | 默认是空的字符串,设置null值的字符串表示形式。从2.0.1开始，这适用于所有支持的类型，包括字符串类型 |
| `emptyValue`                | 默认是空字符串,设置一个空值的字符串表示形式                  |
| `nanValue`                  | 默认是`Nan`,设置非数字的字符串表示形式                       |
| `positiveInf`               | 默认是`Inf`                                                  |
| `negativeInf`               | 默认是`-Inf` 设置负无穷值的字符串表示形式                    |
| `dateFormat`                | 默认是`yyyy-MM-dd`,设置指示日期格式的字符串。自定义日期格式遵循`java.text.SimpleDateFormat`中的格式。这适用于日期类型 |
| `timestampFormat`           | 默认是`yyyy-MM-dd'T'HH:mm:ss.SSSXXX`，设置表示时间戳格式的字符串。自定义日期格式遵循`java.text.SimpleDateFormat`中的格式。这适用于时间戳记类型 |
| `maxColumns`                | 默认是`20480`定义多少列数目的硬性设置                        |
| `maxCharsPerColumn`         | 默认是`-1`定义读取的任何给定值允许的最大字符数。默认情况下为-1，表示长度不受限制 |
| `mode`                      | 默认（允许）允许一种在解析过程中处理损坏记录的模式。它支持以下不区分大小写的模式。请注意，`Spark`尝试在列修剪下仅解析`CSV`中必需的列。因此，损坏的记录可以根据所需的字段集而有所不同。可以通过`spark.sql.csv.parser.columnPruning.enabled`（默认启用）来控制此行为。 |
| mode下面的参数:             | ---------------------------------------------------          |
| `PERMISSIVE`                | 当它遇到损坏的记录时，将格式错误的字符串放入由“ columnNameOfCorruptRecord”配置的*字段中，并将其他字段设置为“ null”。为了保留损坏的记录，用户可以在用户定义的模式中设置一个名为`columnNameOfCorruptRecord` |
| `DROPMALFORMED`             | 忽略整个损坏的记录                                           |
| `FAILFAST`                  | 遇到损坏的记录时引发异常                                     |
| mode参数结束                | -------------------------------------------------------      |
| `columnNameOfCorruptRecord` | 默认值指定在`spark.sql.columnNameOfCorruptRecord`,允许重命名由`PERMISSIVE`模式创建的格式错误的新字段。这会覆盖`spark.sql.columnNameOfCorruptRecord` |
| `multiLine`                 | 默认是`false`,解析一条记录，该记录可能跨越多行               |

## 二、写入文件

## 1.Json

```scala
spark.write.json(path)
//or
inputDF.write.format("json").save(path)
```

## 2.Parquet

```scala
spark.write.parquet(path)
//or
inputDF.write.format("parquet").save(path)
```

## 3.Jdbc

```scala
val connectionProperties = new Properties()
connectionProperties.put("user", "username")
connectionProperties.put("password", "password")
//connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
spark.write
	//.option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
	.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
//or
spark.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "hadoop").save()
```

## 4.Csv

csv默认以英文逗号分割。

```scala
//推断头信息，省略首行的头信息
spark.write.csv(outputFile)
//or
spark.read.format("csv").save(outputFile)
```

## 5.Text

dataframe只能是一列数据，需要提前合并

```scala
spark.write.text(outputFile)
//or
spark.write.format("text").save(outputFile)
```

合并dataframe为一列写入text

```scala
//splitRex是分隔符
val allCols = dataFrame.columns.map(col(_))
dataFrame.select(concat_ws(splitRex, allCols: _*))
  .write.mode(SaveMode.Overwrite).text(saveDir)
```

## 自定义分隔符

  有时候我们输出数据时需要自定义分隔符，我们仍然可以使用写入csv的格式来写，但是不能通过seq参数来设置了，只能通过delimiter参数设置。

```spark
inputDF.write.option("delimiter", "\t").csv(outputFile)
```

## 参考

```
sqoop import --connect jdbc:mysql://localhost:3306/cca \
--username root \
--password 199037 \
-table orders \
--target-dir /user/cuiyufei/sample-data/avro \
--delete-target-dir \
--as-avrodatafile \
-m 5
```

[sparkSQL Dataset](http://spark.apache.org/docs/2.4.6/api/scala/index.html#org.apache.spark.sql.Dataset)

[Spark -- DataFrame按指定分隔符读取和写入文件](https://blog.csdn.net/Aeve_imp/article/details/107520678)

```
import org.apache.spark.sql.functions.date_format
al orders_filtered = orders.
    filter("order_status in ('COMPLETE', 'CLOSED')")
orders.
    filter(orders("order_status").isin("COMPLETE", "CLOSED")).
    show	

onthly_revenue_sorted.
    coalesce(1).
    write.
    mode("overwrite").
    option("header", "true").
    csv(s"/user/${username}/retail_db/monthly_revenue")
    
    
val nyseschema = StructType(List(
                             StructField("StockTicker",StringType,true),
                             StructField("transactiondate",StringType,true),
                             StructField("openprice",FloatType,true),
                             StructField("highprice",FloatType,true),
                             StructField("lowprice",FloatType,true),
                             StructField("closeprice",FloatType,true),
                             StructField("volume",IntegerType,true)                                 
                            )
                       )
nyse = spark.read.format(‘csv’).schema(’’‘stockticker string,
transactiondate string,
openprice float,
highprice float,
lowprice float,
closeprice float,
volume bigint’’’).load(’/user/swarajnewar/nyse’)
```

