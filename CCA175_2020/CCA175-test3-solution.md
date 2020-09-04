## Problem 1

### Instructions

Get the customers who have not placed any orders, sorted by `customer_lname` and then `customer_fname`

### Data Description

Data is available in local file system `/data/retail_db`

retail_db information:

- Source directories: `/data/retail_db/orders` and `/data/retail_db/customers`

- Source delimiter: comma(",")

- Source Columns - orders - order_id, order_date, order_customer_id, order_status

- Source Columns - customers - customer_id, customer_fname, customer_lname and many more

### Output Requirements
* Target Columns: customer_lname, customer_fname
* Number of Files: 1
* Place the output file in the HDFS directory  `/result/test3/problem1/`
* File format should be text
* delimiter is (",")
* Compression: Uncompressed

解题思路：

1、查看hdfs数据

```
hdfs dfs -cat /data/retail_db/orders/*|head -10
```

2、直接读取数据

```
val orders = spark.read.format("csv").load("/data/retail_db/orders")
orders.show(3)
```

3、读取数据自动识别数据类型

```
val orders = spark.read.format("csv").option("inferSchema", true).load("/data/retail_db/orders")
orders.printSchema
```

注：

```
option("header", true)//把第一行当作列标题
```

Solution(scala):

 ```scala
val orders = spark.read.format("csv").option("header", "false").option("sep", ",").option("header", "false").schema("order_id int, order_date timestamp, order_customer_id int, order_status string").load("/data/retail_db/orders")
val customers = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("/data/retail_db/customers").selectExpr("_c0 as customer_id", "_c1 as customer_fname", "_c2 as customer_lname")

val customersNoOrders = customers.join(orders, customers("customer_id") === orders("order_customer_id"), "left").where(orders("order_status").isNull).select("customer_fname", "customer_lname").orderBy("customer_fname", "customer_lname")
val result = customersNoOrders.selectExpr("concat(customer_fname, ',', customer_lname)")
result.coalesce(1).write.text("/result/test3/problem1/")
#customersNoOrders.coalesce(1).write.csv("/result/test3/problem1/")
 ```

```
val p2ord = spark.read.schema("order_id string, order_date string, order_customer_id string, order_status string").csv("/public/retail_db/orders")
val p2cust = spark.read.csv("/public/retail_db/customers").selectExpr("_c0 as customer_id", "_c1 as customer_fname", "_c2 as customer_lname")
val p2jn = p2cust.join(p2ord,'customer_id === 'order_customer_id,"left_anti").select('customer_lname, 'customer_fname).sort('customer_lname, 'customer_fname)
p2jn.coalesce(1).write.mode("overwrite").csv("/user/username/problem2/solution/")
```



## Problem 2

### Instructions

Get top 3 crime types based on number of incidents in `RESIDENCE` area using "Location Description".

### Data Description

Data is available in HDFS under `/data/crime/`

crime data information:

- Structure of data: (ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrst, Domestic, Beat, District, Ward, Community Area, FBI Code, X Coordinate, Y Coordinate, Year, Updated on, Latitude, Longitude, Location)
- File format - text file
- Delimiter - "," (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma and enclosed using double quotes.

### Output Requirements

* Output Fields: crime_type, incident_count
* Output File Format: JSON
* Delimiter: N/A
* Compression: No
* Place the output file in the HDFS directory `/result/test3/problem2/`


Solution(scala):

```scala
val crime_data = spark.read.format("csv").option("header", true).option("inferSchema", "true").load("/data/crime/")

val basic_data = crime_data.select(col("ID").alias("id"), col("Primary Type").alias("crime_type"), col("Location Description").alias("location")).where("location = 'RESIDENCE'")

val result = basic_data.groupBy("crime_type").agg(count("id").alias("incident_count")).orderBy(desc("incident_count")).limit(3)
result.write.mode("overwrite").json("/result/test3/problem2/")
```

```
val p3df = spark.read.csv("/public/crime/csv").selectExpr("_c0 as id","_c5 as crime_type", "_c7 as loc")
p3df.filter("loc = 'RESIDENCE'")
    .groupBy('crime_type).agg(count('id).alias("incident_count"))
    .sort('incident_count desc).limit(3)
    .write.mode("overwrite").json("/user/username/problem3/solution/")
```



## Problem 3

### Instructions

Convert NYSE data into parquet.

### NYSE data Description

Data is available in HDFS file system under `/data/nyse` 

NYSE Data information:

Fields (stockticker:string, transactiondate:string, openprice:float, highprice:float, lowprice:float, closeprice:float, volume:bigint)

### Output Requirements

* Column Names: stockticker, transactiondate, openprice, highprice, lowprice, closeprice, volume
* Convert file format to parquet
* Place the output file in the HDFS directory `/result/test3/problem3/`

Solution:

```scala
val nyse = spark.read.format("csv").schema("stockticker string, transactiondate string, openprice float, highprice float, lowprice float, closeprice float, volume bigint").load("/data/nyse")
nyse.coalesce(1).write.mode("overwrite").parquet("/result/test3/problem3/")
```

```
val p4df = spark.read.schema("stockticker string, transactiondate string, openprice float, highprice float, lowprice float, closeprice float, volume bigint")
 .csv("/user/username/nyse")
p4df.write.mode("overwrite").parquet("/user/username/problem4/solution/")
```



## Problem 4

### Instructions

Get total number of orders for each customer where the cutomer_state = 'TX'.

### Data Description

retail_db data is available in HDFS at `/data/retail_db`

retail_db data information:

- Source directories: `/data/retail_db/orders and /data/retail_db/customers`
- Source Columns - orders - order_id, order_date, order_customer_id, order_status
- Source Columns - customers - customer_id, customer_fname, customer_lname, customer_state (8th column) and many more
- delimiter: (",")

### Output Requirements

* Output Fields: customer_fname, customer_lname, order_count
* File Format: text
* Delimiter: Tab character ("\t")
* Place the result file in the HDFS directory `/result/test3/problem4/`

Solution:

```scala
val orders = spark.read.format("csv").option("sep", ",").option("header", "false").schema("order_id int, order_date timestamp, order_customer_id int, order_status string").load("/data/retail_db/orders")
val customers = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("/data/retail_db/customers").selectExpr("_c0 as customer_id", "_c1 as customer_fname", "_c2 as customer_lname", "_c7 as customer_state")

val orders_count = customers.where("customer_state ='TX'").join(orders, customers("customer_id") === orders("order_customer_id")).groupBy("customer_id").agg(count("order_id").alias("order_count"))

val result = orders_count.join(customers, orders_count("customer_id") === customers("customer_id")).select("customer_fname", "customer_lname", "order_count")

result.coalesce(1).write.mode("overwrite").option("sep", "\t").csv("/result/test3/problem4/")
```

```
val p6ord = spark.read.schema("order_id int, order_date timestamp, order_customer_id int, order_status string").csv("/public/retail_db/orders")
val p6cust =spark.read.csv("/public/retail_db/customers").selectExpr("_c0 as customer_id", "_c1 as customer_fname", "_c2 as customer_lname").filter("_c7 ='TX'")
val p6jn = p6cust.join(p6ord.select('order_customer_id),'customer_id === 'order_customer_id)           .groupBy('customer_fname,'customer_lname).agg(count('customer_id).alias("order_count"))
p6jn.write.option("sep","\t").csv("/user/username/problem6/solution/")
```



## Problem 5

### Instructions

List the names of the Top 5 products by revenue ordered on '2013-07-26'. Revenue is considered only for `COMPLETE` and `CLOSED` orders.

### Data Description

retail_db data is available in HDFS at `/data/retail_db`

retail_db data information:

Source directories:

- `/public/retail_db/orders`

- `/public/retail_db/order_items`
- `/public/retail_db/products`
- Source delimiter: comma(",")
- Source Columns - orders - order_id, order_date, order_customer_id, order_status
- Source Columns - order_itemss - order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price
- Source Columns - products - product_id, product_category_id, product_name, product_description, product_price, product_image

### Output Requirements

* Target Columns: order_date, order_revenue, product_name, product_category_id
* Data has to be sorted in descending order by order_revenue
* File Format: text
* Delimiter: colon (:)
* Place the output file in the HDFS directory `/result/test1/problem5/`

Solution:

```scala
val orders = spark.read.format("csv").schema("order_id int, order_date string, order_customer_id int, order_status string").load("/data/retail_db/orders")

val products = spark.read.format("csv").schema("product_id int, product_category_id int, product_name string, product_description string, product_price float, product_image string").load("/data/retail_db/products")

val order_items = spark.read.format("csv").schema("order_item_id int,order_item_order_id int, order_item_product_id int, order_item_quantity int,order_item_subtotal float,order_item_product_price float").load("/data/retail_db/order_items")

val orde_filter_data = orders.where("order_date like '2013-07-26%'").filter("order_status = 'CLOSED' or order_status = 'COMPLETE'")

val order_and_items = orde_filter_data.join(order_items, orde_filter_data("order_id") === order_items("order_item_order_id")).groupBy("order_date", "order_item_product_id").agg(round(sum("order_item_subtotal"),2).alias("order_revenue")).orderBy(desc("order_revenue"))

val result = order_and_items.join(products, order_and_items("order_item_product_id") === products("product_id")).select("order_date", "order_revenue", "product_name", "product_category_id").limit(5)
result.coalesce(1).write.mode("overwrite").option("sep", ":").csv("/result/test1/problem5/")
```

```
val p7ord = spark.read.option("inferSchema","true").csv("/public/retail_db/orders").toDF("order_id", "order_date", "order_customer_id", "order_status").filter("order_date ='2013-07-26' and order_status in ('COMPLETE','CLOSED')")
val p7oitm = spark.read.option("inferSchema","true").csv("/public/retail_db/order_items").toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")
val p7prod = spark.read.option("inferSchema","true").csv("/public/retail_db/products").toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")

val p7jn = p7oitm.join(p7ord,'order_item_order_id === 'order_id).join(p7prod,'order_item_product_id === 'product_id)

p7jn.groupBy('order_item_product_id,'product_name,'product_category_id,'order_date).agg(sum('order_item_subtotal).alias("revenue")).sort('revenue desc).limit(5).selectExpr("order_date", "revenue as order_revenue", "product_name", "product_category_id").write.mode("overwrite").option("sep",":").csv("/user/username/problem7/solution/")
```



## Problem 6

### Instructions

List the order Items where the order_status = 'PENDING_PAYMENT' order by `order_id`

### Data Description

Data is available in HDFS location.

retail_db data information:

- Source directories: `/data/retail_db/orders`
- Source delimiter: comma(",")
- Source Columns - orders - order_id, order_date, order_customer_id, order_status

### Output Requirements

* Target columns: order_id, order_date, order_customer_id, order_status
  File Format: orc 
* Place the output files in the HDFS directory `/result/test1/problem6/`

Solution:

```scala
val orders = spark.read.format("csv").schema("order_id int, order_date string, order_customer_id int, order_status string").load("/data/retail_db/orders")

val result = orders.where("order_status = 'PENDING_PAYMENT'").orderBy("order_id")
result.write.mode("overwrite").orc("/result/test1/problem6/")
//result.coalesce(1).write.mode("overwrite").option("compression", "snappy").orc("/result/test1/problem6/")
```

```
val p7ord = spark.read.option("inferSchema","true").csv("/public/retail_db/orders").toDF("order_id", "order_date", "order_customer_id", "order_status")
p7ord.filter("order_status = 'PENDING_PAYMENT'").sort('order_id).write.mode("overwrite").orc("/user/username/problem8/solution/")
```



## Problem 7

### Instructions

Remove header from h1b data.

### Data Description

h1b data with ascii null "," as delimiter is available in HDFS.

h1b data information:

- HDFS location: `/data/h1b/`
- First record is the header for the data

### Output Requirements

* Remove the header from the data and save rest of the data as is orc
* Data should be compressed using snappy algorithm
* Place the H1B data in the HDFS directory `/result/test3/problem7/`

Solution:

```scala
val h1b = spark.read.format("csv").option("sep", "\0").load("/data/h1b/h1b.csv")
h1b.coalesce(1).write.mode("overwrite").option("compression", "snappy").orc("/result/test3/problem7/")
```

```
val p9df = spark.read.option("sep","\u0000").option("inferSchema","true").option("header","true").csv("/public/h1b/h1b_data")
p9df.write.option("compression","snappy").mode("overwrite").option("sep","\u0000").csv("/user/username/problem9/solution/")
```



## Problem 8

### Instructions

Get number of LCAs filed for each year.

### Data Description

h1b data with ascii null "," as delimiter is available in HDFS

h1b data information:

- HDFS Location: `/data/h1b/`
- Ignore first record which is header of the data
- YEAR is 8th field in the data
- There are some LCAs for which YEAR is NA, ignore those records

### Output Requirements

* File Format: text
* Output Fields: YEAR, NUMBER_OF_LCAS
* Delimiter: Ascii null "\0"
* Place the output files in the HDFS directory `/result/test3/problem8/`

Solution:

```scala
val h1b = spark.read.format("csv").option("header", "false").option("sep", "\0").load("/data/h1b/h1b_data").selectExpr("_c7 as YEAR")
val result = h1b.filter("YEAR != NA").groupBy("YEAR").agg(count("_c0").alias("NUMBER_OF_LCAS"))
result.coalesce(1).write.mode("overwrite").format("text").option("sep", "\0").save("/result/test3/problem8/")

//h1b_write = h1b_stage.selectExpr("concat(YEAR,"\0",NUMBER_OF_LCAS)")
///h1b_write.coalesce(1).write.mode("overwrite").format("text").save("/result/test3/problem8/")
```

```
val p10df = spark.read.option("sep","\u0000").csv("/public/h1b/h1b_data").filter("_c7 != 'NA'")
p10df.groupBy('_c7).agg(count('_c0).alias("NUMBER_OF_LCAS")).withColumnRenamed("_c7","YEAR").write.mode("overwrite").option("sep","\u0000").csv("/user/username/problem10/solution/")
```



## Problem 9

### Instructions

Get number of LCAs by status for the year 2016.

### Data Description

h1b data with ascii null "," as delimiter is available in HDFS

h1b data information:

- HDFS Location: `/data/h1b/`
- Ignore first record which is header of the data
- YEAR is 8th field in the data
- STATUS is 2nd field in the data
- There are some LCAs for which YEAR is NA, ignore those records

### Output Requirements

* File Format: json
* Output Field Names: year, status, count
* Place the output files in the HDFS directory `/result/test3/problem9/`
* Replace `whoami` with your OS user name

Solution:

```scala
val h1b = spark.read.format("csv").option("sep", ",").option("header", true).option("inferSchema", true).load("/data/h1b")
val result = h1b.where("YEAR = '2016' and YEAR != 'NA'").groupBy("YEAR","CASE_STATUS").agg(count("ID").alias("count")).selectExpr("YEAR as year", "CASE_STATUS as status", "count")
result.coalesce(1).write.mode("overwrite").format("json").save("/result/test3/problem9/")
```

```
val p11df = spark.read.option("sep","\u0000").csv("/public/h1b/h1b_data").filter("_c7='2016'")
p11df.groupBy('_c1,'_c7).agg(count('_c1).alias("count")).selectExpr("_c7 as year","_c1 as status","count").write.mode("overwrite").json("/user/username/problem11/solution/")
```



## Problem 10

### Instructions

Get top 5 employers for year 2016 where the status is WITHDRAWN or CERTIFIED-WITHDRAWN or DENIED.

### Data Description

h1b data with ascii null "," as delimiter is available in HDFS

h1b data information:

- HDFS Location: `/data/h1b/`
- Ignore first record which is header of the data
- YEAR is 7th field in the data
- STATUS is 2nd field in the data
- EMPLOYER is 3rd field in the data
- There are some LCAs for which YEAR is NA, ignore those records

### Output Requirements

- File Format: parquet
- Output Fields: employer_name, lca_count
- Data needs to be in descending order by count
- Place the output files in the HDFS directory `/result/test3/problem10/`

Solution:

```scala
val h1b = spark.read.format("csv").option("sep", ",").option("header", true).option("inferSchema", true).load("/data/h1b").selectExpr("CASE_STATUS as status", "EMPLOYER_NAME as employer_name", "YEAR as year", "ID as id")
val result = h1b.where("year != 'NA' and year = '2016' and status in('WITHDRAWN', 'CERTIFIED-WITHDRAWN', 'DENIED')").groupBy("employer_name").agg(count("id").alias("lca_count")).orderBy(desc("lca_count")).limit(5)
result.write.mode("overwrite").format("parquet").save("/result/test3/problem10/")
```

```
val p12df =spark.read.option("sep","\u0000").csv("/public/h1b/h1b_data").filter("_c7='2016' and _c1 in ('WITHDRAWN','CERTIFIED-WITHDRAWN','DENIED')")
p12df.groupBy('_c2).agg(count('_c0).alias("lca_count"))
     .withColumnRenamed("_c2","employer_name")
     .sort('lca_count desc)
     .limit(5)
     .write.mode("overwrite").parquet("/user/username/problem12/solution/")
```



## Problem 11

### Instructions

Copy all h1b data from HDFS to Hive table excluding those where year is NA or prevailing_wage is NA.

### Data Description

h1b data with ascii null "," as delimiter is available in HDFS

h1b data information:

- HDFS Location: `/data/h1b/no_header`
- Fields:ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
- Ignore data where PREVAILING_WAGE is NA or YEAR is NA
  PREVAILING_WAGE is 7th field
- YEAR is 8th field
- Number of records matching criteria: 3002373

### Output Requirements

- Save it in Hive Database
- Create Database: CREATE DATABASE IF NOT EXISTS `retail_db`
- Switch Database: USE `retail_db`
- Save data to hive table h1b_data
- Create table command:

```
CREATE TABLE h1b_data (
  ID                 INT,
  CASE_STATUS        STRING,
  EMPLOYER_NAME      STRING,
  SOC_NAME           STRING,
  JOB_TITLE          STRING,
  FULL_TIME_POSITION STRING,
  PREVAILING_WAGE    DOUBLE,
  YEAR               INT,
  WORKSITE           STRING,
  LONGITUDE          STRING,
  LATITUDE           STRING
)
```

Solution:

```scala
val h1b_raw = spark.read.format("csv").option("header", "false").option("sep", ",").load("/data/h1b/no_header")


val h1b_stage = h1b_raw.where("_c6!='NA' AND _c7!='NA'")
h1b_stage.createOrReplaceTempView("h1b_stage")
spark.sql("DROP TABLE retail_db.h1b_data")
spark.sql("create table retail_db.h1b_data(ID INT,CASE_STATUS STRING,EMPLOYER_NAME STRING,SOC_NAME STRING,JOB_TITLE STRING,FULL_TIME_POSITION STRING,PREVAILING_WAGE DOUBLE,YEAR INT,WORKSITE STRING,LONGITUDE STRING,LATITUDE STRING)")
spark.sql("INSERT INTO retail_db.h1b_data select * from h1b_data limit 10")
```

```
val p13df = spark.read.option("sep","\u0000").option("inferSchema","true").csv("/public/h1b/h1b_data_noheader").toDF("ID","CASE_STATUS","EMPLOYER_NAME","SOC_NAME","JOB_TITLE","FULL_TIME_POSITION","PREVAILING_WAGE","YEAR","WORKSITE","LONGITUDE","LATITUDE").filter("year!='NA' and prevailing_wage !='NA'").withColumn("year",expr("cast (year as int)")).withColumn("prevailing_wage",expr("cast(prevailing_wage as double)"))
p13df.write.mode("overwrite").saveAsTable("database_name.h1b_data")

```



## Problem 12

### Instructions

Get the stock tickers from NYSE data for which full name is missing in NYSE symbols data.

### NYSE data Description

NYSE data with "," as delimiter is available in HDFS

NYSE data information:

- HDFS location: `/data/nyse`
- There is no header in the data
- NYSE Symbols data with " " as delimiter is available in HDFS

NYSE Symbols data information:

- HDFS location: `/data/nyse_symbols`
- First line is header and it should be included

### Output Requirements

- Get unique stock ticker for which corresponding names are missing in NYSE symbols data
- Save data back to HDFS
- File Format: avro
- Avro dependency details:
  groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
- Place the sorted NYSE data in the HDFS directory `/result/test3/problem12/`

Solution:

```scala
val nyse = spark.read.format("csv").schema("stockticker string, transactiondate string, openprice float, highprice float, lowprice float, closeprice float, volume float").load("/public/nyse")

val nyse_symbols = spark.read.format("csv").option("sep","\t").option("header","true").option("inferSchema","true").load(""/public/nyse_symbols")

val nyse_missing = nyse.join(nyse_symbols,nyse("stockticker") === nyse_symbols("Symbol"),"left").where(col("Symbol").isNull)

nyse_missing.select("stockticker").distinct().orderBy("stockticker").write.mode("overwrite").format("com.databricks.spark.avro").save("/user/swarajnewar/web_problem17/solution/")

```

## Problem 13

### Instructions

Get the name of stocks displayed along with other information.

### Data Description

NYSE data information:

- HDFS location: `/data/nyse`
- There is no header in the data
- NYSE Symbols data with tab character (\t) as delimiter is available in HDFS

NYSE Symbols data information:

- HDFS location: /public/nyse_symbols
- First line is header and it should be included

### Output Requirements

- Get all NYSE details along with stock name if exists, if not stockname should be empty
- Column Order: stockticker, stockname, transactiondate, openprice, highprice, lowprice, closeprice, volume
- Delimiter: ,
- File Format: text
- Place the data in the HDFS directory `/result/test3/problem13/`

Solution:

```scala
val nyse = spark.read.format("csv").schema("stockticker string, transactiondate string, openprice float, highprice float, lowprice float, closeprice float, volume float").load("/public/nyse")

val nyse_symbols = spark.read.format("csv").
option("header","true").option("inferSchema","true").load("/public/nyse_symbols")

val nyse_joined = nyse.join(nyse_symbols,nyse("stockticker") === nyse_symbols("Symbol"),"left").withColumn("stockname",col("Description")).select("stockticker", "stockname", "transactiondate", "openprice", "highprice", "lowprice", "closeprice", "volume")

nyse_joined.write.mode("overwrite").format("csv").option("sep",",").save("/result/test3/problem13/")
```

## Problem 14

### Instructions

Get number of companies who filed LCAs for each year.

### Data Description

h1b data with ascii null "," as delimiter is available in HDFS

h1b data information:

- HDFS Location: `/data/h1b/no_header`
- Fields:
  ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE
- Use EMPLOYER_NAME as the criteria to identify the company name to get number of companies
  YEAR is 8th field
- There are some LCAs for which YEAR is NA, ignore those records

### Output Requirements

- File Format: text
- Delimiter: tab character "\t"
- Output Field Order: year, lca_count
- Place the output files in the HDFS directory `/result/test3/problem14/`

Solution:

```scala
val h1b_raw = spark.read.format("csv").option("sep",",").load("/data/h1b/no_header").toDF("ID", "CASE_STATUS", "EMPLOYER_NAME", "SOC_NAME", "JOB_TITLE", "FULL_TIME_POSITION", "PREVAILING_WAGE", "YEAR", "WORKSITE", "LONGITUDE", "LATITUDE")
val h1b_stage = h1b_raw.where("YEAR!='NA'").groupBy("YEAR").agg(count("EMPLOYER_NAME").alias("lca_count")).withColumn("year",col("YEAR")).select("year","lca_count")
h1b_stage.write.mode("overwrite").format("csv").option("sep","\t").save("/result/test3/problem14/")
```

```
val p19df = spark.read.option("sep","\0").csv("/public/h1b/h1b_data_noheader")
p19df.selectExpr("_c7 as year","_c2 as employer_name")
  .filter("year != 'NA'")
  .groupBy('year).agg(count('employer_name).alias("lca_count"))
  .write.mode("overwrite").option("sep","\t").csv("/user/username/problem19/solution/")
```



## Problem 15

### Instructions

Export h1b data from hdfs to MySQL Database

### Data Description

h1b data with ascii character "," as delimiter is available in HDFS

h1b data information:

HDFS Location: `/data/h1b/no_header`

Fields:

- ID, CASE_STATUS, EMPLOYER_NAME, SOC_NAME, JOB_TITLE, FULL_TIME_POSITION, PREVAILING_WAGE, YEAR, WORKSITE, LONGITUDE, LATITUDE

### Output Requirements

- Export data to MySQL Database
- MySQL database is running on localhost:3306
- User: root
- Password: 199037
- Database Name: retail_db 
- Table Name: h1b_data
- Nulls are represented as: NA
- After export nulls should not be stored as NA in database. It should berepresented as database n

Create table command:

```
CREATE TABLE h1b_data (
ID INT,
CASE_STATUS VARCHAR(50),
EMPLOYER_NAME VARCHAR(100),
SOC_NAME VARCHAR(100),
JOB_TITLE VARCHAR(100),
FULL_TIME_POSITION VARCHAR(50),
PREVAILING_WAGE FLOAT,
YEAR INT,
WORKSITE VARCHAR(50),
LONGITUDE VARCHAR(50),
LATITUDE VARCHAR(50));
```

Above create table command can be run using
Login using `mysql -u root  -p`
When prompted enter password 199037
Switch to database using `use retail_db`

Solution:

```
val h1b = spark.read.format("csv").option("header", "false").schema("ID string, CASE_STATUS string, EMPLOYER_NAME string, SOC_NAME string, JOB_TITLE string, FULL_TIME_POSITION string, PREVAILING_WAGE float, YEAR int, WORKSITE string, LONGITUDE string, LATITUDE string")load("/data/h1b/no_header")

h1b.limit(10).write.mode("overwrite").format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3306", "user" -> "root", "password" -> "199037", "dbtable" -> "retail_db.h1b_data")).save()
```

## Problem 16

### Instructions

Get NYSE data in ascending order by date and descending order by volume

### Data Description

NYSE data with "," as delimiter is available in HDFS

Note:Download this data from https://github.com/dgadiraju/data

`NYSE data information:`

- HDFS location: `/data/nyse`
- There is no header in the data

### Output Requirements

- Save data back to HDFS
- Column order: stockticker, transactiondate, openprice, highprice, lowprice,
- closeprice, volume
- File Format: text
- Delimiter: :

Solution:

```scala
//option("inferSchema", true)判断类型，schema定义类型
val nyse = spark.read.format("csv").schema("stockticker string, transactiondate string, openprice float, highprice float, lowprice float,closeprice float, volume int").load("/data/nyse")
val nyseOrdered = nyse.orderBy(asc("transactiondate"),desc("volume"))
val nyseColumnsMap = nyse.columns.map(col(_))
val result = nyseOrdered.select(concat_ws(":", nyseColumnsMap:_*))
result.limit(10).write.mode("overwrite").text("/result/test4/problem16")
```

## Problem 17

### Instructions

Get word count for the input data using space as delimiter (for each word, we need to get how many times it is repeated in the entire input data set)

### Data Description

Data is available in HDFS /public/randomtextwriter

word count data information:

- Number of executors should be 10
- executor memory should be 3 GB
- Executor cores should be 20 in total (2 per executor)
- Number of output files should be 8
- Avro dependency details: groupId -> com.databricks, artifactId -> sparkavro_2.10, version -> 2.0.1

### Output Requirements

- Output File format: Avro
- Output fields: word, count
- Compression: Uncompressed

Solution:

```python
#Notes:
	I don't have randomtextwriter file on my windows machine. 
	So, I'm using a text file with some random words. 
	#change the path as required
	#load("/public/randomtextwriter")
	
	To save the output in avro format you need to launch your spark shell with --packages org.apache.spark:spark-avro_2.12:3.0.0
	avro is developed by data bricks. it is not by default avilable in spark. you need to import it.

#Solution: 
	launch the spark shell like this
	pyspark
	–master yarn
	–num-executors 10
	–executor-memory 3GB
	–executor-cores 2
	--packages org.apache.spark:spark-avro_2.12:3.0.0 #use this if you are using windows machine
	#databricks --packages com.databricks:spark-avro_2.11:4.0.0 # use this for linux # inplace of avro, you need to specify com.databricks.spark.avro
	

	from pyspark.sql.functions import explode,split,count 
	
	#read the file
	rt=spark.read.format('text').load("C:\\SparkCourse\\CCA175\\data-master\\nyse_all\\randomtext\\") 
	
	Notes: If you read text file, output will be a text column with name "value"
	
	>>> rt.show(5)
	+--------------------+
	|               value|
	+--------------------+
	|Offer rules: You ...|
	+--------------------+

	#use split to divide the text into words and explode to put it in multiple rows
	rtp=rt.selectExpr("explode(split(value, ' ')) as word")

	#another way
	rtp=rt.select(explode(split("value", " ")).alias("word"))
		
	#output and save 
	output=rtp.groupBy('word').agg(count('word').alias('wordcount')).orderBy('wordcount', ascending=False)
	
	output.coalesce(8).write.mode('overwrite').format("avro").save("C:\\SparkCourse\\CCA175\\data-master\\nyse_all\\randomtext\\output\\")
	
	#validate 
	spark.read.format('avro').load("C:\\SparkCourse\\CCA175\\data-master\\nyse_all\\randomtext\\output\\").show(5,False)
```

## Bonus Problem 1

Details - Duration 40 minutes
Data set URL 825
Choose language of your choice Python or Scala
Data is available in HDFS file system under /public/crime/csv
You can check properties of files using hadoop fs -ls -h /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,”
Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending order
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month
Output File Format: TEXT
Output Columns: Month in YYYYMM format, crime count, crime type
Output Delimiter: \t (tab delimited)
Output Compression: gzip

```python
Sol:

DF APIs approach:
pyspark --master yarn --conf spark.ui.port=0 --num-executors=6 --executor-memory=3G --executor-cores=2

from pyspark.sql.functions import *

crime = spark.read.format(‘csv’).option(‘header’,‘true’).load(’/public/crime/csv/crime_data.csv’)

crime_filtered = crime.select(col(‘Date’).alias(‘date’), col(‘Primary Type’).alias(‘crime_type’))

crime_df = crime_filtered.withColumn(‘date’,to_timestamp(substring(‘date’,1,10), ‘MM/dd/yyyy’))

crimeDF = crime_df.withColumn(‘month’, date_format(‘date’,‘YYYYMM’))

crimeFinal = crimeDF.groupBy(‘month’,‘crime_type’).agg(count(‘crime_type’).alias(‘Cnt’)).orderBy(‘month’,col(‘Cnt’).desc())

crimeSave = crimeFinal.selectExpr(“concat(month,’\t’,crime_type,’\t’,Cnt)”).coalesce(2)

crimeSave.write.format(‘text’).mode(‘overwrite’).option(‘codec’,‘gzip’).save(’/user/swarajnewar/solutions/solutions01/crimeData’)

DF-SQL approach:
pyspark --master yarn --conf spark.ui.port=0 --num-executors=6 --executor-memory=3G --executor-cores=2

from pyspark.sql.functions import *

crime_raw = spark.read.format(‘csv’).option(‘header’,‘true’).load(’/public/crime/csv/crime_data.csv’).withColumn(‘date’,to_timestamp(substring(‘Date’,1,10), ‘MM/dd/yyyy’)).withColumn(‘crimeType’, col(‘Primary Type’))

crime_stage = crime_raw.select(crime_raw.date, crime_raw.crimeType)

crime_stage.createOrReplaceTempView(‘crime_stage’)

crime_analysis = spark.sql(“select date, date_format(date,‘YYYYMM’) as month, crimeType from crime_stage”)

crime_analysis.createOrReplaceTempView(‘crime_analysis’)

crime_save = spark.sql(“select month, COUNT(*) as Cnt, crimeType from crime_analysis GROUP BY month, crimeType ORDER BY month, Cnt DESC”).coalesce(2)

crime_save.write.format(‘csv’).mode(‘overwrite’).option(‘sep’,’\t’).option(‘codec’, ‘gzip’).save(’/user/swarajnewar/solutions/solutions01/crimeDataSQL’)
```

## Bonus Problem 2

### Instructions

Get top n traded stocks by volume each month from NYSE data within a given year.

### Data Description

NYSE data with "," as delimiter is available in HDFS

NYSE data information:

- HDFS location: /public/nyse
- There is no header in the data
- NYSE Symbols data with " " as delimiter is available in HDFS

```python
Sol:

Taking the year as 2016 and n as 5 :

nyse = spark.read. \
format('csv'). \
schema('stockticker string, transactiondate string, openprice float, highprice float, lowprice float, closeprice float, volume float'). \
load('/public/nyse')

top5StockByVolume2016 =
nyse.withColumn(‘transactiondate’,to_timestamp(‘transactiondate’, ‘yyyyMMdd’)).
where(date_format(‘transactiondate’,‘yyyy’)==‘2016’).
withColumn(‘month’, date_format(‘transactiondate’,‘MM’)).
withColumn(‘rnk’, rank().over(Window.partitionBy(col(‘month’)).
orderBy(nyse.volume.desc()))).
where(col(‘rnk’)<=5).
orderBy(col(‘month’))

```



```python
orders = spark.read.jdbc('jdbc:mysql://localhost:3306', 'retail_db.orders',  properties = {'user' : 'root', 'password' : '199037'})

>>> orders.printSchema()
root
 |-- order_id: integer (nullable = true)
 |-- order_date: timestamp (nullable = true)
 |-- order_customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)
>>> orders.select('order_id', 'order_status')
DataFrame[order_id: int, order_status: string]
>>> orders.describe()
DataFrame[summary: string, order_id: string, order_customer_id: string, order_status: string]
```

```
select(col("Date").alias("date"), col("Primary Type").alias("crime_type"))
selectExpr("Date as date","'Primary Type' as crime_type"))
withColumnRenamed("Date", "date")
toDF("date","type")
```





```scala
import java.util.Properties
val connectionProperties = new Properties()
connectionProperties.put("user", "root")
connectionProperties.put("password", "199037")
val orders= spark.read.jdbc("jdbc:mysql://localhost:3306", "retail_db.orders", connectionProperties)

```

## Tips

[data](https://github.com/dgadiraju/data)

<http://nn02.itversity.com/cca175/>

<https://github.com/swarajnewar/CCA175-Practice-Tests-With-Solutions>

<https://github.com/ramapilli16/CCA175-PySpark-Practice-with-solutions>

<https://github.com/jagadeeshpamarthi/CCA175Preparation>

<https://github.com/Prakash-Ponnusamy1/CCA175_Master_Preparation>

<https://github.com/AAbdul12/CCA175-ClouderaMaterial>

<https://github.com/DeNdiki/PySpark>

<https://github.com/veeraravi/demo_cca175>

<https://www.jianshu.com/p/2ac79fa55062>

<https://blog.csdn.net/OldDirverHelpMe/article/details/106120312>

<https://blog.csdn.net/dabokele/article/details/52802150>

<https://blog.csdn.net/Aeve_imp/article/details/107520678>