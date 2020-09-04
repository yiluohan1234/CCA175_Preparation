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

```

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