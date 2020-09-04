**今天(2019.01.29)考了CCA175考试，针对考试中一些考点和技巧做出总结**

1. 考试的题目还是很基础的，进行一些转换，然后把结果输出出来；
2. 先熟读题目，再进行操作，不要卡在一道题上浪费时间，要是一时某道题做不出来，可以先做别的题，如果是9道题的话，能做过7道基本上就合格了；
3. Sqoop的导入和导出必考，要熟悉Sqoop的压缩和保存的文件格式；
4. 给的数据文件基本上都是textFile，所以要熟悉RDD和DF的转换以及各种操作 ，有特定分隔符的textFile可以用`spark.read.option("delimiter","###").csv(path)`的方式进行读取（###换成指定的分隔符，\n或者逗号），如需指定特定的列名的话，还可以`toDF("columnName1","columnName2")`的方式去指定列名
5. 文件要求保存成特定分隔符的textFile的话，可以 `df.rdd.map(_.toSeq.map(_+"").reduce(_+"###"+_)).saveAsTextFile(path)`（###换成指定的分隔符，\n或者逗号）
6. 熟悉Hive外部表的创建和Hive表数据的导入；
7. 写的代码改一改可以供后面的题目复用，所以可以先用文本编辑器把写的代码保存一下；
8. 考试环境屏幕很小，环境也比较卡，如遇到操作不了的情况，可以点击刷新按钮，刷新考试环境，刷新后会快一些。

## **NO.1** CORRECT TEXT

Problem Scenario 49 : You have been given below code snippet (do a sum of values by key}, with
intermediate output.

```
val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C",
"bar=D", "bar=D")
val data = sc.parallelize(keysWithValuesList)
//Create key value pairs
val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
val initialCount = 0;
val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
```

Now define two functions (addToCounts, sumPartitionCounts) such, which will produce following
results.
Output 1:

```
countByKey.collect
res3: Array[(String, Int)] = Array((foo,5), (bar,3))
```

```
import scala.collection._
val initialSet = scala.collection.mutable.HashSet.empty[String]
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
```

Now define two functions (addToSet, mergePartitionSets) such, which will produce following results.
Output 2: 

```
uniqueByKey.collect
res4: Array[(String, scala.collection.mutable.HashSet[String])] = Array((foo,Set(B, A}}, (bar,Set(C, D)))
```

**Answer:**

See the explanation for Step by Step Solution and configuration. Explanation:

```
val addToCounts = (n: Int, v: String) => n + 1
val sumPartitionCounts = (p1: Int, p2: Int} => p1 + p2
val addToSet = (s: mutable.HashSet[String], v: String) => s += v
val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
```

## NO.2 CORRECT TEXT

Problem Scenario 81 : You have been given MySQL DB with following details. You have been given
following product.csv file 

product.csv 
productID,productCode,name,quantity,price

```
1001,PEN,Pen Red,5000,1.23
1002,PEN,Pen Blue,8000,1.25
1003,PEN,Pen Black,2000,1.25
1004,PEC,Pencil 2B,10000,0.48
1005,PEC,Pencil 2H,8000,0.49
1006,PEC,Pencil HB,0,9999.99
```

Now accomplish following activities.
1 . Create a Hive ORC table using SparkSql
2 . Load this data in Hive table.
3 . Create a Hive parquet table using SparkSQL and load data in it.

**Answer:**

```
val products = sc.textFile("/user/cuiyufei/cca/product.csv") 

val products= sc.textFile("hdfs://localhost:9000/user/cuiyufei/exam/products.csv")
products.first()

case class Product(productid: Integer, code: String, name: String, quantity:Integer, price: Float)

val prdRDD = products.map(_.split(",")).map(p => Product(p(0).toInt,p(1),p(2),p(3).toInt,p(4).toFloat))
prdRDD.first()
prdRDD.count()

val prdDF = prdRDD.toDF()

import org.apache.spark.sql.SaveMode prdDF.write.mode(SaveMode.Overwrite).format("orc").saveAsTable("product_orc_table") 

//hive
show tables
CREATE EXTERNAL TABLE products (productid int, code string, name string, quantity int, price float) STORED AS orc LOCATION '/user/hive/warehouse/product_orc_table';
//Step 8 : Now create a parquet table import org.apache.spark.sql.SaveMode
prdDF.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("product_parquet_table") 

CREATE EXTERNAL TABLE products_parquet (productid int,code string,name string, quantity int, price float) STORED AS parquet LOCATION '/user/hive/warehouse/product_parquet_table';

Select * from products;
Select * from products_parquet;
```

## NO.3 CORRECT TEXT

Problem Scenario 84 : In Continuation of previous question, please accomplish following activities.

1. Select all the products which has product code as null
2. Select all the products, whose name starts with Pen and results should be order by Price descending order.
3. Select all the products, whose name starts with Pen and results should be order by
   Price descending order and quantity ascending order.
4. Select top 2 products by price

**Answer:**
See the explanation for Step by Step Solution and configuration. Explanation:
Solution :
Step 1 : Select all the products which has product code as null

```
val results = sqlContext.sql("SELECT * FROM products WHERE code IS NULL") results.show()
```

Step 2 : Select all the products , whose name starts with Pen and results should be order by Price
descending order. 

```
val results = sqlContext.sql("SELECT * FROM products WHERE name LIKE 'Pen %' ORDER BY price DESC")
results.show()
```

Step 3 : Select all the products , whose name starts with Pen and results should be order by Price descending order and quantity ascending order. 

```
val results = sqlContext.sql("SELECT * FROM products WHERE name LIKE 'Pen %' ORDER BY price DESC, quantity") 
results.show()
```

Step 4 : Select top 2 products by price

```
val results = sqlContext.sql("SELECT * FROM products ORDER BY price desc LIMIT2")
results. show()
```

## NO.4 CORRECT TEXT

Problem Scenario 4: You have been given MySQL DB with following details. 

```
user=retail_dba
password=cloudera 
database=retail_db 
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.
Import Single table categories (Subset data} to hive managed table , where category_id between 1 and 22
**Answer:**
See the explanation for Step by Step Solution and configuration. Explanation:
Solution :
Step 1 : Import Single table (Subset data)

```
sqoop import --connect=jdbc:mysql://quickstart:3306/retail_db --username=retail_dba --password=cloudera --table=categories --where 'category_id between 1 and 22' --hive-import -m 1
```

 Note: Here the ' is the same you find on ~ key
This command will create a managed table and content will be created in the following directory. /user/hive/warehouse/categories
Step 2 : Check whether table is created or not (In Hive) show tables;

```
select * from categories;
```

## NO.5 CORRECT TEXT

Problem Scenario 13 : You have been given following mysql database details as well as other info.

```
user=retail_dba
password=cloudera 
database=retail_db 
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Create a table in retailedb with following definition.
     CREATE table departments_export (department_id int(11), department_name varchar(45), created_date T1MESTAMP DEFAULT NOW());
2. Now import the data from following directory into departments_export table, /user/cloudera/departments_new

**Answer:**

```
#mysql
mysql -uretail_dba -pcloudera
use retail_db;show tables;
CREATE table departments_export (department_id int(11), department_name varchar(45), created_date T1MESTAMP DEFAULT NOW());
```

```
sqoop export --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --export-dir /user/cloudera/departments_new --batch
```

## NO.6 CORRECT TEXT

Problem Scenario 96 : Your spark application required extra Java options as below. -
XX:+PrintGCDetails-XX:+PrintGCTimeStamps
Please replace the XXX values correctly
./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=talse - -conf XXX hadoopexam.jar

**Answer:**

See the explanation for Step by Step Solution and configuration. Explanation:
Solution

```
XXX: spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" Notes: ./bin/spark-submit \

--class <maln-class>
--master <master-url> \ --deploy-mode <deploy-mode> \
-conf <key>=<value> \ #other options
< application-jar> \ [application-arguments]
Here, conf is used to pass the Spark related contigs which are required for the application to run like any specific property(executor memory) or if you want to override the default property which is set in Spark-default.conf.
```

解决方式：

```
spark-shell -h
```

```
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
--conf spark.eventLog.enabled=false
```

## NO.7 CORRECT TEXT

Problem Scenario 35 : You have been given a file named spark7/EmployeeName.csv
(id,name). 

EmployeeName.csv 

```
E01,Lokesh
E02,Bhupesh
E03,Amit
E04,Ratan
E05,Dinesh
E06,Pavan
E07,Tejas
E08,Sheela
E09,Kumar
E10,Venkat
```

1. Load this file from hdfs and sort it by name and save it back as (id,name) in results directory. However, make sure while saving it should be able to write In a single file.

**Answer:**

```
val employees=sc.textFile("/user/cuiyufei/cca/EmployeeName.csv")
val employeesRDD=employees.map(v => (v.split(",")(0), v.split(",")(1)))
val employeesSwapRDD = employeesRDD.map(items => items.swap)
val employeesResultsRDD = employeesSwapRDD.sortByKey().map(items => items.swap)
employeesResultsRDD.repartition(1).saveAsTextFile('/user/cuiyufei/spark7/result.txt')
```

## **NO.8** CORRECT TEXT

Problem Scenario 89 : You have been given below patient data in csv format,
patientID,name,dateOfBirth,lastVisitDate 

```
1001,Ah Teck,1991-12-31,2012-01-20
1002,Kumar,2011-10-29,2012-09-20
1003,Ali,2011-01-30,2012-10-21
```

 Accomplish following activities.
1 . Find all the patients whose lastVisitDate between current time and '2012-09-15'
2 . Find all the patients who born in 2011
3 . Find all the patients age
4 . List patients whose last visited more than 60 days ago
5 . Select patients 18 years old or younger

## NO.9 CORRECT TEXT

Problem Scenario 40 : You have been given sample data as below in a file called spark15/file1.txt

```
3070811,1963,1096,,"US","CA",,1,
3022811,1963,1096,,"US","CA",,1,56
3033811,1963,1096,,"US","CA",,1,23
```

Below is the code snippet to process this tile.

```
val field=sc.textFile("/user/cuiyufei/cca/spark15/file1.txt")
val mapper = field.map(x=> A)
mapper.map(x => x.map(x=> {B})).collect
```

Please fill in A and B so it can generate below final output 

```
Array(Array(3070811,1963,109G, 0, "US", "CA", 0,1, 0) ,Array(3022811,1963,1096, 0, "US", "CA", 0,1, 56) ,Array(3033811,1963,1096, 0, "US", "CA", 0,1, 23)
)
```

**Answer:**

```
A:x.split(",", -1)
B:if (x.isEmpty) 0 else x
这里(或{都可以
```



## NO.10 CORRECT TEXT

Problem Scenario 46 : You have been given belwo list in scala (name,sex,cost) for each work done. 

```
List(("Deeapak", "male", 4000), ("Deepak", "male", 2000), ("Deepika", "female", 2000), ("Deepak", "female", 2000), ("Deepak", "male", 1000), ("Neeta", "female", 2000))
```

Now write a Spark program to load this list as an RDD and do the sum of cost for combination of name and sex (as key)

**Answer:**

```
val rdd = sc.parallelize(List(("Deeapak", "male", 4000), ("Deepak", "male", 2000), ("Deepika", "female", 2000), ("Deepak", "female", 2000), ("Deepak", "male", 1000), ("Neeta" , "female", 2000)))
val byKey = rdd.map({case (name,sex,cost) => (name,sex)->cost})
//val byKey = rdd.map({case (name,sex,cost) => ((name,sex), cost)})
val byKeyGrouped = byKey.groupByKey
val result = byKeyGrouped.map{case ((id1,id2),values) => (id1,id2,values.sum)} 
//val result = byKeyGrouped.map({case ((id1,id2),values) => (id1,id2,values.sum)})
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark12/result.txt")
```

## NO.11 CORRECT TEXT

Problem Scenario 79 : You have been given MySQL DB with following details. 

```
user=retail_dba
password=cloudera 
database=retail_db 
table=retail_db.orders 
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Columns of products table : (product_id | product categoryid | product_name | product_description | product_price | product_image )
Please accomplish following activities.
1 . Copy "retaildb.products" table to hdfs in a directory p93_products
2 . Filter out all the empty prices
3 . Sort all the products based on price in both ascending as well as descending order.
4 . Sort all the products based on price as well as product_id in descending order.
5 . Use the below functions to do data ordering or ranking and fetch top 10 elements top() takeOrdered() sortByKey()

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username=retail_dba --password cloudera --table products --target-dir /user/cuiyufei/cca/p93_products -m 1
```

```
hdfs dfs -cat /user/cuiyufei/cca/p93_products/part-m-00000
9MapReduce framework to copy data from RDBMS to hdfs
Step 2 : Step 2 : Read the data from one of the partition, created using above command, hadoop fs -
cat p93_products/part-m-00000
Step 3 : Load this directory as RDD using Spark and Python (Open pyspark terminal and do following). productsRDD = sc.textFile("p93_products")
Step 4 : Filter empty prices, if exists
#filter out empty prices lines
nonemptyjines = productsRDD.filter(lambda x: len(x.split(",")[4]) > 0)
Step 5 : Now sort data based on product_price in order.
sortedPriceProducts=nonempty_lines.map(lambdaline:(float(line.split(",")[4]),line.split(",")[2])).sortByKey()
for line in sortedPriceProducts.collect(): print(line)
Step 6 : Now sort data based on product_price in descending order. sortedPriceProducts=nonempty_lines.map(lambda line: (float(line.split(",")[4]),line.split(",")[2])).sortByKey(False)
for line in sortedPriceProducts.collect(): print(line) Step 7 : Get highest price products name.
sortedPriceProducts=nonemptyJines.map(lambda line : (float(line.split(",")[4]),line- split(,,,,,)[2])) -sortByKey(False).take(1) print(sortedPriceProducts)
Step 8 : Now sort data based on product_price as well as product_id in descending order. #Dont forget to cast string #Tuple as key ((price,id),name) sortedPriceProducts=nonemptyJines.map(lambda line : ((float(line print(sortedPriceProducts)
Step 9 : Now sort data based on product_price as well as product_id in descending order, using top()
function.
#Dont forget to cast string #Tuple as key ((price,id),name)
sortedPriceProducts=nonemptyJines.map(lambda line: ((float(line.s^^
print(sortedPriceProducts)
Step 10 : Now sort data based on product_price as ascending and product_id in ascending order,
using takeOrdered{) function. #Dont forget to cast string
#Tuple as key ((price,id),name) sortedPriceProducts=nonemptyJines.map(lambda line: ((float(line.split(","}[4]},int(line.split(","}[0]}},line.split(","}[2]}}.takeOrdered(10, lambda tuple : (tuple[0][0],tuple[0][1]))
Step 11 : Now sort data based on product_price as descending and product_id in ascending order, using takeOrdered() function.
#Dont forget to cast string
#Tuple as key ((price,id},name)
#Using minus(-) parameter can help you to make descending ordering , only for numeric value. sortedPrlceProducts=nonemptylines.map(lambda line: ((float(line.split(","}[4]},int(line.split(","}[0]}},line.split(","}[2]}}.takeOrdered(10, lambda tuple : (-tuple[0][0],tuple[0][1]}}

```



## NO.12 CORRECT TEXT

Problem Scenario 69 : Write down a Spark Application using Python, In which it read a file "Content.txt" (On hdfs) with following content.
And filter out the word which is less than 2 characters and ignore all empty lines. Once doen store the filtered data in a directory called "problem84" (On hdfs) Content.txt
Hello this is ABCTECH.com This is ABYTECH.com Apache Spark Training
This is Spark Learning Session Spark is faster than MapReduce 

```
#conf = SparkConf().setAppName("CCA 175 Problem 84") 
#sc = sparkContext(conf=conf)
contentRDD = sc.textFile("/user/cuiyufei/cca/Content.txt")
nonempty_lines = contentRDD.filter(lambda x: len(x) > 0)
finalRDD = nonempty_lines.flatMap(lambda x: x.split(" ")).filter(lambda x:len(x)>2)
finalRDD.saveAsTextFile("/user/cuiyufei/cca/problem84") 
#step 2 : Submit this application
#spark-submit -master yarn problem84.py
#finalRDD.collect()
```

## NO.13 CORRECT TEXT

Problem Scenario 73 : You have been given data in json format as below.

```
{"first_name":"Ankit", "last_name":"Jain"}
{"first_name":"Amir", "last_name":"Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh", "last_name":"Yadav"}
```

 Do the following activity

1. create employee.json file locally.
2. Load this file on hdfs
3. Register this data as a temp table in Spark using Python.
4. Write select query and print this data.
5. Now save back this selected data in json format.

**Answer:**

```
from pyspark import SQLContext
sqIContext = SQLContext(sc)
#employee = sqlContext.jsonFile("/user/cuiyufei/cca/employee.json")
employee = sqlContext.read.json("/user/cuiyufei/cca/employee.json")
employee.registerTempTable("EmployeeTab")
employeelnfo = sqlContext.sql("select * from EmployeeTab")
for row in employeelnfo.collect():
	print(row)
employeelnfo.toJSON().saveAsTextFile("/user/cuiyufei/cca/employeeJson1")
```

## NO.14 CORRECT TEXT

Problem Scenario 68 : You have given a file as below. spark75/file1.txt
File contain some text. As given Below
*spark75/file1.txt*

```
Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures are common and should be automatically handled by the framework.
The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File System (HDFS) and a processing part called MapReduce. Hadoop splits files into large blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers packaged code for nodes to process in parallel based on the data that needs to be processed.
his approach takes advantage of data locality nodes manipulating the data they have access to to allow the dataset to be processed faster and more efficiently than it would be in a more conventional supercomputer architecture that relies on a parallel file system where computation and data are distributed via high-speed networking.
For a slightly more complicated task, lets look into splitting up sentences from our documents into word bigrams. A bigram is pair of successive tokens in some sequence.
We will look at building bigrams from the sequences of words in each sentence, and then try to find the most frequently occuring ones.
```

The first problem is that values in each partition of our initial RDD describe lines from the file rather than sentences. Sentences may be split over multiple lines. The glom() RDD method is used to create a single entry for each document containing the list of all lines, we can then join the lines up, then resplit them into sentences using "." as the separator, using flatMap so that every object in our RDD is now a sentence.
A bigram is pair of successive tokens in some sequence. Please build bigrams from the sequences of words in each sentence, and then try to find the most frequently occuring ones.

```
sentences = sc.textFile("/user/cuiyufei/cca/spark75/file1.txt").glom().map(lambda x: " ".join(x)).flatMap(lambda x: x.split("."))
bigrams = sentences.map(lambda x:x.split()).flatMap(lambda x: [((x[i],x[i+1]),1) for i in range(0,len(x)-1)])
freq_bigrams = bigrams.reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False) 
freq_bigrams.take(10)
```

## NO.15 CORRECT TEXT

Problem Scenario 21 : You have been given log generating service as below. startjogs (It will generate continuous logs)
tailjogs (You can check , what logs are being generated) stopjogs (It will stop the log service)
Path where logs are generated using above service : /opt/gen_logs/logs/access.log
Now write a flume configuration file named flumel.conf , using that configuration file dumps logs in HDFS file system in a directory called flumel. Flume channel should have following property as well. After every 100 message it should be committed, use non-durable/faster channel and it should be able to hold maximum 1000 events
Solution :
Step 1 : Create flume configuration file, with below configuration for source, sink and channel. #Define source , sink , channel and agent,
agent1 .sources = source1 agent1 .sinks = sink1 agent1.channels = channel1 #Describe/configure source1
agent1 .sources.source1.type = exec
agent1.sources.source1.command = tail -F /opt/gen logs/logs/access.log ##Describe sinkl
agentl .sinks.sinkl.channel = memory-channel agentl .sinks.sinkl .type = hdfs
agentl .sinks.sink1.hdfs.path = flumel
agentl .sinks.sinkl.hdfs.fileType = Data Stream #Now we need to define channell property. agent1.channels.channel1.type = memory agent1.channels.channell.capacity = 1000
agent1.channels.channell.transactionCapacity = 100 #Bind the source and sink to the channel agent1.sources.source1.channels = channel1 agent1.sinks.sink1.channel = channel1
Step 2 : Run below command which will use this configuration file and append data in hdfs. Start log service using : startjogs
Start flume service:
flume-ng agent -conf /home/cloudera/flumeconf -conf-file /home/cloudera/flumeconf/flumel.conf-Dflume.root.logger=DEBUG,INFO,console Wait for few mins and than stop log service.
Stop_logs
Answer:
See the explanation for Step by Step Solution and configuration.

## NO.16 CORRECT TEXT

Problem Scenario 88 : You have been given below three files product.csv (Create this file in hdfs) productID,productCode,name,quantity,price,supplierid 

```
1001,PEN,Pen Red,5000,1.23,501
1002,PEN,Pen Blue,8000,1.25,501
1003,PEN,Pen Black,2000,1.25,501
1004,PEC,Pencil 2B,10000,0.48,502
1005,PEC,Pencil 2H,8000,0.49,502
1006,PEC,Pencil HB,0,9999.99,502
2001,PEC,Pencil 3B,500,0.52,501
2002,PEC,Pencil 4B,200,0.62,501
2003,PEC,Pencil 5B,100,0.73,501
2004,PEC,Pencil 6B,500,0.47,502
```

supplier.csv
supplierid,name,phone 

```
501,ABC Traders,88881111
502,XYZ Company,88882222
503,QQ Corp,88883333
```

products_suppliers.csv 

productID,supplierID 

```
2001,501
2002,501
2003,501
2004,502
2001,503
```

Now accomplish all the queries given in solution.

1. It is possible that, same product can be supplied by multiple supplier. Now find each product, its price according to each supplier.
2. Find all the supllier name, who are supplying 'Pencil 3B'
3. Find all the products , which are supplied by ABC Traders.

**Answer:**

```
case class product(productID: Int, productCode:String, name:String, quantity:Float, price:Float, supplierid:Int)
case class supplier(supplierid:Int, name:String, phone:String)
case class product_supplier(productID:Int, supplierID:Int)
val products=sc.textFile("/user/cuiyufei/cca/spark88/product.csv").map(_.split(",")).map(p => product(p(0).toInt, p(1), p(2), p(3).toFloat, p(4).toFloat, p(5).toInt)).toDF()
products.registerTempTable("products")
val suppliers=sc.textFile("/user/cuiyufei/cca/spark88/supplier.csv").map(_.split(",")).map(p => supplier(p(0).toInt, p(1), p(2))).toDF()
suppliers.registerTempTable("supplier")
val products_suppliers=sc.textFile("/user/cuiyufei/cca/spark88/products_suppliers.csv").map(_.split(",")).map(p=>product_supplier(p(0).toInt, p(1).toInt)).toDF()
products_suppliers.registerTempTable("products_suppliers")
val results = sqlContext.sql("select a.name, b.name, b.price from supplier a left join products b on a.supplierid=b.supplierid")
val results_2=sqlContext.sql("select a.name from supplier a join products b on a.supplierid=b.supplierid and b.name='Pencil 3B'")
val results_3=sqlContext.sql("select a.name from products a left join supplier b on a.supplierid=b.supplierid")
```





## NO.17 CORRECT TEXT

Problem Scenario 85 : In Continuation of previous question, please accomplish following activities.

1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
2. Select code and name both separated by ' -' and header name should be Product Description'.
3. Select all distinct prices.
    4 . Select distinct price and name combination.
    5 . Select all price data sorted by both code and productID combination.
    6 . count number of products.
    7 . Count number of products for each code.

**Answer:**

```
val result_1=sqlContext.sql("select productID AS ID, code AS Code, name AS Description, price AS 'Unit Price' from products")
val result_2=sqlContext.sql("select concat(productCode, '-', name) as description_name from products")
val result_3 = sqlContext.sql("select distinct price from products")
val result_4 = sqlContext.sql("select distinct price, name from products")
val result_5 = sqlContext.sql("select price from products order by productCode, productID")
val result_6 = sqlContext.sql("select count(1) from products")
val result_7 = sqlContext.sql("select productCode, count(1) from products group by productCode")
```



## NO.18 CORRECT TEXT

Problem Scenario 38 : You have been given an RDD as below, val rdd: RDD[Array[Byte]]
Now you have to save this RDD as a SequenceFile. And below is the code snippet. import org.apache.hadoop.io.compress.GzipCodec
rdd.map(bytesArray => (A.get(), new B(bytesArray))).saveAsSequenceFile('7output/path",classOt[GzipCodec]) What would be the correct replacement for A and B in above snippet. Answer:
See the explanation for Step by Step Solution and configuration. Explanation:

**Answer:**

```
A. NullWritable
B. BytesWritable
```

## NO.19 CORRECT TEXT

Problem Scenario 59 : You have been given below code snippet.
val x = sc.parallelize(1 to 20)
val y = sc.parallelize(10 to 30) 
operationl 
z.collect
Write a correct code snippet for operationl which will produce desired output, shown below. Array[lnt] = Array(16,12, 20,13,17,14,18,10,19,15,11)

**Answer:**

```
val z = x.intersection(y)
```

## NO.20 CORRECT TEXT

Problem Scenario 48 : You have been given below Python code snippet, with intermediate output. We want to take a list of records about people and then we want to sum up their ages and count them.
So for this example the type in the RDD will be a Dictionary in the format of {name: NAME, age:AGE,
gender:GENDER}.
The result type will be a tuple that looks like so (Sum of Ages, Count) people = [] 

```
people.append({'name':'Amit', 'age':45,'gender':'M'})
people.append({'name':'Ganga', 'age':43,'gender':'F'})
people.append({'name':'John', 'age':28,'gender':'M'})
people.append({'name':'Lolita', 'age':33,'gender':'F'})
people.append({'name':'Dont Know', 'age':18,'gender':'T'})
peopleRdd=sc.parallelize(people) //Create an RDD
peopleRdd.aggregate((0,0), seqOp, combOp) //Output of above line : 167, 5)
```

 Now define two operation seqOp and combOp , such that
seqOp : Sum the age of all people as well count them, in each partition. 
combOp : Combine results from all partitions.

**Answer:**

需要首先判断是scala还是python，在根据要求去分析。

```
seqOp:(lambda x,y:(x[0]+y['age'],x[1]+1))
combOp:(lambda x,y:(x[0]+y[0], x[1]+y[1]))
```

## NO.21 CORRECT TEXT

Problem Scenario 10 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```
Please accomplish following.
1. Create a database named hadoopexam and then create a table named departments in it, with following fields. department_id int, department_name string e.g. location should be hdfs://quickstart.cloudera:8020/user/hive/warehouse/hadoopexam.db/departments
2. Please import data in existing table created above from retaidb.departments into hive table
    hadoopexam.departments.
3. Please import data in a non-existing table, means while importing create hive table named hadoopexam.departments_new

**Answer:**

```
#hive
create database hadoopexam;
use hadoopexam;
create table departments(department_id int, department_name string);
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --hive-overwrite --hive-home /user/hive/warehouse --hive-import --hive-table hadoopexam.departments
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --hive-overwrite --hive-home /user/hive/warehouse --hive-import --hive-table hadoopexam.departments_new --create-hive-table
```



## NO.22 CORRECT TEXT

Problem Scenario 31 : You have given following two files
1 . Content.txt: Contain a huge text file containing space separated words.
2 . Remove.txt: Ignore/filter all the words given in this file (Comma Separated).
Write a Spark program which reads the Content.txt file and load as an RDD, remove all the words
from a broadcast variables (which is loaded as an RDD of words from Remove.txt). And count the occurrence of the each word and save it as a text file in HDFS. 

Content.txt

```
Hello this is ABCTech.com
This is TechABY.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce
```

Remove.txt
Hello, is, this, the
**Answer:**

```
val content = sc.textFile("/user/cuiyufei/cca/spark31/Content.txt")
val contentRDD = content.flatMap(_.split(" "))
val remove = sc.textFile("/user/cuiyufei/cca/spark31/Remove.txt")
val removeRDD = remove.flatMap(line => line.split(",")).map(line => line.trim)
val bRemove = sc.broadcast(removeRDD.collect.toList)
val filterRDD = content.filter({case(word) => !bRemove.value.contains(word)})
val result = filterRDD.map(word => (word, 1)).reduceByKey(_ + _)
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/result31")
```

**val bRemove = sc.broadcast(removeRDD.collect.toList)**

## NO.23 CORRECT TEXT

Problem Scenario 6 : You have been given following mysql database details as well as other info.

```
user=retail_dba 
password=cloudera 
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```
Compression Codec : org.apache.hadoop.io.compress.SnappyCodec Please accomplish following.
1. Import entire database such that it can be used as a hive tables, it must be created in default
     schema.
2. Also make sure each tables file is partitioned in 3 files e.g. part-00000, part-00002, part-
     00003
3. Store all the Java files in a directory called java_output to evalute the further

**Answer:**

```
sqoop import-all-tables --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera -m 3 --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-import  --hive-overwrite --create-hive-table --outdir java_output
```



## NO.24 CORRECT TEXT

Problem Scenario 60 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"}, 3} 
val b = a.keyBy(_.length) 
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","woif","bear","bee"), 3) 
val d = c.keyBy(_.length) 
operation1
```
Write a correct code snippet for operationl which will produce desired output, shown below. 
```
Array[(lnt, (String, String))] = Array((6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)),
(6,(salmon,turkey)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)), (3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))
```
Answer:

```
b.join(d).collect
```

## NO.25 CORRECT TEXT

Problem Scenario 17 : You have been given following mysql database details as well as other info.

```
user=retail_dba 
password=cloudera 
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```
Please accomplish below assignment.
1. Create a table in hive as below, create table departments_hiveOl(department_id int,
    department_name string, avg_salary int);
2. Create another table in mysql using below statement CREATE TABLE IF NOT EXISTS
    departments_hive01(id int, department_name varchar(45), avg_salary int);
3. Copy all the data from departments table to departments_hive01 using insert into departments_hive01 select a.*, null from departments a;
    Also insert following records as below
    insert into departments_hive01 values(777, "Not known",1000);
    insert into departments_hive01 values(8888, null,1000); insert into departments_hive01 values(666, null,1100);
4. Now import data from mysql table departments_hive01 to this hive table. Please make sure that data should be visible using below hive command. Also, while importing if null value found for department_name column replace it with "" (empty string) and for id column with -999 select * from departments_hive;

**Answer:**

```
#hive
create table departments_hive01(department_id int, department_name string, avg_salary int);
#mysql
create table if not exists departments_hive01(id int, department_name varchar(45), avg_salary int);
insert into departments_hive01 select a.*, null from departments a;
insert into departments_hive01 values(777, "Not known", 1000);
insert into departments_hive01 values(8888, null, 1000);
insert into departments_hive01 values(666, null, 1100);
#sqoop
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments_hive01 --hive-home /user/hive/warehouse --hive-import --hive-table departments_hive01 --fields-terminated-by '\001' --null-string "" --null-non-string "-999"  --split-by "id" -m 1
```



## NO.26 CORRECT TEXT

Problem Scenario 14 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```
Please accomplish following activities.
1. Create a csv file named updated_departments.csv with the following contents in local file system.
    updated_departments.csv

    ```
    2,fitness
    3,footwear
    12,fathematics
    13,fcience
    14,engineering
    1000,management
    ```

2. Upload this csv file to hdfs filesystem,

3. Now export this data from hdfs to mysql retaildb.departments table. During upload make sure
    existing department will just updated and new departments needs to be inserted.

4. Now update updated_departments.csv file with below content.

    ```
    2,Fitness
    3,Footwear
    12,Fathematics
    13,Science
    14,Engineering
    1000,Management
    2000,Quality Check
    ```

5. Now upload this file to hdfs.

6. Now export this data from hdfs to mysql retail_db.departments table. During upload make sure existing department will just updated and no new departments needs to be inserted.

**Answer:**

```
vi updated_departments.csv
sqoop export --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --export-dir /user/cuiyufei/cca/updated_departments.csv --batch -m 1 --update-key 'department_id' --update-mode allowinsert

sqoop export --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --export-dir /user/cuiyufei/cca/updated_departments01.csv --batch -m 1 --update-key 'department_id' --update-mode updateonly
```

```
allowinsert
updateonly
```



## NO.27 CORRECT TEXT

Problem Scenario 39 : You have been given two files spark16/file1.txt

```
1,9,5
2,7,4
3,8,3
```

spark16/file2.txt

```
1,g,h
2,i,j
3,k,l
```

Load these two tiles as Spark RDD and join them to produce the below results

```
(l, ((9,5), (g,h)))
(2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
```

And write code snippet which will sum the second columns of above joined results (5+4+3).

**Answer:**

```
val a = sc.textFile("/user/cuiyufei/cca/spark16/file1.txt").map(_.split(","))
val one = a.map(line =>(line(0),(line(1),line(2))))
#a.map{case Array(a,b,c) => (a,(b,c))}
#val one = sc.textFile("/user/cuiyufei/cca/spark16/file1.txt").map(_.split(",",-1) match {case Array(a, b, c) => (a, ( b, c)) })
val b = sc.textFile("/user/cuiyufei/cca/spark16/file2.txt").map(_.split(","))
val two = b.map(line =>(line(0),(line(1),line(2))))
val joined = one.join(two)
val result = joined.map{case(_, ((_,num2),(_,_)))=>num2.toInt}.reduce(_ + _)

val one = sc.textFile("/user/cuiyufei/cca/spark16/file1.txt").map(_.split(",",-1) match {case Array(a, b, c) => (a, ( b, c)) }
```

## NO.28 CORRECT TEXT

Problem Scenario 22 : You have been given below comma separated employee information. name,salary,sex,age

```
alok,100000,male,29
jatin,105000,male,32
yogesh,134000,male,39
ragini,112000,female,35
jyotsana,129000,female,39
valmiki,123000,male,29
```

Use the netcat service on port 44444, and nc above data line by line. Please do the following
activities.

1. Create a flume conf file using fastest channel, which write data in hive warehouse directory, in a table called flumeemployee (Create hive table as well tor given data).
2. Write a hive query to read average salary of all employees.

**Answer:**

```

```

## NO.29 CORRECT TEXT

Problem Scenario 83 : In Continuation of previous question, please accomplish following activities.

1. Select all the records with quantity >= 5000 and name starts with 'Pen'
2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with
    'Pen'
3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen'
4. Select all the products which name is 'Pen Red', 'Pen Black'
5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.

**Answer:**



## NO.30 CORRECT TEXT

Problem Scenario 28 : You need to implement near real time solutions for collecting information
when submitted in file with below Data
echo "IBM,100,20160104" >> /tmp/spooldir2/.bb.txt echo "IBM,103,20160105" >> /tmp/spooldir2/.bb.txt mv /tmp/spooldir2/.bb.txt /tmp/spooldir2/bb.txt After few mins
echo "IBM,100.2,20160104" >> /tmp/spooldir2/.dr.txt echo "IBM,103.1,20160105" >> /tmp/spooldir2/.dr.txt mv /tmp/spooldir2/.dr.txt /tmp/spooldir2/dr.txt
You have been given below directory location (if not available than create it) /tmp/spooldir2
.
As soon as file committed in this directory that needs to be available in hdfs in /tmp/flume/primary as well as /tmp/flume/secondary location.
However, note that/tmp/flume/secondary is optional, if transaction failed which writes in this
directory need not to be rollback.
Write a flume configuration file named flumeS.conf and use it to load data in hdfs with following additional properties .
1 . Spool /tmp/spooldir2 directory
2 . File prefix in hdfs sholuld be events
3 . File suffix should be .log
4 . If file is not committed and in use than it should have _ as prefix.
5 . Data should be written as text to hdfs
**Answer:**

## NO.31 CORRECT TEXT

Problem Scenario 26 : You need to implement near real time solutions for collecting information when submitted in file with below information. You have been given below directory location (if not available than create it) /tmp/nrtcontent. Assume your departments upstream service is continuously committing data in this directory as a new file (not stream of data, because it is near real time solution). As soon as file committed in this directory that needs to be available in hdfs in /tmp/flume location
Data
echo "I am preparing for CCA175 from ABCTECH.com" > /tmp/nrtcontent/.he1.txt mv
/tmp/nrtcontent/.he1.txt /tmp/nrtcontent/he1.txt After few mins
echo "I am preparing for CCA175 from TopTech.com" > /tmp/nrtcontent/.qt1.txt mv /tmp/nrtcontent/.qt1.txt /tmp/nrtcontent/qt1.txt
Write a flume configuration file named flumes.conf and use it to load data in hdfs with following additional properties.
1 . Spool /tmp/nrtcontent
2 . File prefix in hdfs sholuld be events
3 . File suffix should be Jog
4 . If file is not commited and in use than it should have as prefix.
5 . Data should be written as text to hdfs
Answer:

## NO.32 CORRECT TEXT

Problem Scenario 61 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3) 
val d = c.keyBy(_.length) 
operationl
```

Write a correct code snippet for operationl which will produce desired output, shown below. 

```
Array[(lnt, (String, Option[String]}}] = Array((6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(dog,Some(dog))), (3,(dog,Some(bee))), (3,(rat,Some(dogg)), (3,(rat,Some(cat)j), (3,(rat.Some(gnu))). (3,(rat,Some(bee))), (8,(elephant,None)))
```

**Answer:**

```
b.leftOuterJoin(d).collect
```



## NO.33 CORRECT TEXT

Problem Scenario 90 : You have been given below two files

```
course.txt
id,course
1,Hadoop
2,Spark
3,HBase
```

```
fee.txt
id,fee
2,3900
3,4200
4,2900
```

Accomplish the following activities.

1. Select all the courses and their fees , whether fee is listed or not.
2. Select all the available fees and respective course. If course does not exists still list the fee
3. Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null.

**Answer:**

```
case class course(id:Int, course:String)
case class fee(id:Int, fee:Float)
val courses = sc.textFile("/user/cuiyufei/cca/course.txt")
val fees = sc.textFile("/user/cuiyufei/cca/fee.txt")
val feesDF=fees.map(_.split(",")).map(p => fee(p(0).toInt, p(1).toFloat)).toDF()
val coursesDF= courses.map(_.split(",")).map(p => course(p(0).toInt, p(1))).toDF()
feesDF.registerTempTable("fee")
coursesDF.registerTempTable("course")
val result_1 = sqlContext.sql("select a.course,b.fee from course a left join fee b on a.id=b.id")
val result_2 = sqlContext.sql("select a.course, b.fee from course a right join fee b on a.id=b.id")
val result_3 = sqlContext.sql("select a.course, b.fee from course a left join fee b on a.id=b.id where b.fee is not null")
```



## NO.34 CORRECT TEXT

Problem Scenario 58 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
val b = a.keyBy(_.length)
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. 

```
Array[(lnt, Seq[String])] = Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)), (3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle)))
```

**Answer:**

```
b.groupByKey.collect
```

## NO.35 CORRECT TEXT

Problem Scenario 91 : You have been given data in json format as below. 

```
{"first_name":"Ankit", "last_name":"Jain"}
{"first_name":"Amir", "last_name":"Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh", "last_name":"Yadav"}
```

Do the following activity
1 . create employee.json tile locally.
2 . Load this tile on hdfs
3 . Register this data as a temp table in Spark using Python.
4 . Write select query and print this data.
5 . Now save back this selected data in json format.

**Answer:**

sqlContext:read.json

toJSON().saveAsTextFile()

```
from pyspark import SQLContext
sqIContext = SQLContext(sc)
#employee = sqlContext.jsonFile("/user/cuiyufei/cca/employee.json")
employee = sqlContext.read.json("/user/cuiyufei/cca/employee.json")
employee.registerTempTable("EmployeeTab")
employeelnfo = sqlContext.sql("select * from EmployeeTab")
for row in employeelnfo.collect():
	print(row)
employeelnfo.toJSON().saveAsTextFile("/user/cuiyufei/cca/employeeJson1")
```



## NO.36 CORRECT TEXT

Problem Scenario 24 : You have been given below comma separated employee information. 

Data Set:

```
name,salary,sex,age
alok,100000,male,29
jatin,105000,male,32
yogesh,134000,male,39
ragini,112000,female,35
jyotsana,129000,female,39
valmiki,123000,male,29
```

Requirements:
Use the netcat service on port 44444, and nc above data line by line. Please do the following
activities.

1. Create a flume conf file using fastest channel, which write data in hive warehouse directory, in a table called flumemaleemployee (Create hive table as well tor given data).
2. While importing, make sure only male employee data is stored.

## NO.37 CORRECT TEXT

Problem Scenario 16 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish below assignment.

1. Create a table in hive as below.
    create table departments_hive(department_id int, department_name string);
2. Now import data from mysql table departments to this hive table. Please make sure that data should be visible using below hive command, select" from departments_hive

**Answer:**

```
#hive
create table departments_hive(department_id int, department_name string);
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --hive-import --hive-home /user/hive/warehouse --hive-overwrite --fields-terminated-by '\001' --hive-table departments_hive -m 1
```

## NO.38 CORRECT TEXT

Problem Scenario 32 : You have given three files as below. `/user/cuiyufei/cca/spark32/file1.txt`
`/user/cuiyufei/cca/spark32/file2.txt` `/user/cuiyufei/cca/spark32/file3.txt` Each file contain some text. 

/user/cuiyufei/cca/spark32/file1.txt

```
Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures are common and should be automatically handled by the framework.
```

/user/cuiyufei/cca/spark32/file2.txt

```
The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File System (HDFS) and a processing part called MapReduce. Hadoop splits files into large blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers packaged code for nodes to process in parallel based on the data that needs to be processed. 
```

/user/cuiyufei/cca/spark32/file3.txt

```
his approach takes advantage of data locality nodes manipulating the data they have access to to allow the dataset to be processed faster and more efficiently than it would be in a more conventional supercomputer architecture that relies on a parallel file system where computation and data are distributed via high-speed networking.
```

Now write a Spark code in scala which will load all these three files from hdfs and do the word count by filtering following words. And result should be sorted by word count in reverse order.
Filter words ("a","the","an", "as", "a","with","this","these","is","are","in", "for", "to","and","The","of")
Also please make sure you load all three files as a Single RDD (All three files must be loaded using single API call).
You have also been given following codec
import org.apache.hadoop.io.compress.GzipCodec
Please use above codec to compress file, while saving in hdfs.

**Answer:**

```
val file = sc.textFile("/user/cuiyufei/cca/spark32/file1.txt, /user/cuiyufei/cca/spark32/file2.txt, /user/cuiyufei/cca/spark32/file3.txt")
val fileRDD = file.flatMap(_.split(" ")).map(line => line.trim)
val filterRDD = sc.parallelize(Array("a","the","an", "as", "a","with","this","these","is","are","in", "for", "to","and","The","of"))
val fileFilterRDD = fileRDD.subtract(filterRDD)
val result = fileFilterRDD.map(word => (word, 1)).reduceByKey(_ + _).map(line => line.swap).sortByKey(false).map(line => line.swap)
result.saveAsTextFile("/user/cuiyufei/cca/spark32/tar/", classOf[GzipCodec])
```

result.saveAsTextFile("/user/cuiyufei/cca/spark32/tar/", **classOf[GzipCodec]**)

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
2. Now sort the products data sorted by `product_price` per category, use `produc_tcategory_id` colunm to group by category

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table products --target-dir p93_products -m 1

```



## NO.40 CORRECT TEXT

Problem Scenario 37 : ABCTECH.com has done survey on their Exam Products feedback using a web
based form. With the following free text field as input in web ui.
Name: String Subscription Date: String Rating : String
And servey data has been saved in a file called spark9/feedback.txt

```
Christopher|Jan 11, 2015|5
Kapil|11 Jan, 2015|5
Thomas|6/17/2014|5
John|22-08-2013|5
Mithun|2013|5
Jitendra||5
```

Write a spark program using regular expression which will filter all the valid dates and save in two separate file (good record and bad record)

## NO.41 CORRECT TEXT

Problem Scenario 41 : You have been given below code snippet.

```
val au1 = sc.parallelize(List (("a", Array(1,2)), ("b", Array(1,2)))) 
val au2 = sc.parallelize(List (("a", Array(3)), ("b", Array(2))))
```

Apply the Spark method, which will generate below output.

```
Array[(String, Array[lnt])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a, (Array(3)), (b,Array(2)))
```

**Answer:**

```
au1.union(au2).collect
```

## NO.42 CORRECT TEXT

Problem Scenario GG : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2) 
val b = a.keyBy(_.length) 
val c = sc.parallelize(List("ant", "falcon", "squid"), 2) 
val d = c.keyBy(_.length) 
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below.

```
 Array[(lnt, String)] = Array((4,lion))
```

**Answer:**

```
b.subtractByKey(d).collect
```

## NO.43 CORRECT TEXT

Problem Scenario 67 : You have been given below code snippet.

```
lines = sc.parallelize(['lts fun to have fun,','but you have to know how.'])
r1 = lines.map( lambda x: x.replace(',',' ').replace('.',' ').lower())
r2 = r1.flatMap(lambda x:x.split())
r3 = r2.map(lambda x: (x, 1))
operation1
r5 = r4.map(lambda x:(x[1],x[0]))
r6 = r5.sortByKey(ascending=False)
r6.take(20)
```

Write a correct code snippet for operation1 which will produce desired output, shown below.

```
[(2, 'fun'), (2, 'to'), (2, 'have'), (1, its'), (1, 'know'), (1, 'how'), (1, 'you'), (1, 'but')]
```

**Answer:**

```
r4 = r2.reduceByKey(lambda x,y:x+y)
```

## NO.44 CORRECT TEXT

Problem Scenario 11 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Import departments table in a directory called departments.
2. Once import is done, please insert following 5 records in departments mysql table.
    Insert into departments(10, physics); Insert into departments(11, Chemistry); Insert into departments(12, Maths); Insert into departments(13, Science); Insert into departments(14, Engineering);
3. Now import only new inserted records and append to existring directory . which has been created in first step.

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments -target-dir /user/cloudera/departments -m 1
#mysql
Insert into departments values(10, 'physics');
Insert into departments values(11, 'Chemistry');
Insert into departments values(12, 'Maths');
Insert into departments values(13, 'Science');
Insert into departments values(14, 'Engineering');
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments -target-dir /user/cloudera/departments --check-column "department_id" --incremental append --last-value  1000 -m 1
```

## NO.45 CORRECT TEXT

Problem Scenario 70 : Write down a Spark Application using Python, In which it read a file
"Content.txt" (On hdfs) with following content. Do the word count and save the results in a directory
called "problem85" (On hdfs)
Content.txt

```
Hello this is ABCTECH.com
This is XYZTECH.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce
```

**Answer:**

```
content = sc.textFile("/user/cuiyufei/cca/Content.txt")
words = content.filter(lambda x: len(x) > 0).flatMap(lambda x:x.split(" "))
result = words.map(lambda x:(x, 1)).reduceByKey(lambda x,y: x+y)
result.repartition(1).saveAsTextFile("/user/cloudera/problem85")
```

```
#problem85.py
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("CCA 175 Problem 85")
sc = sparkContext(conf=conf)
content = sc.textFile("/user/cuiyufei/cca/Content.txt")
words = content.filter(lambda x: len(x) > 0).flatMap(lambda x:x.split(" "))
result = words.map(lambda x:(x, 1)).reduceByKey(lambda x,y: x+y)
result.repartition(1).saveAsTextFile("/user/cloudera/problem85")
```

spark-submit -master yarn problem85.py

## NO.46 CORRECT TEXT

Problem Scenario 92 : You have been given a spark scala application, which is bundled in jar named
hadoopexam.jar.
Your application class name is com.hadoopexam.MyTask
You want that while submitting your application should launch a driver on one of the cluster node.
Please complete the following command to submit the application.

```
spark-submit XXX --master yarn YYY SSPARK HOME/lib/hadoopexam.jar 10
```

**Answer：**

```
XXX:--class com.hadoopexam.MyTask
YYY:--deploy_mode cluster
```

--master yarn --deploy-mode

## NO.47 CORRECT TEXT

Problem Scenario 50 : You have been given below code snippet (calculating an average score}, with intermediate output.

```
type ScoreCollector = (Int, Double)
type PersonScores = (String, (Int, Double))
val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
val wilmaAndFredScores = sc.parallelize(initialScores).cache()
val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger) 
val averagingFunction = (personScore: PersonScores) => { val (name, (numberScores, totalScore)) = personScore (name, totalScore / numberScores)
}
val averageScores = scores.collectAsMap().map(averagingFunction)
Expected output: averageScores: scala.collection.Map[String,Double] = Map(Fred ->
91.33333333333333, Wilma -> 95.33333333333333)
```

Define all three required function , which are input for combineByKey method, e.g. (createScoreCombiner, scoreCombiner, scoreMerger). And help us producing required results. 

**Answer:**

```
val createScoreCombiner = (score: Double) => (1, score)
val scoreCombiner = (collector: ScoreCollector, score: Double) => {
    val (numberScores, totalScore) = collector (numberScores + 1, totalScore + score)
}
val scoreMerger= (collector1: ScoreCollector, collector2: ScoreCollector) => {
    val (numScores1, totalScore1) = collector1
    val (numScores2, totalScore2) = collector2
    (numScores1 + numScores2, totalScore1 + totalScore2)
}
```

## NO.48 CORRECT TEXT

Problem Scenario 20 : You have been given MySQL DB with following details.

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. Write a Sqoop Job which will import "retaildb.categories" table to hdfs, in a directory name "categories_targetJob".

**Answer:**

```
sqoop job --create sqoopjob -- import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --target-dir /user/cuiyufei/cca/categories_targetJob --fields-terminated-by '|' --lines-terminated-by '\n' -m 1
sqoop job --list
sqoop job --show sqoopjob
sqoop job --delete sqoopjob
sqoop job --exec sqoopjob(需要输入密码，mysql密码)
```

sqoop job --create sqoopjob -- import --connect 

## NO.49 CORRECT TEXT

Problem Scenario 94 : You have to run your Spark application on yarn with each executor 20GB and number of executors should be 50. Please replace XXX, YYY, ZZZ export HADOOP_CONF_DIR=XXX

```
./bin/spark-submit \
--class com.hadoopexam.MyTask \
xxx\
--deploy-mode cluster \ #can be client for client mode YYY\
ZZZ \
/path/to/hadoopexam.jar \
1 000
```

Answer:

```
XXX: --master yarn
YYY: --executor-memory 20G 
ZZZ: --num-executors 50
```

--executor-memory 50G 

--num-executors 50

## NO.50 CORRECT TEXT

Problem Scenario 53 : You have been given below code snippet.

```
val a = sc.parallelize(1 to 10, 3) operation1
b.collect
Output 1
Array[lnt] = Array(2, 4, 6, 8,10) operation2
Output 2
Array[lnt] = Array(1,2,3)
```

Write a correct code snippet for operation1 and operation2 which will produce desired output, shown above.

**Answer:**

```
val b = a.filter(p => p%2==0)
// val b= a.filter(_%2==0)
a.filter(p => p < 4).collect
//a.filter(_<4).collect
```

## NO.51 CORRECT TEXT

Problem Scenario 5 : You have been given following mysql database details. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. List all the tables using sqoop command from retail_db
2. Write simple sqoop eval command to check whether you have permission to read database tables or not.
3. Import all the tables as avro files in /user/hive/warehouse/retail cca174.db
4. Import departments table as a text file in /user/cloudera/departments.

**Answer:**

```

```

## NO.52 CORRECT TEXT

Problem Scenario 29 : Please accomplish the following exercises using HDFS command line options.

1. Create a directory in hdfs named hdfs_commands.
2. Create a file in hdfs named data.txt in hdfs_commands.
3. Now copy this data.txt file on local filesystem, however while copying file please make sure file properties are not changed e.g. file permissions.
4. Now create a file in local directory named data_local.txt and move this file to hdfs in hdfs_commands directory.
5. Create a file data_hdfs.txt in hdfs_commands directory and copy it to local file system.
6. Create a file in local filesystem named file1.txt and put it to hdfs

**Answer:**

```
#1
hdfs dfs -mkdir hdfs_commands

hdfs dfs -touchz hdfs_commands/data.txt

hdfs dfs -copyToLocal -p hdfs_commands/data.txt ./
#4
touch data_local.txt
hdfs dfs -moveFromLocal data_local.txt hdfs_commands
#5
hdfs dfs -touchz hdfs_commands/data_hdfs.txt
hdfs dfs -get hdfs_commands/data_hdfs.txt ./
#6
touch file1.txt
hdfs dfs -put file1.txt hdfs_commands
```

## NO.53 CORRECT TEXT

Problem Scenario 62 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x)) 
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. 

```
Array[(lnt, String)] = Array((3,xdogx), (5,xtigerx), (4,xlionx), (3,xcatx), (7,xpantherx), (5,xeaglex))
```

**Answer:**

```
b.mapValues(line => "x"+line+"x").collect
//b.mapValues("x" + _ + "x").collect
```

## NO.54 CORRECT TEXT

Problem Scenario 45 : You have been given 2 files , with the content as given Below (spark12/technology.txt)
(spark12/salary.txt) 

(spark12/technology.txt) 

first,last,technology

```
Amit,Jain,java
Lokesh,kumar,unix
Mithun,kale,spark
Rajni,vekat,hadoop
Rahul,Yadav,scala
```

 (spark12/salary.txt) 

first,last,salary

```
Amit,Jain,100000
Lokesh,kumar,95000
Mithun,kale,150000
Rajni,vekat,154000
Rahul,Yadav,120000
```

Write a Spark program, which will join the data based on first and last name and save the joined results in following format, first Last.technology.salary

**Answer:**

```
val technology = sc.textFile("/user/cuiyufei/cca/spark12/technology.txt")
val technologyRDD = technology.map(_.split(",")).map(line=> ((line(0), line(1)), line(2)))
val salary = sc.textFile("/user/cuiyufei/cca/spark12/salary.txt")
val salaryRDD = salary.map(_.split(",")).map(line=> ((line(0), line(1)), line(2)))
val joinRDD = technologyRDD.join(salaryRDD)
val result = joinRDD.map({case((first,last), (tec,salary))  => first+" " + last + "." + tec + "." + salary})
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark12_result")

```

## NO.55 CORRECT TEXT

Problem Scenario 63 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x))
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below.

```
Array[(Int, String}] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))
```

**Answer:**

```
b.reduceByKey(_ + _).collect
```

## NO.56 CORRECT TEXT

Problem Scenario 52 : You have been given below code snippet. 

```
val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
Operation_xyz
```

Write a correct code snippet for Operation_xyz which will produce below output. scala collection.

```
Map[Int,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> 6, 2 -> 3, 4 -> 2, 7 -> 1)
```

**Answer:**

```
b.countByValue
```

## NO.57 CORRECT TEXT

Problem Scenario 23 : You have been given log generating service as below. Start_logs (It will generate continuous logs)
Tail_logs (You can check , what logs are being generated)
Stop_logs (It will stop the log service)
Path where logs are generated using above service : /opt/gen_logs/logs/access.log
Now write a flume configuration file named flume3.conf , using that configuration file dumps logs in HDFS file system in a directory called flumeflume3/%Y/%m/%d/%H/%M
Means every minute new directory should be created). Please us the interceptors to provide
timestamp information, if message header does not have header info.
And also note that you have to preserve existing timestamp, if message contains it. Flume channel should have following property as well. After every 100 message it should be committed, use non- durable/faster channel and it should be able to hold maximum 1000 events.
Answer:

## NO.58 CORRECT TEXT

Problem Scenario 33 : You have given a files as below. spark5/EmployeeName.csv (id,name) spark5/EmployeeSalary.csv (id,salary)
Data is given below: 

EmployeeName.csv 

```
E01,Lokesh
E02,Bhupesh
E03,Amit
E04,Ratan
E05,Dinesh
E06,Pavan
E07,Tejas
E08,Sheela
E09,Kumar
E10,Venkat
```

EmployeeSalary.csv 

```
E01,50000
E02,50000
E03,45000
E04,45000
E05,50000
E06,45000
E07,50000
E08,10000
E09,10000
E10,10000
```

Now write a Spark code in scala which will load these two tiles from hdfs and join the same, and
produce the (name.salary) values.
And save the data in multiple file group by salary (Means each file will have name of employees with same salary). Make sure file name include salary as well.

**Answer:**

```
val employeeName = sc.textFile("/user/cuiyufei/cca/spark5/EmployeeName.csv")
val employeeNameRDD = employeeName.map(_.split(",")).map(line => (line(0), line(1)))
val employeeSalary = sc.textFile("/user/cuiyufei/cca/spark5/EmployeeSalary.csv")
val employeeSalaryRDD = employeeSalary.map(_.split(",")).map(line => (line(0), line(1)))
val joined = employeeNameRDD.join(employeeSalaryRDD)
val keyRemoved = joined.map({case(id,(name, salary)) => (name,salary)})
//val keyRemoved = joined.values
val rddGroupByKey = keyRemoved.map(line => line.swap).groupByKey()
val rddByKey = rddGroupByKey.map{case (k,v) => k->sc.makeRDD(v.toSeq)}
rddByKey.foreach{ case (k,rdd) => rdd.saveAsTextFile("/user/cuiyufei/cca/spark5/Employee"+k)}
```

## NO.59 CORRECT TEXT

Problem Scenario 55 : You have been given below code snippet.

```
val pairRDD1 = sc.parallelize(List( ("cat",2), ("cat", 5), ("book", 4),("cat", 12)))
val pairRDD2 = sc.parallelize(List( ("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)))
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. 

```
Array[(String, (Option[Int], Option[Int]))] = Array((book,(Some(4),None)),
(mouse,(None,Some(4))), (cup,(None,Some(5))), (cat,(Some(2),Some(2)), (cat,(Some(2),Some(12))), (cat,(Some(5),Some(2))), (cat,(Some(5),Some(12))), (cat,(Some(12),Some(2))), (cat,(Some(12),Some(12))))
```

**Answer:**

```
pairRDD1.fullOuterJoin(pairRDD2).collect
```

## NO.60 CORRECT TEXT

Problem Scenario 34 : You have given a file named spark6/user.csv. Data is given below:

user.csv 

```
id,topic,hits
Rahul,scala,120
Nikita,spark,80
Mithun,spark,1
myself,cca175,180
```

Now write a Spark code in scala which will remove the header part and create RDD of values as
below, for all rows. And also if id is myself" than filter out row. Map(id -> om, topic -> scala, hits -> 120)
**Answer:**

```
val user = sc.textFile("/user/cuiyufei/cca/spark6/user.csv").map(line => line.trim).map(_.split(","))
val header = user.first
val userRDD = user.filter(_(0) != header(0))
val zipRDD = userRDD.map(splits => header.zip(splits).toMap)
val result = zipRDD.filter(line => line("id") != "myself")
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark6_result")
```

## NO.61 CORRECT TEXT

Problem Scenario 43 : You have been given following code snippet.

```
val grouped = sc.parallelize(Seq(((1,"two"), List((3,4), (5,6)))))
val flattened = grouped.flatMap {A =>
groupValues.map { value => B }
}
```

You need to generate following output. Hence replace A and B.

```
Array((1,two,3,4),(1,two,5,6))
```

**Answer:**

```
A:case (key, groupValues)
B:(key._1, key._2, value._1, value._2)
```

## NO.62 CORRECT TEXT

Problem Scenario 25 : You have been given below comma separated employee information. That
needs to be added in /home/cloudera/flumetest/in.txt file (to do tail source) sex,name,city

```
1,alok,mumbai
1,jatin,chennai
1,yogesh,kolkata
2,ragini,delhi
2,jyotsana,pune
1,valmiki,banglore
```

Create a flume conf file using fastest non-durable channel, which write data in hive warehouse directory, in two separate tables called flumemaleemployee1 and flumefemaleemployee1 (Create hive table as well for given data}. Please use tail source with /home/cloudera/flumetest/in.txt file.
Flumemaleemployee1 : will contain only male employees data flumefemaleemployee1 : Will contain only woman employees data

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
3. Calculate total revenue perday and per customer
4. Calculate maximum revenue customer

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table orders --target-dir /user/cuiyufei/cca/p92_orders
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table order_items --target-dir /user/cuiyufei/cca/p92_order_items

order = sc.textFile("/user/cuiyufei/cca/p92_orders").map(lambda x:(int(x.split(",")[0]), x))
order_items = sc.textFile("/user/cuiyufei/cca/p92_order_items").map(lambda x:(int(x.split(",")[1]), x))

joinedRDD = order_items.join(order)

ordersPerDatePerCustomer = joinedRDD.map(lambda line:((line[1][1].split(",")[1], line[1][1].split(",")[2]), float(line[1][0].split(",")[4])))
amountCollectedPerDayPerCustomer = ordersPerDatePerCustomer.reduceByKey(lambda runningSum, amount: runningSum + amount)
revenuePerDatePerCustomerRDD = amountCollectedPerDayPerCustomer.map(lambda threeElementTuple: (threeElementTuple[0][0], (threeElementTuple[0][1],threeElementTuple[1])))

perDateMaxAmountCollectedByCustomer = revenuePerDatePerCustomerRDD.reduceByKey(lambda runningAmountTuple, newAmountTuple: (runningAmountTuple if runningAmountTuple[1] >= newAmountTuple[1] else newAmountTuple))
```



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

1. Copy "retaildb.orders" and "retaildb.orderjtems" table to hdfs in respective directory p89_orders
    and p89_order_items .
2. Join these data using orderid in Spark and Python
3. Now fetch selected columns from joined data Orderld, Order date and amount collected on this
    order.
4. Calculate total order placed for each date, and produced the output sorted by date.

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table orders --target-dir /user/cuiyufei/cca/p89_orders
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table order_items --target-dir /user/cuiyufei/cca/p89_order_items
order = sc.textFile("/user/cuiyufei/cca/p89_orders").map(lambda x:(int(x.split(",")[0]), x))
order_items = sc.textFile("/user/cuiyufei/cca/p89_order_items").map(lambda x:(int(x.split(",")[1]), x))

joinedRDD = order_items.join(order)
#(32768, (u'81958,32768,1073,1,199.99,199.99', u'32768,2014-02-12 00:00:00.0,1900,PENDING_PAYMENT'))
distinctOrdersDate = joinedRDD.map(lambda line:line[1][1].split(",")[1] + "," + line[1][1].split(",")[0]).distinct()
newLineTuple = distinctOrdersDate.map(lambda line: (line.split(",")[0], 1))
totalOrdersPerDate = newLineTuple.reduceByKey(lambda x,y:x+y)

```



## NO.65 CORRECT TEXT

Problem Scenario 72 : You have been given a table named "employee2" with following detail. 

first_name string
last_name string
Write a spark script in python which read this table and print all the rows and individual column values.

```
sqIContext = HiveContext(sc)
employee2 = sqlContext.sql("select * from employee2")
for row in employee2.collect(): 
	print(row)
for row in employee2.collect(): 
	print( row.first_name)
```



## NO.66 CORRECT TEXT

Problem Scenario 9 : You have been given following mysql database details as well as other info.

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Import departments table in a directory.
2. Again import departments table same directory (However, directory already exist hence it should
    not overrride and append the results)
3. Also make sure your results fields are terminated by '|' and lines terminated by '\n\

Answer:

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir /user/cuiyufei/cca/spark9_result --fields-terminated-by '|' --lines-terminated-by '\n' -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir /user/cuiyufei/cca/spark9_result --fields-terminated-by '|' --lines-terminated-by '\n' --append -m 1
```



## NO.67 CORRECT TEXT

Problem Scenario 44 : You have been given 4 files , with the content as given below:
spark11/file1.txt

```
Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures are common and should be automatically handled by the framework.
```

spark11/file2.txt

```
The core of Apache Hadoop consists of a storage part known as Hadoop Distributed FileSystem (HDFS) and a processing part called MapReduce. Hadoop splits files into large blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers packaged code for nodes to process in parallel based on the data that needs to be processed.
```

spark11/file3.txt

```
his approach takes advantage of data locality nodes manipulating the data they have access to to allow the dataset to be processed faster and more efficiently than it would be in a more conventional supercomputer architecture that relies on a parallel file system where computation and data are distributed via high-speed networking.
```

spark11/file4.txt

```
Apache Storm is focused on stream processing or what some call complex event processing. Storm implements a fault tolerant method for performing a computation or pipelining multiple computations on an event as it flows into a system. 
```

One might use Storm to transform unstructured data as it flows into a system into a desired format (spark11Afile1.txt) (spark11/file2.txt) (spark11/file3.txt) (spark11/file4.txt)
Write a Spark program, which will give you the highest occurring words in each file. With their file name and highest occurring words.

Answer:

```
val file1 = sc.textFile("/user/cloudera/spark11/file1.txt")
val file2 = sc.textFile("/user/cloudera/spark11/file2.txt")
val file3 = sc.textFile("/user/cloudera/spark11/file3.txt")
val file4 = sc.textFile("/user/cloudera/spark11/file4.txt")
val content1 = file1.flatMap( line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).map(e=>e.swap)
val content2 = file2.flatMap( line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).map(e=>e.swap)
val content3 = file3.flatMap( line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).map(e=>e.swap)
val content4 = file4.flatMap( line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _).map(item => item.swap).sortByKey(false).map(e=>e.swap)
val file1word = sc.makeRDD(Array(file1.name+"->"+content1.first._1+"-"+content1.first._2))
val file2word = sc.makeRDD(Array(file2.name+"->"+content2.first._1+"-"+content2.first._2))
val file3word = sc.makeRDD(Array(file3.name+"->"+content3.first._1+"-"+content3.first._2))
val file4word = sc.makeRDD(Array(file4.name+"->"+content4.first._1+"-"+content4.first._2))
val unionRDDs = file1word.union(file2word).union(file3word).union(file4word) 
unionRDDs.repartition(1).saveAsTextFile("spark11/union.txt")
```



## NO.68 CORRECT TEXT

Problem Scenario 15 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. In mysql departments table please insert following record. Insert into departments values(9999,
    '"Data Science"1);
2. Now there is a downstream system which will process dumps of this file. However, system is designed the way that it can process only files if fields are enlcosed in(') single quote and separate of the field should be (-) and line needs to be terminated by : (colon).
3. If data itself contains the " (double quote ) than it should be escaped by \.
4. Please import the departments table in a directory called departments_enclosedby and file should be able to process by downstream system.

Answer:

```
#mysql 
insert into departments values(9999, "Data Science")
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_enclouseby --enclosed-by "\'" --fields-terminated-by "-" --lines-terminated-by ":" --escaped-by "\\"
```



## NO.69 CORRECT TEXT

Problem Scenario 86 : In Continuation of previous question, please accomplish following activities.
1 . Select Maximum, minimum, average , Standard Deviation, and total quantity.
2 . Select minimum and maximum price for each product code.

3. Select Maximum, minimum, average , Standard Deviation, and total quantity for each product code, hwoever make sure Average and Standard deviation will have maximum two decimal values.
4. Select all the product code and average price only where product count is more than or equal to 3.
5. Select maximum, minimum , average and total of all the products for each code. Also produce the same across all the products.
    Answer:

## NO.70 CORRECT TEXT

Problem Scenario 3: You have been given MySQL DB with following details. 

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following activities.

1. Import data from categories table, where category=22 (**Data should be stored in categories subset**)
2. Import data from categories table, where category>22 (Data should be stored in categories_subset_2)
3. Import data from categories table, where category between 1 and 22 (Data should be stored in categories_subset_3)
4. While importing catagories data change the delimiter to '|' (Data should be stored in
    categories_subset_S)
5. Importing data from catagories table and restrict the import to category_name,category id columns only with delimiter as '|'
6. Add null values in the table using below SQL statement ALTER TABLE categories modify category_department_id int(11); INSERT INTO categories values
    (eO.NULL.'TESTING');
7. Importing data from catagories table (In categories_subset_17 directory) using '|' delimiter and
    categoryjd between 1 and 61 and encode null values for both string and non string columns.
8. Import entire schema retail_db in a directory categories_subset_all_tables

Answer:

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --warehouse-dir categories_subset --where "category_id = 22" -m l
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --warehouse_dir categories_subset2 --where "category > 22" -m l
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --warehouse_dir categories_subset3 --where "category between 1 and 22" -m l
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --fields-terminated-by '|' --warehouse_dir categories_subset --where "category between 1 and 22" -m l
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --pasword cloudera --table=categories --warehouse-dir=categories_subset_8 --columns category_name,category_id --fields-terminated-by '|' -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --pasword cloudera --table=categories --warehouse-dir=categories_subset_17  --where "cagegory between 1 and 61" --fields-terminated-by '|' --null-string null --null-non-string null -m 1
#mysql 
alter table categories modify category_department_id int(11);
insert into categories values(NULL, 'TESTING');

sqoop import-all-tables --connect jdbc:mysql://quickstart:3306/retail_db --username=retail_dba --password=cloudera --warehouse-dir=categories_subset_all_tables
```

## NO.71 CORRECT TEXT

Problem Scenario 87 : You have been given below three files product.csv (Create this file in hdfs) productID,productCode,name,quantity,price,supplierid

```
1001,PEN,Pen Red,5000,1.23,501
1002,PEN,Pen Blue,8000,1.25,501
1003,PEN,Pen Black,2000,1.25,501
1004,PEC,Pencil 2B,10000,0.48,502
1005,PEC,Pencil 2H,8000,0.49,502
1006,PEC,Pencil HB,0,9999.99,502
2001,PEC,Pencil 3B,500,0.52,501
2002,PEC,Pencil 4B,200,0.62,501
2003,PEC,Pencil 5B,100,0.73,501
2004,PEC,Pencil 6B,500,0.47,502
```

supplier.csv
supplierid,name,phone 

```
501,ABC Traders,88881111
502,XYZ Company,88882222
503,QQ Corp,88883333
```

products_suppliers.csv 
productID,supplierID 

```
2001,501
2002,501
2003,501
2004,502
2001,503
```

Now accomplish all the queries given in solution.
Select product, its price , its supplier name where product price is less than 0.6 using SparkSQL
**Answer:**

```
case class product(productID:Int, productCode:String, name:String, quantity:Float, price:Float, supplierid:Int)
case class supplier(supplierid:Int, name:String, phone:String)
case class product_supplier(productID:Int, supplierID:Int)
val products = sc.textFile("/user/cuiyufei/cca/spark88/product.csv")
val suppliers = sc.textFile("/user/cuiyufei/cca/spark88/supplier.csv")
val products_suppliers = sc.textFile("/user/cuiyufei/cca/spark88/products_suppliers.csv")
val productsDF = products.map(_.split(",")).map(p => product(p(0).toInt, p(1), p(2), p(3).toFloat, p(4).toFloat, p(5).toInt)).toDF
productsDF.registerTempTable("product")
val suppliersDF = suppliers.map(_.split(",")).map(p => supplier(p(0).toInt, p(1), p(2))).toDF
suppliersDF.registerTempTable("supplier")
val products_suppliersDF = products_suppliers.map(_.split(",")).map(p => product_supplier(p(0).toInt, p(1).toInt)).toDF
products_suppliersDF.registerTempTable("products_suppliers")
val result = sqlContext.sql("select a.name as product_name, a.price, b.name as supplier_name  from product a join supplier b on a.supplierid=b.supplierid and a.price < 0.6")
```

注意：case class 中的名字，不能和变量的名字重名



## NO.72 CORRECT TEXT

Problem Scenario 19 : You have been given following mysql database details as well as other info.

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

 Now accomplish following activities.

1. Import departments table from mysql to hdfs as textfile in departments_text directory.
2. Import departments table from mysql to hdfs as sequncefile in departments_sequence directory.
3. Import departments table from mysql to hdfs as avro file in departments_avro directory.
4. Import departments table from mysql to hdfs as parquet file in departments_parquet directory.

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_text --as-textfile -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_sequence --as-sequencefile -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_avro --as-avrodatafile -m 1
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --target-dir departments_text --as-parquetfile -m 1
```



## NO.73 CORRECT TEXT

Problem Scenario 82 : You have been given table in Hive with following structure (Which you have created in previous exercise).
productid int code string name string quantity int price float Using SparkSQL accomplish following activities.
1 . Select all the products name and quantity having quantity <= 2000
2 . Select name and price of the product having code as 'PEN'
3 . Select all the products, which name starts with PENCIL
4 . Select all products which "name" begins with 'P\ followed by any two characters, followed by space, followed by zero or more characters
Answer:

## NO.74 CORRECT TEXT

Problem Scenario 18 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Now accomplish following activities.

1. Create mysql table as below.
     mysql --user=retail_dba -password=cloudera
       use retail_db
       CREATE TABLE IF NOT EXISTS departments_hive02(id int, department_name varchar(45), avg_salary int);
       show tables;
2. Now export data from hive table departments_hive01 in departments_hive02. While exporting,
     please note following. wherever there is a empty string it should be loaded as a null value in mysql. wherever there is -999 value for int field, it should be created as null value.

**Answer:**

```
#mysql
create table if not exists departments_hive02(id int, department_name varchar(45), avg_salary int);
sqoop export --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments_hive02 --input-null-string "" --input-null-non-string -999 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --export-dir /user/hive/warehouse/departments_hive01 -m 1
```



## NO.75 CORRECT TEXT 

Problem Scenario 2 :

There is a parent organization called "ABC Group Inc", which has two child companies named Tech
Inc and MPTech.
Both companies employee information is given in two separate text file as below. Please do the
following activity for employee details. Tech Inc.txt
1,Alok,Hyderabad 2,Krish,Hongkong 3,Jyoti,Mumbai
4 ,Atul,Banglore
5 ,Ishan,Gurgaon
MPTech.txt
6 ,John,Newyork
7 ,alp2004,California
8 ,tellme,Mumbai
9 ,Gagan21,Pune
1 0,Mukesh,Chennai
1 . Which command will you use to check all the available command line options on HDFS and How will you get the Help for individual command.

2. Create a new Empty Directory named Employee using Command line. And also create an empty file
    named in it Techinc.txt
3. Load both companies Employee data in Employee directory (How to override existing file in HDFS).
4. Merge both the Employees data in a Single tile called MergedEmployee.txt, merged tiles should have new line character at the end of each file content.
5. Upload merged file on HDFS and change the file permission on HDFS merged file, so that owner and group member can read and write, other user can read the file.
6. Write a command to export the individual file as well as entire directory from HDFS to local file System.
    Answer:

## NO.76 CORRECT TEXT

Problem Scenario 27 : You need to implement near real time solutions for collecting information when submitted in file with below information.
Data
echo "IBM,100,20160104" >> /tmp/spooldir/bb/.bb.txt echo "IBM,103,20160105" >> /tmp/spooldir/bb/.bb.txt mv /tmp/spooldir/bb/.bb.txt /tmp/spooldir/bb/bb.txt After few mins
echo "IBM,100.2,20160104" >> /tmp/spooldir/dr/.dr.txt echo "IBM,103.1,20160105" >> /tmp/spooldir/dr/.dr.txt mv /tmp/spooldir/dr/.dr.txt /tmp/spooldir/dr/dr.txt
Requirements:
You have been given below directory location (if not available than create it) /tmp/spooldir . You have a finacial subscription for getting stock prices from BloomBerg as well as
Reuters and using ftp you download every hour new files from their respective ftp site in directories /tmp/spooldir/bb and /tmp/spooldir/dr respectively.
As soon as file committed in this directory that needs to be available in hdfs in /tmp/flume/finance location in a single directory.
Write a flume configuration file named flume7.conf and use it to load data in hdfs with following additional properties .
1 . Spool /tmp/spooldir/bb and /tmp/spooldir/dr
2 . File prefix in hdfs sholuld be events
3 . File suffix should be .log
4 . If file is not commited and in use than it should have _ as prefix.
5 . Data should be written as text to hdfs
Answer:

## NO.77 CORRECT TEXT

Problem Scenario 30 : You have been given three csv files in hdfs as below.
EmployeeName.csv with the field (id, name) EmployeeManager.csv (id, manager Name) EmployeeSalary.csv (id, Salary)
Using Spark and its API you have to generate a joined output as below and save as a text tile (Separated by comma) for final distribution and output must be sorted by id. ld,name,salary,managerName
EmployeeManager.csv 

```
E01,Vishnu
E02,Satyam
E03,Shiv
E04,Sundar
E05,John
E06,Pallavi
E07,Tanvir
E08,Shekhar
E09,Vinod
E10,Jitendra
```

 EmployeeName.csv 

```
E01,Lokesh
E02,Bhupesh
E03,Amit
E04,Ratan
E05,Dinesh
E06,Pavan
E07,Tejas
E08,Sheela
E09,Kumar
E10,Venkat
```

 EmployeeSalary.csv 

```
E01,50000
E02,50000
E03,45000
E04,45000
E05,50000
E06,45000
E07,50000
E08,10000
E09,10000
E10,10000
```

 **Answer:**

```
val EmployeeManagerRDD = sc.textFile("/user/cuiyufei/cca/spark30/EmployeeManager.csv").map(line => (line.split(",")(0), line.split(",")(1)))
val EmployeeNameRDD = sc.textFile("/user/cuiyufei/cca/spark30/EmployeeName.csv").map(line => (line.split(",")(0), line.split(",")(1)))
val EmployeeSalaryRDD = sc.textFile("/user/cuiyufei/cca/spark30/EmployeeSalary.csv").map(line => (line.split(",")(0), line.split(",")(1)))
val joinedRDD = EmployeeManagerRDD.join(EmployeeNameRDD).join(EmployeeSalaryRDD)
val sortedRDD = joinedRDD.sortByKey()
val result = sortedRDD.map(line => line._1 + "," + line._2._1._2 + "," + line._2._2 + "," + line._2._1._1)
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark30_result")
```





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



## NO.79 CORRECT TEXT

Problem Scenario 95 : You have to run your Spark application on yarn with each executor
Maximum heap size to be 512MB and Number of processor cores to allocate on each executor will be
1 and Your main application required three values as input arguments V1
V2 V3.
Please replace XXX, YYY, ZZZ
./bin/spark-submit -class com.hadoopexam.MyTask --master yarn-cluster --num-executors 3 --driver-memory 512m XXX YYY lib/hadoopexam.jar ZZZ
**Answer:**

```
XXX:--executor-memory 512M
YYY:--excutor-cores 1
ZZZ:V1 V2 V3
```



## NO.80 CORRECT TEXT 

Problem Scenario 1:

You have been given MySQL DB with following details.

```
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

 Please accomplish following activities.
1 . Connect MySQL DB and check the content of the tables.
2 . Copy "retaildb.categories" table to hdfs, without specifying directory name.
3 . Copy "retaildb.categories" table to hdfs, in a directory name "categories_target".
4 . Copy "retaildb.categories" table to hdfs, in a warehouse directory name "categories_warehouse".
**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories -m 1
#hdfs dfs -ls categories
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --target-dir categories_target -m 1
#hdfs dfs -ls categories_target
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table categories --warehouse-dir categories_warehouse -m 1
```

## NO.81 CORRECT TEXT

Problem Scenario 56 : You have been given below code snippet. 

```
val a = sc.parallelize(1 to 100, 3)
```

operation1
Write a correct code snippet for operation1 which will produce desired output, shown below.

```
Array [Array [I nt]] = Array(Array(1, 2, 3,4, 5, 6, 7, 8, 9,10,11,12,13,14,15,16,17,18,19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33), Array(34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,5 6, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66), Array(67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88,
89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100))
```

**Answer:**

```
a.glom.collect
```



## NO.82 CORRECT TEXT

Problem Scenario 47 : You have been given below code snippet, with intermediate output. 

```
val z = sc.parallelize(List(1,2,3,4,5,6), 2)
```

// lets first print out the contents of the RDD with partition labels 

```
def myfunc(index: Int, iter: lterator[(lnt)]): Iterator[String] = { iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator }
```

//In each run , output could be different, while solving problem assume belown output only. 

```
z.mapPartitionsWithlndex(myfunc).collect
res28: Array[String] = Array([partlD:0, val: 1], [partlD:0, val: 2], [partlD:0, val: 3], [partlD:1, val: 4], [partlD:1, val: S], [partlD:1, val: 6])
```

Now apply aggregate method on RDD z , with two reduce function , first will select max value in each partition and second will add all the maximum values from all partitions.
Initialize the aggregate with value 5. hence expected output will be 16.
**Answer:**

```
z.aggregate(5)(math.max(_ , _), _ + _)
```

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
2.  Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
3.  Also make sure you use orderid columns for sqoop to use for boundary conditions.

**Answer:**

```

```

## NO.84 CORRECT TEXT

Problem Scenario 54 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
val b = a.map(x => (x.length, x)) 
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. 

```
Array[(lnt, String)] = Array((4,lion), (7,panther), (3,dogcat), (5,tigereagle))
```

**Answer:**

```
b.reducByKey(_ + _).collect
```



## NO.85 CORRECT TEXT

Problem Scenario 12 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

 Please accomplish following.

1. Create a table in retailedb with following definition.
    CREATE table departments_new (department_id int(11), department_name varchar(45),
    created_date T1MESTAMP DEFAULT NOW());
    2 . Now insert records from departments table to departments_new
    3 . Now import data from departments_new table to hdfs.
    4 . Insert following 5 records in departmentsnew table. Insert into departments_new values(110, "Civil" , null); Insert into departments_new values(111, "Mechanical" , null);
    Insert into departments_new values(112, "Automobile" , null); Insert into departments_new values(113, "Pharma" , null);
    Insert into departments_new values(114, "Social Engineering" , null);
2. Now do the incremental import based on created_date column.

**Answer:**

```
create table departments_new(department_id int(11), department_name varchar(45), created_date TIMESTAMP default now());
insert into departments_new select *, null from departments;
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments_new --target-dir p83_target -m 1
insert into departments_new values(110, "Civil", null);
insert into departments_new values(111, "Mechanical", null);
insert into departments_new values(112, "Automobile", null);
insert into departments_new values(113, "Pharma", null);
insert into departments_new values(114, "Engineering", null);
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments_new --check-column created_date --incremental append --last-value '2019-06-12 22:20:53' --target-dir p83_target -m 1

```



## NO.86 CORRECT TEXT

Problem Scenario 57 : You have been given below code snippet.

```
 val a = sc.parallelize(1 to 9, 3) 
 operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below.

```
Array[(String, Seq[lnt])] = Array((even,ArrayBuffer(2, 4, 6, 8)), (odd,ArrayBuffer(1, 3, 5, 7, 9)))
```

**Answer:**

```
a.groupBy(line => if(line%2 ==0) "even" else "odd").collect
```



## NO.87 CORRECT TEXT

Problem Scenario 36 : You have been given a file named spark8/data.csv (type,name). data.csv

```
1 ,Lokesh
2 ,Bhupesh
2 ,Amit
2 ,Ratan
2 ,Dinesh
1 ,Pavan
1 ,Tejas
2 ,Sheela
1 ,Kumar
1 ,Venkat
```

1. Load this file from hdfs and save it back as (id, (all names of same type)) in results directory. However, make sure while saving it should be

**Answer:**

```
val data = sc.textFile("/user/cuiyufei/cca/spark36/data.csv")
val dataRDD = data.map(line => line.trim).map(line => (line.split(",")(0), line.split(",")(1)))
val swapped = dataRDD.map(item => item.swap)
val combinedOutput = dataRDD.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)
combinedOutput.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark36_reuslt")
```



## NO.88 CORRECT TEXT Problem Scenario 71 :

Write down a Spark script using Python,
In which it read a file "Content.txt" (On hdfs) with following content.
After that split each row as (key, value), where key is first word in line and entire line as value. Filter out the empty lines.
And save this key value in "problem86" as Sequence file(On hdfs)
Part 2 : Save as sequence file , where key as null and entire line as value. Read back the stored sequence files.
Content.txt
Hello this is ABCTECH.com This is XYZTECH.com Apache Spark Training
This is Spark Learning Session Spark is faster than MapReduce Answer:
See the explanation for Step by Step Solution and configuration. Explanation:
Solution :

## NO.89 CORRECT TEXT

Problem Scenario 42 : You have been given a file (spark10/sales.txt), with the content as given in
below.

spark10/sales.txt 
Department,Designation,costToCompany,State 

```
Sales,Trainee,12000,UP
Sales,Lead,32000,AP
Sales,Lead,32000,LA
Sales,Lead,32000,TN
Sales,Lead,32000,AP
Sales,Lead,32000,TN
Sales,Lead,32000,LA
Sales,Lead,32000,LA
Marketing,Associate,18000,TN
Marketing,Associate,18000,TN
HR,Manager,58000,TN
```

And want to produce the output as a csv with group by Department,Designation,State with additional columns with sum(costToCompany) and TotalEmployeeCountt
Should get result like Dept,Desg,state,empCount,totalCost Sales,Lead,AP,2,64000 Sales.Lead.LA.3.96000 Sales,Lead,TN,2,64000
Answer:

## NO.90 CORRECT TEXT

Problem Scenario 7 : You have been given following mysql database details as well as other info. 

```
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
```

Please accomplish following.

1. Import department tables using your custom boundary query, which import departments between
    1 to 25.
2. Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
3. Also make sure you have imported only two columns from table, which are department_id,department_name

**Answer:**

```
sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --table departments --boundary_query "select 1, 25 from departments" --target-dir /user/cloudera/departments --columns department_id,department_name -m 2
```

## NO.91 CORRECT TEXT

Problem Scenario 64 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(Ust("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val d = c.keyBy(_.length)
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. Array[(lnt, (Option[String], String))] = Array((6,(Some(salmon),salmon)), (6,(Some(salmon),rabbit}}, (6,(Some(salmon),turkey)), (6,(Some(salmon),salmon)), (6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), (3,(Some(dog),dog)), (3,(Some(dog),cat)), (3,(Some(dog),gnu)), (3,(Some(dog),bee)), (3,(Some(rat), (3,(Some(rat),cat)), (3,(Some(rat),gnu)), (3,(Some(rat),bee)), (4,(None,wo!f)),
(4,(None,bear)))
**Answer:**

```
b.rightOuterJoin(d).collect
```



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
     p92_orders and p92 order items 
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



## NO.93 CORRECT TEXT

Problem Scenario 76 : You have been given MySQL DB with following details. user=retail_dba
password=cloudera database=retail_db table=retail_db.orders table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of order table : (orderid , order_date , ordercustomerid, order_status}
.....
Please accomplish following activities.
1 . Copy "retail_db.orders" table to hdfs in a directory p91_orders.
2 . Once data is copied to hdfs, using pyspark calculate the number of order for each status.
3 . Use all the following methods to calculate the number of order for each status. (You need to know
all these functions and its behavior for real exam) 

- countByKey()

- groupByKey()

- reduceByKey()
- aggregateByKey()

- combineByKey()

**Answer:**

## NO.94 CORRECT TEXT

Problem Scenario 65 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
val b = sc.parallelize(1 to a.count.toInt, 2)
val c = a.zip(b)
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. 

```
Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))
```

**Answer:**

```
c.sortByKey(false).collect
```

## NO.95 CORRECT TEXT

Problem Scenario 51 : You have been given below code snippet. 

```
val a = sc.parallelize(List(1, 2, 1, 3), 1)
val b = a.map((_, "b")) 
val c = a.map((_, "c"))
Operation_xyz
```

Write a correct code snippet for Operationxyz which will produce below output. Output:
Array[(lnt, (lterable[String], lterable[String]))] = Array( (2,(ArrayBuffer(b),ArrayBuffer(c))), (3,(ArrayBuffer(b),ArrayBuffer(c))),
(1,(ArrayBuffer(b, b),ArrayBuffer(c, c))) )
**Answer:**

```
b.cogroup(c).collect
```

## NO.96 CORRECT TEXT

Problem Scenario 93 : You have to run your Spark application with locally 8 thread or locally on 8 cores. Replace XXX with correct values.
spark-submit --class com.hadoopexam.MyTask XXX \ -deploy-mode cluster SSPARK_HOME/lib/hadoopexam.jar 10
**Answer:**

```
XXX:--master local[8]
```
