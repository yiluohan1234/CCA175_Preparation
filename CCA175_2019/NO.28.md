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

Step 1:

```
CREATE TABLE flumeemployee(name string, salary int, sex string, age int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
```

Step 2 : Create flume configuration file

```
a1.sources = s1
a1.sinks = k1
a1.channels = c1

a1.sources.s1.type = netcat
a1.sources.s1.bind = 127.0.0.1
a1.sources.s1.port = 44444

a1.sinks.k1.channel = memory-channel
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /user/hive/warehouse/flumeemployee
a1.sinks.k1.hdfs.writeFormat = TEXT
a1.sinks.k1.hdfs.fileType = DataStream

a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.tranactionCapacity = 100

a1.sources.s1.channels = c1
a1.sinks.k1.channel = c1
```

Step 3:

```
flume-ng agent --conf /home/cloudera/flumeconf --conf-file /home/cloudera/flumeconf/flume2.conf --name a1
```

 Step 4 : Open another terminal and use the netcat service.

```
nc localhost 44444
```

Step 5 : Enter data line by line.

```
alok,100000,male,29
jatin,105000,male,32
yogesh,134000,male,39
ragini,112000,female,35
jyotsana,129000,female,39
valmiki,123000,male,29
```

Step 6 : Open hue and check the data is available in hive table or not. 

step 7 : Stop flume service by pressing ctrl+c
Step 8 : Calculate average salary on hive table using below query. You can use either hive command line tool or hue. 

```
select avg(salary) from flumeemployee;
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

Step 1 : Select all the records with quantity >= 5000 and name starts with 'Pen' 

```
val results = sqlContext.sql(......SELECT * FROM products WHERE quantity >= 5000 AND name LIKE 'Pen %.......) 
results.show()
```

Step 2 : Select all the records with quantity >= 5000 , price is less than 1.24 and name starts with 'Pen' 

```
val results = sqlContext.sql(......SELECT * FROM products WHERE quantity >= 5000 AND price < 1.24 AND name LIKE 'Pen %.......) 
results. show()
```

Step 3 : Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen' 

```
val results = sqlContext.sql('.....SELECT * FROM products WHERE NOT (quantity >= 5000
AND name LIKE 'Pen %')......) 
results. show()

```

Step 4 : Select all the products wchich name is 'Pen Red', 'Pen Black'

```
val results = sqlContext.sql('.....SELECT' FROM products WHERE name IN ('Pen Red', 'Pen Black')......)
results. show()
```

Step 5 : Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.

```
val results = sqlContext.sql(......SELECT * FROM products WHERE (price BETWEEN 1.0
AND 2.0) AND (quantity BETWEEN 1000 AND 2000)......) 
results. show()
```

