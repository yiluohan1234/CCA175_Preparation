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

Now write a Spark code in scala which will remove the header part and create RDD of values as below, for all rows. And also if id is myself" than filter out row. Map(id -> om, topic -> scala, hits -> 120)
**Answer:**

```
val user = sc.textFile("/user/cloudera/exam/task60/user.csv").map(line => line.trim).map(_.split(","))
val header = user.first
val userRDD = user.filter(_(0) != header(0)).filter(_(0) != "myself")
val result = userRDD.map(splits => header.zip(splits).toMap)
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark6_result")
```

注：

toMap，toSeq