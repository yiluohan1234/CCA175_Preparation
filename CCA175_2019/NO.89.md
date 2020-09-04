## NO.89 CORRECT TEXT

Problem Scenario 42 : You have been given a file (spark10/sales.txt), with the content as given in below.

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

And want to produce the output as a csv with group by Department,Designation,State with additional columns with sum(costToCompany) and TotalEmployeeCount
Should get result like Dept,Desg,state,empCount,totalCost Sales,Lead,AP,2,64000 Sales.Lead.LA.3.96000 Sales,Lead,TN,2,64000
**Answer:**

```
val sales = sc.textFile("/user/cloudera/exam/task89/sales.txt").map(_.split(",")).map(line => ((line(0), line(1), line(3)), line(2).toInt))
val combine = sales.combineByKey(score =>(score, 0), (s:(Int, Int), x:Int)=> s._1+x, s._2+1, (c1:(Int,Int), c2:(Int, Int))=>(c1._1+c2._1, c1._2+c2._2))
val agg = sales.aggregateByKey((0,0))((init, value) => (init._1 + 1,init._2 + value), (x, y) => (x._1 + y._1, x._2 + y._2)).map(e => (e._1._1,e._1._2, e._1._3, e._2._1, e._2._2))
```

方法一：

```
import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
import  sqlContext.implicits._
case class sale(Department: String,Designation: String, costToCompany: Int, State: String)
val saleDF = sc.textFile("/user/cloudera/exam/task89/sales.txt").map(_.split(",")).map(p => sale(p(0), p(1), p(2).toInt, p(3))).toDF
saleDF.registerTempTable("sale")
val results = sqlContext.sql("select Department, Designation, State, count(1), sum(costToCompany) from sale group by Department, Designation, State")
results.coalesce(1).write.csv("hdfs://localhost:9000/user/cuiyufei/89/sample.csv")
results.coalesce(1).write.format("com.databricks.spark.csv").save("/data/home/sample.csv")
results.rdd.repartition(1).saveAsTextFile("hdfs://localhost:9000/user/cuiyufei/89/sample.csv")
```



方法二：

```
case class sale(Department: String,Designation: String, costToCompany: Int, State: String)
val saleRDD = sc.textFile("/user/cloudera/exam/task89/sales.txt").map(_.split(",")).map(p => sale(p(0), p(1), p(2).toInt, p(3))).map(e =>((e.Department, e.Designation, e.State), (1, e.costToCompany)))
val results = saleRDD.reduceByKey((a,b) => (a._1+b._1, a._2+b._2)).map(e => (e._1._1,e._1._2, e._1._3, e._2._1, e._2._2))
results.toDF.coalesce(1).write.csv("hdfs://localhost:9000/user/cuiyufei/89/sample.csv")
```

方法三：

```
val salesRDD = sc.textFile("/user/cloudera/exam/task89/sales.txt").map(_.split(",")).map(e => ((e(0), e(1), e(3)),(e(2).toInt, 1)))
val results = salesRDD.reduceByKey((a,b)=>(a._1+b._1, a._2+b._2)).map(e =>(e._1._1,e._1._2,e._1._3,e._2._2,e._2._1))
results.toDF.coalesce(1).write.csv("hdfs://localhost:9000/user/cuiyufei/89/sample.csv")
```

方法四：

```
case class sale(Department: String,Designation: String, costToCompany: Int, State: String)
val saleRDD = sc.textFile("/user/cloudera/exam/task89/sales.txt").map(_.split(",")).map(p => sale(p(0), p(1), p(2).toInt, p(3))).map(e =>((e.Department, e.Designation, e.State), e.costToCompany))
val results = saleRDD.aggregateByKey((0,0))((init, value) => (init._1 + 1,init._2 + value), (x, y) => (x._1 + y._1, x._2 + y._2)).map(e => (e._1._1,e._1._2, e._1._3, e._2._1, e._2._2))
results.toDF.coalesce(1).write.csv("hdfs://localhost:9000/user/cuiyufei/89/sample.csv")
```

