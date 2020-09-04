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
val courses = sc.textFile("/user/cloudera/exam/task33/course.txt")
val fees = sc.textFile("/user/cloudera/exam/task33/fee.txt")
val feesDF=fees.map(_.split(",")).map(p => fee(p(0).toInt, p(1).toFloat)).toDF()
val coursesDF= courses.map(_.split(",")).map(p => course(p(0).toInt, p(1))).toDF()
feesDF.registerTempTable("fee")
coursesDF.registerTempTable("course")
val result_1 = sqlContext.sql("select a.course,b.fee from course a left join fee b on a.id=b.id")
val result_2 = sqlContext.sql("select a.course, b.fee from course a right join fee b on a.id=b.id")
val result_3 = sqlContext.sql("select a.course, b.fee from course a left join fee b on a.id=b.id where b.fee is not null")
```

