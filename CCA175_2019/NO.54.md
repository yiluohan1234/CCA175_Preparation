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
val technology = sc.textFile("/user/cloudera/exam/task54/technology.txt")
val technologyRDD = technology.map(_.split(",")).map(line=> ((line(0), line(1)), line(2)))
val salary = sc.textFile("/user/cloudera/exam/task54/salary.txt")
val salaryRDD = salary.map(_.split(",")).map(line=> ((line(0), line(1)), line(2)))
val joinRDD = technologyRDD.join(salaryRDD)
val result = joinRDD.map({case((first,last), (tec,salary))  => first+" " + last + "." + tec + "." + salary})
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark12_result")


val result = joinedRDD.map({case((first, last), (tec,sa))=>first+"," + last + "," + tec+","+sa})
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark12_test")
```

