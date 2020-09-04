## NO.22 CORRECT TEXT

Problem Scenario 31 : You have given following two files
1 . Content.txt: Contain a huge text file containing space separated words.
2 . Remove.txt: Ignore/filter all the words given in this file (Comma Separated).
Write a Spark program which reads the Content.txt file and load as an RDD, remove all the words from a broadcast variables (which is loaded as an RDD of words from Remove.txt). And count the occurrence of the each word and save it as a text file in HDFS. 

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
val content = sc.textFile("/user/cloudera/exam/task22/Content.txt")
val contentRDD = content.flatMap(_.split(" "))
val remove = sc.textFile("/user/cloudera/exam/task22/Remove.txt")
val removeRDD = remove.flatMap(_.split(",")).map(line => line.trim)
val bRemove = sc.broadcast(removeRDD.collect.toList)
val filterRDD = contentRDD.filter({case(word) => !bRemove.value.contains(word)})
val result = filterRDD.map(word => (word, 1)).reduceByKey(_ + _)
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/result31")
```

**广播变量：RDD转换为列表，在判断是否在列表当中**

获取广播变量的值

```
广播变量.value
```

