## NO.45 CORRECT TEXT

Problem Scenario 70 : Write down a Spark Application using Python, In which it read a file
"Content.txt" (On hdfs) with following content. Do the word count and save the results in a directory called "problem85" (On hdfs)
Content.txt

```
Hello this is ABCTECH.com
This is XYZTECH.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce
file = sc.textFile("Content.txt")
result = file.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).saveAsTextFile("problem85")
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

注：

```
#过滤空行
filter(lambda x: len(x) > 0)
```

