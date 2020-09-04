## NO.88 CORRECT TEXT 

Problem Scenario 71 :

Write down a Spark script using Python,
In which it read a file "Content.txt" (On hdfs) with following content.
After that split each row as (key, value), where key is first word in line and entire line as value. Filter out the empty lines. And save this key value in "problem86" as Sequence file(On hdfs)
Part 2 : Save as sequence file , where key as null and entire line as value. Read back the stored sequence files.
Content.txt
Hello this is ABCTECH.com This is XYZTECH.com Apache Spark Training
This is Spark Learning Session Spark is faster than MapReduce Answer:
See the explanation for Step by Step Solution and configuration. Explanation:
**Solution :**

```
content = sc.textFile("/user/cloudera/exam/task88/Content.txt").filter(lambda line:len(line)>0)
contentRDD = content.map(lambda line:(line.split(" ")[0], line))
contentRDD.saveAsSequenceFile("problem86")
```

