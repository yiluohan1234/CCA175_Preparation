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

注：`sc.makeRDD(Array(file4.name+"->"+content4.first._1+"-"+content4.first._2))`

