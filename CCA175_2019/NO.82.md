## NO.82 CORRECT TEXT

Problem Scenario 47 : You have been given below code snippet, with intermediate output. 

```
val z = sc.parallelize(List(1,2,3,4,5,6), 2)
```

lets first print out the contents of the RDD with partition labels 

```
def myfunc(index: Int, iter: lterator[(lnt)]): Iterator[String] = { iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator }
```

In each run , output could be different, while solving problem assume belown output only. 

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

