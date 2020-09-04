# NO.19 CORRECT TEXT

Problem Scenario 59 : You have been given below code snippet.

```
val x = sc.parallelize(1 to 20)
val y = sc.parallelize(10 to 30)
operation1
z.collect
```

Write a correct code snippet for operation1 which will produce desired output, shown below.

```
 Array[int] = Array(16,12,20,13,17,14,18,10,19,15,11)
```

**Answer:**

```
val z = x.intersection(y)
```

## 注：并集、交集和差集

### union

```
def union(other: RDD[T]): RDD[T]
```

该函数比较简单，就是将两个RDD进行合并，**不去重**

```
val a = sc.parallelize(List(("a", Array(1,2,3)), ("b", Array(1,2))))
val b = sc.parallelize(List(("a", Array(1,2,3)), ("b", Array(2))))
a.union(b).collect

res3: Array[(String, Array[Int])] = Array((a,Array(1, 2, 3)), (b,Array(1, 2)), (a,Array(1, 2, 3)), (b,Array(2)))
```

### intersection

```
def intersection(other: RDD[T]): RDD[T]
def intersection(other: RDD[T], numPartitions: Int): RDD[T]
def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T]
```

该函数返回两个RDD的交集，并且**去重**。

```
val a = sc.parallelize(List(("a", Array(1,2,3)), ("b", Array(1,2))))
val b = sc.parallelize(List(("a", Array(1,2,3)), ("b", Array(2))))
a.intersection(b).collect
res0: Array[(String, Array[Int])] = Array()
```

### subtract

```
def subtract(other: RDD[T]): RDD[T]
def subtract(other: RDD[T], numPartitions: Int): RDD[T]
def subtract(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T]
```

该函数类似于intersection，但返回在RDD中出现，并且不在otherRDD中出现的元素，**不去重**。

```
val a = sc.parallelize(List(("a", Array(1,2,3)), ("b", Array(1,2))));
val b = sc.parallelize(List(("a", Array(1,2,3)), ("b", Array(2))));
a.subtract(b).collect
res5: Array[(String, Array[Int])] = Array((a,Array(1, 2, 3)), (b,Array(1, 2)))
```

