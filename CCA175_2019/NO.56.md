## NO.56 CORRECT TEXT

Problem Scenario 52 : You have been given below code snippet. 

```
val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
Operation_xyz
```

Write a correct code snippet for Operation_xyz which will produce below output. scala collection.

```
Map[Int,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> 6, 2 -> 3, 4 -> 2, 7 -> 1)
```

**Answer:**

```
b.countByValue
```

注：

不知道的情况下，可以用pyspark输入形成一个RDD让后`help(RDD)`查找即可