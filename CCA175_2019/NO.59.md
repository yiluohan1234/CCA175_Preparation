## NO.59 CORRECT TEXT

Problem Scenario 55 : You have been given below code snippet.

```
val pairRDD1 = sc.parallelize(List( ("cat",2), ("cat", 5), ("book", 4),("cat", 12)))
val pairRDD2 = sc.parallelize(List( ("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)))
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. 

```
Array[(String, (Option[Int], Option[Int]))] = Array((book,(Some(4),None)),
(mouse,(None,Some(4))), (cup,(None,Some(5))), (cat,(Some(2),Some(2)), (cat,(Some(2),Some(12))), (cat,(Some(5),Some(2))), (cat,(Some(5),Some(12))), (cat,(Some(12),Some(2))), (cat,(Some(12),Some(12))))
```

**Answer:**

```
pairRDD1.fullOuterJoin(pairRDD2).collect
```

