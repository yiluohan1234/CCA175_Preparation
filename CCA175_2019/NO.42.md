## NO.42 CORRECT TEXT

Problem Scenario GG : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2) 
val b = a.keyBy(_.length) 
val c = sc.parallelize(List("ant", "falcon", "squid"), 2) 
val d = c.keyBy(_.length) 
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below.

```
Array[(Int, String)] = Array((4,lion))
```

**Answer:**

```
b.subtractByKey(d).collect
```

