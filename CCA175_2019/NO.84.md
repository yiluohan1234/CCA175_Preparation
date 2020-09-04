## NO.84 CORRECT TEXT

Problem Scenario 54 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
val b = a.map(x => (x.length, x)) 
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. 

```
Array[(Int, String)] = Array((4,lion), (7,panther), (3,dogcat), (5,tigereagle))
```

**Answer:**

```
b.reducByKey(_ + _).collect
```

