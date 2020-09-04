## NO.24 CORRECT TEXT

Problem Scenario 60 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"}, 3} 
val b = a.keyBy(_.length) 
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","woif","bear","bee"), 3) 
val d = c.keyBy(_.length) 
operation1
```

Write a correct code snippet for operationl which will produce desired output, shown below. 

```
Array[(lnt, (String, String))] = Array((6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)),
(6,(salmon,turkey)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)), (3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))
```

**Answer:**

```
b.join(d).collect
```

