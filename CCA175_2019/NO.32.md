## NO.32 CORRECT TEXT

Problem Scenario 61 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3) 
val d = c.keyBy(_.length) 
operationl
```

Write a correct code snippet for operationl which will produce desired output, shown below. 

```
Array[(Int, (String, Option[String]))] = Array((6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))), (6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))), (3,(dog,Some(dog))), (3,(dog,Some(bee))), (3,(rat,Some(dogg)), (3,(rat,Some(cat)j), (3,(rat.Some(gnu))). (3,(rat,Some(bee))), (8,(elephant,None)))
```

**Answer:**

```
b.leftOuterJoin(d).collect
```

