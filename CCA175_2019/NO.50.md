## NO.50 CORRECT TEXT

Problem Scenario 53 : You have been given below code snippet.

```
val a = sc.parallelize(1 to 10, 3) operation1
b.collect
Output 1
Array[Int] = Array(2, 4, 6, 8,10) operation2
Output 2
Array[Int] = Array(1,2,3)
```

Write a correct code snippet for operation1 and operation2 which will produce desired output, shown above.

**Answer:**

```
val b = a.filter(p => p%2==0)
// val b= a.filter(_%2==0)
a.filter(p => p < 4).collect
//a.filter(_<4).collect
```

