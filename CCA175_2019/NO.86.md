## NO.86 CORRECT TEXT

Problem Scenario 57 : You have been given below code snippet.

```
 val a = sc.parallelize(1 to 9, 3) 
 operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below.

```
Array[(String, Seq[Int])] = Array((even,ArrayBuffer(2, 4, 6, 8)), (odd,ArrayBuffer(1, 3, 5, 7, 9)))
```

**Answer:**

```
a.groupBy(line => if(line%2 ==0) "even" else "odd").collect
```

