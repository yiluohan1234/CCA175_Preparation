## NO.95 CORRECT TEXT

Problem Scenario 51 : You have been given below code snippet. 

```
val a = sc.parallelize(List(1, 2, 1, 3), 1)
val b = a.map((_, "b")) 
val c = a.map((_, "c"))
Operation_xyz
```

Write a correct code snippet for Operationxyz which will produce below output. Output:

```
Array[(lnt, (lterable[String], lterable[String]))] = Array( (2,(ArrayBuffer(b),ArrayBuffer(c))), (3,(ArrayBuffer(b),ArrayBuffer(c))),
(1,(ArrayBuffer(b, b),ArrayBuffer(c, c))) )
```

**Answer:**

```
b.cogroup(c).collect
```

