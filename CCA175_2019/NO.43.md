## NO.43 CORRECT TEXT

Problem Scenario 67 : You have been given below code snippet.

```
lines = sc.parallelize(['lts fun to have fun,','but you have to know how.'])
r1 = lines.map( lambda x: x.replace(',',' ').replace('.',' ').lower())
r2 = r1.flatMap(lambda x:x.split())
r3 = r2.map(lambda x: (x, 1))
operation1
r5 = r4.map(lambda x:(x[1],x[0]))
r6 = r5.sortByKey(ascending=False)
r6.take(20)
```

Write a correct code snippet for operation1 which will produce desired output, shown below.

```
[(2, 'fun'), (2, 'to'), (2, 'have'), (1, its'), (1, 'know'), (1, 'how'), (1, 'you'), (1, 'but')]
```

**Answer:**

```
r4 = r2.reduceByKey(lambda x,y:x+y)
```

