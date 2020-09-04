## NO.53 CORRECT TEXT

Problem Scenario 62 : You have been given below code snippet.

```
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x)) 
operation1
```

Write a correct code snippet for operation1 which will produce desired output, shown below. 

```
Array[(lnt, String)] = Array((3,xdogx), (5,xtigerx), (4,xlionx), (3,xcatx), (7,xpantherx), (5,xeaglex))
```

**Answer:**

```
b.mapValues(line => "x"+line+"x").collect
//b.mapValues("x" + _ + "x").collect
//b.map({case(x,y)=>(x, "x"+y+"x")}).collect
```

