## NO.27 CORRECT TEXT

Problem Scenario 39 : You have been given two files spark16/file1.txt

```
1,9,5
2,7,4
3,8,3
```

spark16/file2.txt

```
1,g,h
2,i,j
3,k,l
```

Load these two tiles as Spark RDD and join them to produce the below results

```
(l, ((9,5), (g,h)))
(2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
```

And write code snippet which will sum the second columns of above joined results (5+4+3).



**Answer:**

```
val a = sc.textFile("/user/cloudera/exam/task27/file1.txt").map(_.split(","))
val one = a.map(line =>(line(0),(line(1),line(2))))
#a.map{case Array(a,b,c) => (a,(b,c))}
# val c = a.map{case p => (p(0), (p(1), p(2)))}
#val one = sc.textFile("/user/cloudera/exam/task27/file1.txt").map(_.split(",",-1) match {case Array(a, b, c) => (a, ( b, c)) })
val b = sc.textFile("/user/cuiyufei/cca/spark16/file2.txt").map(_.split(","))
val two = b.map(line =>(line(0),(line(1),line(2))))
val joined = one.join(two)
val result = joined.map{case(_, ((_,num2),(_,_)))=>num2.toInt}.reduce(_ + _)

val one = sc.textFile("/user/cuiyufei/cca/spark16/file1.txt").map(_.split(",",-1) match {case Array(a, b, c) => (a, ( b, c)) }
```