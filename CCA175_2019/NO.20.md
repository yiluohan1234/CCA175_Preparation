## NO.20 CORRECT TEXT

Problem Scenario 48 : You have been given below Python code snippet, with intermediate output. We want to take a list of records about people and then we want to sum up their ages and count them.
So for this example the type in the RDD will be a Dictionary in the format of {name: NAME, age:AGE,
gender:GENDER}.
The result type will be a tuple that looks like so (Sum of Ages, Count) people = [] 

```
people.append({'name':'Amit', 'age':45,'gender':'M'})
people.append({'name':'Ganga', 'age':43,'gender':'F'})
people.append({'name':'John', 'age':28,'gender':'M'})
people.append({'name':'Lolita', 'age':33,'gender':'F'})
people.append({'name':'Dont Know', 'age':18,'gender':'T'})
peopleRdd=sc.parallelize(people) //Create an RDD
peopleRdd.aggregate((0,0), seqOp, combOp) //Output of above line : 167, 5)
```

 Now define two operation seqOp and combOp , such that
seqOp : Sum the age of all people as well count them, in each partition. 
combOp : Combine results from all partitions.

**Answer:**

需要首先判断是scala还是python，在根据要求去分析。

```
seqOp:(lambda x,y:(x[0]+y['age'],x[1]+1))
combOp:(lambda x,y:(x[0]+y[0], x[1]+y[1]))
```

## 注：aggregate算子

```
def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): U
```

aggregate函数首先对每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值（zeroValue）进行combine操作。这个操作返回的类型不需要和RDD中元素类型一致，所以在使用 aggregate()时，需要提供我们期待的返回类型的初始值，然后通过一个函数把RDD中的元素累加起来??放入累加器?。考虑到每个节点是在本地进行累加的，最终还需要提供第二个函数来将累加器两两合并。

其中seqOp操作会聚合各分区中的元素，然后combOp操作会把所有分区的聚合结果再次聚合，两个操作的初始值都是zeroValue. seqOp的操作是遍历分区中的所有元素(T)，第一个T跟zeroValue做操作，结果再作为与第二个T做操作的zeroValue，直到遍历完整个分区。combOp操作是把各分区聚合的结果，再聚合。aggregate函数返回一个跟RDD不同类型的值。因此，需要一个操作seqOp来把分区中的元素T合并成一个U，另外一个操作combOp把所有U聚合。

```scala
scala> val a =  sc.parallelize(List(1,2,3,4))

scala> a.aggregate((0,0))((acc,value) => (acc._1 + value, acc._2 + 1),(acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
res32: (Int, Int) = (10,4)
```

