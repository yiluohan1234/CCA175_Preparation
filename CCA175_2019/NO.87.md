## NO.87 CORRECT TEXT

Problem Scenario 36 : You have been given a file named spark8/data.csv (type,name). data.csv

```
1 ,Lokesh
2 ,Bhupesh
2 ,Amit
2 ,Ratan
2 ,Dinesh
1 ,Pavan
1 ,Tejas
2 ,Sheela
1 ,Kumar
1 ,Venkat
```

1. Load this file from hdfs and save it back as (id, (all names of same type)) in results directory. However, make sure while saving it should be

**Answer:**

```
val data = sc.textFile("/user/cloudera/exam/task87/data.csv")
val dataRDD = data.map(line => line.trim).map(line => (line.split(",")(0), line.split(",")(1)))
val swapped = dataRDD.map(item => item.swap)
val combinedOutput = dataRDD.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)
combinedOutput.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark36_reuslt")
```

## 