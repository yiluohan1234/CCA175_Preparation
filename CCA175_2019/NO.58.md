## NO.58 CORRECT TEXT

Problem Scenario 33 : You have given a files as below. spark5/EmployeeName.csv (id,name) spark5/EmployeeSalary.csv (id,salary)
Data is given below: 

EmployeeName.csv 

```
E01,Lokesh
E02,Bhupesh
E03,Amit
E04,Ratan
E05,Dinesh
E06,Pavan
E07,Tejas
E08,Sheela
E09,Kumar
E10,Venkat
```

EmployeeSalary.csv 

```
E01,50000
E02,50000
E03,45000
E04,45000
E05,50000
E06,45000
E07,50000
E08,10000
E09,10000
E10,10000
```

Now write a Spark code in scala which will load these two tiles from hdfs and join the same, and
produce the (name.salary) values.
And save the data in multiple file group by salary (Means each file will have name of employees with same salary). Make sure file name include salary as well.

**Answer:**

```
val employeeName = sc.textFile("/user/cuiyufei/cca/spark5/EmployeeName.csv")
val employeeNameRDD = employeeName.map(_.split(",")).map(line => (line(0), line(1)))
val employeeSalary = sc.textFile("/user/cuiyufei/cca/spark5/EmployeeSalary.csv")
val employeeSalaryRDD = employeeSalary.map(_.split(",")).map(line => (line(0), line(1)))
val joined = employeeNameRDD.join(employeeSalaryRDD)
val keyRemoved = joined.map({case(id,(name, salary)) => (name,salary)})
//val keyRemoved = joined.values
val rddGroupByKey = keyRemoved.map(line => line.swap).groupByKey()
val rddByKey = rddGroupByKey.map{case (k,v) => k->sc.makeRDD(v.toSeq)}
rddByKey.foreach{ case (k,rdd) => rdd.saveAsTextFile("/user/cuiyufei/cca/spark5/Employee"+k)}
```

