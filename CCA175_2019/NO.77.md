## NO.77 CORRECT TEXT

Problem Scenario 30 : You have been given three csv files in hdfs as below.
EmployeeName.csv with the field (id, name) EmployeeManager.csv (id, manager Name) EmployeeSalary.csv (id, Salary)
Using Spark and its API you have to generate a joined output as below and save as a text tile (Separated by comma) for final distribution and output must be sorted by id. ld,name,salary,managerName
EmployeeManager.csv 

```
E01,Vishnu
E02,Satyam
E03,Shiv
E04,Sundar
E05,John
E06,Pallavi
E07,Tanvir
E08,Shekhar
E09,Vinod
E10,Jitendra
```

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

 **Answer:**

```
val EmployeeManagerRDD = sc.textFile("/user/cuiyufei/cca/spark30/EmployeeManager.csv").map(line => (line.split(",")(0), line.split(",")(1)))
val EmployeeNameRDD = sc.textFile("/user/cuiyufei/cca/spark30/EmployeeName.csv").map(line => (line.split(",")(0), line.split(",")(1)))
val EmployeeSalaryRDD = sc.textFile("/user/cuiyufei/cca/spark30/EmployeeSalary.csv").map(line => (line.split(",")(0), line.split(",")(1)))
val joinedRDD = EmployeeManagerRDD.join(EmployeeNameRDD).join(EmployeeSalaryRDD)
val sortedRDD = joinedRDD.sortByKey()
val result = sortedRDD.map(line => line._1 + "," + line._2._1._2 + "," + line._2._2 + "," + line._2._1._1)
result.repartition(1).saveAsTextFile("/user/cuiyufei/cca/spark30_result")
```



