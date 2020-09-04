## NO.35 CORRECT TEXT

Problem Scenario 91 : You have been given data in json format as below. 

```
{"first_name":"Ankit", "last_name":"Jain"}
{"first_name":"Amir", "last_name":"Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh", "last_name":"Yadav"}
```

Do the following activity
1 . create employee.json tile locally.
2 . Load this tile on hdfs
3 . Register this data as a temp table in Spark using Python.
4 . Write select query and print this data.
5 . Now save back this selected data in json format.

**Answer:**

sqlContext:read.json

toJSON().saveAsTextFile()

```
from pyspark import SQLContext
sqIContext = SQLContext(sc)
#employee = sqlContext.jsonFile("/user/cuiyufei/cca/employee.json")
employee = sqlContext.read.json("/user/cuiyufei/cca/employee.json")
employee.registerTempTable("EmployeeTab")
employeelnfo = sqlContext.sql("select * from EmployeeTab")
for row in employeelnfo.collect():
	print(row)
employeelnfo.toJSON().saveAsTextFile("/user/cuiyufei/cca/employeeJson1")
```

