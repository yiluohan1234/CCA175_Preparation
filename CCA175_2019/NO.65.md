## NO.65 CORRECT TEXT

Problem Scenario 72 : You have been given a table named "employee2" with following detail. 

first_name string
last_name string
Write a spark script in python which read this table and print all the rows and individual column values.

```
sqIContext = HiveContext(sc)
employee2 = sqlContext.sql("select * from employee2")
for row in employee2.collect(): 
	print(row)
for row in employee2.collect(): 
	print( row.first_name)
```

