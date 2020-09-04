## ~~Problem 1 - Instructions~~

~~Connect to the MySQL database on the cluster using Sqoop and import data from the employee table into
HDFS. Not all of the MySQL data is required.~~

### ~~Data Description~~

~~A MySQL instance is running on the **gateway** node with the **employee** table.~~

~~MySQL database information:~~

* ~~Installation: On the cluster node gateway~~
* ~~Database name: problem1~~
* ~~Table name: employee~~
* ~~Username: cloudera~~
* ~~Password: cloudera~~

### ~~Output Requirements~~

* ~~Place the files in the HDFS directory **/user/cert/problem1/solution/**~~
* ~~Use a text format with a comma as the columnar delimiter~~
* ~~Load every employee record~~
* ~~Only load columns that employee id, first name, last name, and date of hire~~

## ~~Problem 2 - Instructions~~

~~Use Sqoop to export customer data from HDFS into a MySQL database table. Place the da---solution table in MySQL, which has been created and is currently empty.~~

### ~~Data Description~~

~~The data files are in the HDFS directory **/user/cert/problem2/data/customer/**~~

~~MySQL database information:~~

* ~~Installation: On the cluster node gateway~~
* ~~Database name: problem2~~
* ~~Table name: solution~~
* ~~Username: cloudera~~
* ~~Password: cloudera~~

### ~~Output Requirements~~

* ~~Export all of the customer data from HDFS into the MySQL **solution** table~~
* ~~The solution table already exists in the MySQL database but currently has no rows~~

## Problem 3 - Instructions

Your task is to retrieve billing records that have a large charge associated with them and store those records in compressed Parquet files

### Data Description

There are billing records stored in a metastore table called **billing** in the **problem3** database.

|Column|Type|
|---|---|
|id|int|
|charge|float|
|code|string|
|tstamp|string|

### Output Requirements

* Place the files in the HDFS directory **/user/cert/problem3/solution/**
* Only retrieve billing records that have a charge greater than $10.00
* The files should use the Parquet file format with gzip compression.
* The schema of the Parquet file should be the same as the input metastore table

|id|charge|code|tstamp|
|---|---|---|---|
|5103830|19.41|X1|03/29/15 18:39:34|
|5456102|13.77|GA18|03/29/15 18:40:33|
|5481343|10.33|BB2|03/29/15 18:40:21|

## Problem 4 - Instructions

Customer data needs to be converted into a new file format to improve query performance.

### Data Description

Customer records are stored in the HDFS directory **/user/cert/problem4/data/customer**

### Output Requirements

* Place the results in the HDFS directory **/user/cert/problem4/solution/**
* The solution files should stored in Snappy compressed Parquet files.
* The output should contain the following columns.

|Column|Type|
|---|---|
|id|int|
|fname|string|
|lname|string|
|street|string|
|city|string|
|state|string|
|zip|int|

## Problem 5 - Instructions

Calculate how many customers live in each city of the country.

### Data Description

Records are stored in the HDFS directory **/user/cert/problem5/data/customer/**

The files contain many columns, including address information for the customer.

|Column|Type|
|---|---|
|id|int|
|fname|string|
|lname|string|
|address|string|
|city|string|
|state|string|
|zip code|int|
|home phone|string|
|work phone|string|
|mobile phone||

### Output Requirements

* Place the files in the HDFS directory **/user/cert/problem5/solution/**
* Use a text format with a tab as the columnar delimiter
* The result should contain a single entry for each city
* Output the city, the state, and the total number of all customers that live in that city

|city|state|number of customer|
|---|---|---|
|St. Paul|MN|48|
|Minneapolis|MN|91|
|Minneapolis|NC|39|

## Problem 6 - Instructions

For security purposes, your company needs to refer to employess on particular forms without using their full name. Create a new dataset that stores an employee alias.

### Data Description

Employee records are stored in the HDFS directory **/user/cert/problem5/data/employee/** with a Parquet file format.

### Output Requirements

* Place the files in the HDFS directory **/user/cert/problem6/solution/**
* The data should be stored in Snappy compressed Parquet file
* Create a column called “alias” by taking the first letter of the first name and appending the last
name
* Write out the employee id, first name, last name and alias

|employee id|first name|last name|alias|
|---|---|---|---|
|7499998|Britanney|Hewitt|BHewitt|
|7499999|Carol|Vasquez-Santana|CVasquez-Santana|
|7500000|Matthew|Holmes|MHolmes|

## Problem 7 - Instructions

Create a report for bills owed by customers

### Data Description

Customer records are stored in the HDFS directory **/user/cert/problem7/data/customer/** in a comma delimited text file format.

|Column|
|---|
|id|
|fname|
|lname|
|address|
|city|
|state|
|zip|

Billing records are stored in the HDFS directory **/user/cert/problem7/data/billing/** in a tab-delimited text file format. The custid field is the foreign key to the customer that owns the bill.

|Column|
|---|
|custid|
|amount|
|code|
|billdate|

### Output Requirements

* Place the result data in the HDFS directory **/user/cert/problem7/solution/**
* Use a text format with a tab as the columnar delimiter
* The first column should be the customer’s full name(first, space, last)
* The second column should be the amount of money owed by the customer

|full name|amount|
|---|---|
|Gwendolyn Ware|0.37|
|Juan Gloson|2.00|
|James Perez|1.69|

## Problem 8 - Instructions

Generate a report of all employees and the anniversary of their birthday.

### Data Description

Employee records are stored in the HDFS directory **/user/cert/problem8/data/customer/**
The last two columns in the files are the employees birthday and the date they were hired..

### Output Requirements

* Place the result data in the HDFS directory **/user/cert/problem8/solution/**
* Use text file format with tab as the column delimiter
* The output should contain the employee’s last name and first name as the first column
* The output should contain a month and day of the employee’s birthday as the second column
* Sort the records by anniversary date. It is not necessary sorting employees that have the same
anniversary date

|full name|birthday|
|---|---|
|Rosalyn Sanches|12/18|
|Leslie Woods|12/31|
|Rui Want|12/31|

## Problem 9 - Instructions

Loud Acre Mobile is concerned about phones over-heating. Sensor data is captured from each device in the network on a regular basis. Find the average temperature for each model of phone.

### Data Description

Phone sesor records are stored in the HDFS directory **/user/cert/problem9/data/sensor**

|Column|Type|
|---|---|
|Timestamp|Int|
|Customer ID|Int|
|Phone ID|String|
|Phone Model|String|
|Lattitude|Float|
|Longitude|Float|
|Firmware Version|Int|
|Bluetooth Satus|Int|
|GPS Status|Int|
|WiFi Status|Int|
|Battery Remaining|Float|
|Temperature|Float|
|Signal Strength|Float|

### Output Requirements

* Place the result data in the HDFS directory **/user/cert/problem9/solution/**
* Use a text format with a comma as the columnar delimiter
* The output should be phone model and average temparature

|Phone Model|Average Temperature|
|---|---|
|M5Note6|41.235678344555|
|GixNewISO|40.120853782894|
|T5H20|46.365945723859|
