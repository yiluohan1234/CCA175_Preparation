## ~~Problem 1 - Instructions~~

~~Connect to the MySQL database on the cluster using Sqoop and import data from the sensor table into HDFS. Not all of the MySQL data is required.~~

### ~~Data Description~~

~~A MySQL instance is running on the **gateway** node with the **sensor** table.~~

~~MySQL database information:~~

* ~~Installation: On the cluster node gateway~~
* ~~Database name: problem1~~
* ~~Table name: sensor~~
* ~~Username: cloudera~~
* ~~Password: cloudera~~

### ~~Output Requirements~~
* ~~Place the files in the HDFS directory **/user/cert/problem1/solution/**~~
* ~~Use a text format with a comma as the columnar delimiter~~
* ~~Load every sensor record~~
* ~~Only load columns that contain time, sensor id, and customer~~

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

## Problem 4 - Instructions

Existing customer JSON files need to be converted into a compressed Avro file format

### Data Description

There are 25 million records stored in the HDFS directory **/user/cert/problem4/data/customer** in the JSON file format.
Each record contains fourteen columns.

### Output Requirements

* Place the files in the HDFS directory **/user/cert/problem4/solution/**
* The solution files should use the Avro file format with Snappy compression
* The schema of the Avro records should be the same as input JSON files

## Problem 5 - Instructions

Calculate how many employees live in each city of the country.

### Data Description

Records are stored in the HDFS directory **/user/cert/problem5/data/employee/** The files contain many columns, including address information for the employee

|Column|Type|
|---|---|
|id|int|
|fname|string|
|lname|string|
|address|string|
|city|string|
|state|string|
|zip code|int|
|birth date|string|
|hire date|string|

### Output Requirements

* Place the files in the HDFS directory **/user/cert/problem5/solution/**
* Use a text format with a comma as the columnar delimiter
* The result should contain a single entry for each city
* Output the city, the state, and the total number of all employees that live in that city

|city|state|number of employee|
|---|---|---|
|St. Paul|MN|48|
|Minneapolis|MN|91|
|Minneapolis|NC|39|

## Problem 6 - Instructions

Output all of the customers who live in the state of Texas.

### Data Description

Customer records are stored in the HDFS directory **/user/cert/problem6/data/customer/** in an Avro
file format.

### Output Requirements

* Place the result data in the HDFS directory **/user/cert/problem6/solution/**
* Use a text format for the output files
* The output should only contain records that have a state values of ‘TX’
* The output should only contain the customer’s full name(first, space, last)

|full name|
|---|
|Erma Campbell|
|Patrick Cole|
|Byron Bradley|

## Problem 7 - Instructions

[---]

### Data Description

a tab-delimited text format.

|Name|Type|
|---|---|
|Id|Int|
|fname|String|
|lname|String|
|Address|String|
|City|String|
|State|String|
|Zip|string|

Billing records are stored in the HDFS directory **/user/cert/problem7/data/billing/** in a tab-delimited text format.
The **id** field is a foreign key to the customer that owns the bill.

|Name|Type|
|---|---|
|Id|Int|
|Amount|Float|
|Code|String|
|Billdate|timestamp|

### Output Requirements

* Place the result data in the HDFS directory **/user/cert/problem7/solution/**
* Use a text format with a tab as the columnar delimiter
* The first column should be the customer’s full name(first, space, last)
* The second column should be the amount of money of a single billing transaction

|full name|Amount|
|---|---|
|Gwendolyn Ware|0.37|
|Juan Gloson|2.00|
|James Perez|1.69|

## Problem 8 - Instructions

Generate a report of all customers sorted by last name.

### Data Description

Customer records are stored in the HDFS directory **/user/cert/problem8/data/customer/** in a Parquet file format.

### Output Requirements

* Place the result data in the HDFS directory **/user/cert/problem8/solution/**
* Use an uncompressed Parquet file format for the output files
* The output should contain the customer’s last name and the customer’s first name
* Sort by last names, it is not necessary to sort identical last names secondarily by first name

|lname|fname|
|---|---|
|Abbott|Cindy|
|Baker|Mark|
|Cortez|Ryan|
## Problem 9 - Instructions

Your coworkers only need a subset of the fields in the employee records. Create a smaller dataset for them.

### Data Description

Employee records are stored in the HDFS directory **/user/cert/problem9/data/employee/** in a tab- delimited text format. There are 20 columns.

### Output Requirements

* Place the files in the HDFS directory **/user/cert/problem9/solution/**
* Use a text format with a pipe character ‘|’ as the columnar delimiter
* Only store the first seven columns, including the id, name, and address information

|id|fname|lname|address|city|state|zipcode|
|---|---|---|---|---|---|---|
|8525387|Morris|Lynn|221 Fairview Road|Adonis|MO|64206-0498|
|8525386|Stanley|Greene|4734 Walnut Street|Jesterville|MD|56338-4986|
