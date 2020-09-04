## NO.71 CORRECT TEXT

Problem Scenario 87 : You have been given below three files product.csv (Create this file in hdfs) productID,productCode,name,quantity,price,supplierid

```
1001,PEN,Pen Red,5000,1.23,501
1002,PEN,Pen Blue,8000,1.25,501
1003,PEN,Pen Black,2000,1.25,501
1004,PEC,Pencil 2B,10000,0.48,502
1005,PEC,Pencil 2H,8000,0.49,502
1006,PEC,Pencil HB,0,9999.99,502
2001,PEC,Pencil 3B,500,0.52,501
2002,PEC,Pencil 4B,200,0.62,501
2003,PEC,Pencil 5B,100,0.73,501
2004,PEC,Pencil 6B,500,0.47,502
```

supplier.csv
supplierid,name,phone 

```
501,ABC Traders,88881111
502,XYZ Company,88882222
503,QQ Corp,88883333
```

products_suppliers.csv 
productID,supplierID 

```
2001,501
2002,501
2003,501
2004,502
2001,503
```

Now accomplish all the queries given in solution.
Select product, its price , its supplier name where product price is less than 0.6 using SparkSQL
**Answer:**

```
case class product(productID:Int, productCode:String, name:String, quantity:Float, price:Float, supplierid:Int)
case class supplier(supplierid:Int, name:String, phone:String)
case class product_supplier(productID:Int, supplierID:Int)
val products = sc.textFile("/user/cuiyufei/cca/spark88/product.csv")
val suppliers = sc.textFile("/user/cuiyufei/cca/spark88/supplier.csv")
val products_suppliers = sc.textFile("/user/cuiyufei/cca/spark88/products_suppliers.csv")
val productsDF = products.map(_.split(",")).map(p => product(p(0).toInt, p(1), p(2), p(3).toFloat, p(4).toFloat, p(5).toInt)).toDF
productsDF.registerTempTable("product")
val suppliersDF = suppliers.map(_.split(",")).map(p => supplier(p(0).toInt, p(1), p(2))).toDF
suppliersDF.registerTempTable("supplier")
val products_suppliersDF = products_suppliers.map(_.split(",")).map(p => product_supplier(p(0).toInt, p(1).toInt)).toDF
products_suppliersDF.registerTempTable("products_suppliers")
val result = sqlContext.sql("select a.name as product_name, a.price, b.name as supplier_name  from product a join supplier b on a.supplierid=b.supplierid and a.price < 0.6")
```

注意：case class 中的名字，不能和变量的名字重名

