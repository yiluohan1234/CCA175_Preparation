# CCA175

## 一、考试大纲

### ~~数据采集~~

~~这包括以下内容：~~

~~使用Sqoop将数据从MySQL数据库导入HDFS~~

~~使用Sqoop从HDFS导出数据到MySQL数据库~~

~~使用Sqoop更改导入期间数据的分隔符和文件格式~~

~~将实时和近乎实时的流数据导入HDFS~~

### 处理流数据，因为它被加载到群集上

使用Hadoop文件系统命令将数据加载到HDFS中转换，分批，储存

将存储在HDFS中的给定格式的一组数据值转换为新的数据值或新的数据格式，并将其写入HDFS。

从HDFS加载RDD数据，用于Spark应用程序

使用Spark将RDD的结果写回HDFS

以各种文件格式读取和写入文件

对数据执行标准提取，变换，加载（ETL）过程

### 数据分析

使用Spark SQL在应用程序中以编程方式与metastore进行交互。通过使用查询加载数据生成报告。

使用转移表作为Spark应用程序的输入源或输出接收器

了解在Spark中查询数据集的基本原理

使用Spark过滤数据

编写计算聚合统计信息的查询

使用Spark加入不同的数据集

生成排名或排序数据

### 配置

这是一个实操的考试，考生不仅要会编写代码，也应该熟悉整个开发环境

提供命令行方式，改变你的应用配置，如增加可用内存大小



## 二、环境

~~旧版：vmware虚拟机版cloudera-quickstart-vm-5.13.0-0[安装地址](https://www.cloudera.com/downloads/quickstart_vms/5-13.html)~~

新版：自己搭建开发测试环境，[安装地址](https://github.com/yiluohan1234/centos_env)

## 三、数据集

Download this data from https://github.com/dgadiraju/data

