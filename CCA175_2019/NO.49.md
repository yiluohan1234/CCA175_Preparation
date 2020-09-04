## NO.49 CORRECT TEXT

Problem Scenario 94 : You have to run your Spark application on yarn with each executor 20GB and number of executors should be 50. Please replace XXX, YYY, ZZZ export HADOOP_CONF_DIR=XXX

```
./bin/spark-submit \
--class com.hadoopexam.MyTask \
xxx\ 
--deploy-mode cluster \ #can be client for client mode YYY\
ZZZ \
/path/to/hadoopexam.jar \
1 000
```

Answer:

```
XXX: --master yarn
YYY: --executor-memory 20G 
ZZZ: --num-executors 50
```

注：

```
spark-shell -h
注意不同模式下的不同参数
```

