## NO.46 CORRECT TEXT

Problem Scenario 92 : You have been given a spark scala application, which is bundled in jar named
hadoopexam.jar.
Your application class name is com.hadoopexam.MyTask
You want that while submitting your application should launch a driver on one of the cluster node.
Please complete the following command to submit the application.



```
spark-submit XXX --master yarn YYY SSPARK HOME/lib/hadoopexam.jar 10
```

**Answer：**

```
XXX:--class com.hadoopexam.MyTask
YYY:--deploy_mode cluster
```

注：spark-shell -h

```
$SPARK_HOME/bin/spark-submit --class com.unicom.cuiyufei.O_statics \
    --master spark://172.18.50.72:7077 \
    --total-executor-cores 225 \
    --executor-memory 6g \
    --executor-cores 3 \
    --driver-memory 1g \
    --conf spark.default.parallelism=500 \
    --conf spark.shuffle.consolidateFiles=true \
    --conf spark.storage.memoryfraction=0.8 \
    --conf spark.shuffle.memoryfraction=0.3 \
    $CUR/IOT.jar \
    $month $prov_id
```

