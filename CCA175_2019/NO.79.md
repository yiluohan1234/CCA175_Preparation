## NO.79 CORRECT TEXT

Problem Scenario 95 : You have to run your Spark application on yarn with each executor
Maximum heap size to be 512MB and Number of processor cores to allocate on each executor will be 1 and Your main application required three values as input arguments V1 V2 V3.
Please replace XXX, YYY, ZZZ
./bin/spark-submit -class com.hadoopexam.MyTask --master yarn-cluster --num-executors 3 --driver-memory 512m XXX YYY lib/hadoopexam.jar ZZZ
**Answer:**

```
XXX:--executor-memory 512M
YYY:--excutor-cores 1
ZZZ:V1 V2 V3
```

注：`spark-shell -h`