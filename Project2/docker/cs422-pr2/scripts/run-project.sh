#!/bin/bash
#$SPARK_HOME/bin/spark-submit --class streaming.Main --master spark://master:7077 /root/jars/samples/cs422-project2_2.11-0.1.0.jar /samples /samples-results

$SPARK_HOME/bin/spark-submit --class streaming.Main /root/jars/cs422-project2_2.11-0.1.0.jar /stream_input 5 5 precise