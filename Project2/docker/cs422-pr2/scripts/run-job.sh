#!/bin/bash

$SPARK_HOME/bin/spark-submit --class Main --master spark://master:7077 /root/jars/samples/cs422-test_2.11-0.1.0.jar /samples /sample-results
