#!/bin/bash
$SPARK_HOME/bin/spark-submit --class streaming.Main /root/jars/cs422-project2_2.11-0.1.0.jar /stream_input 5 5 approx 0.001 0.01 43.139.101.116 1.103.139.4
