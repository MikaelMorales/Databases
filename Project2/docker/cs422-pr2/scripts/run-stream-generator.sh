#!/bin/bash
java -jar /root/jars/streamgenerator.jar hdfs://master:9000 /samples/stream.tsv /stream_input 50 10
