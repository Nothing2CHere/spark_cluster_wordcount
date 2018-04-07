#!/usr/bin/env bash

scala_version=2.10.6


ssh ${1%:*} 'mkdir -p wordcount'
scp ../target/scala-${scala_version%.*}/wordcount_2.10-0.1.jar $1/wordcount
scp run_spark_app.sh $1/wordcount

