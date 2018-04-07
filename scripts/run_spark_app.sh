#!/usr/bin/env bash

spark-submit \
    --class $1 \
    --master yarn-cluster \
    wordcount_2.10-0.1.jar yarn-cluster ${@:2}

