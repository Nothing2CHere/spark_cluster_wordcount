#!/usr/bin/env bash

spark-submit \
    --class WordCount \
    --master yarn-cluster \
    wordcount_2.10-0.1.jar yarn-cluster $@

