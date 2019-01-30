#!/bin/bash
echo === Cancelled Flights ===
spark-submit    --master yarn-client    \
                --executor-memory 512m  \
                --num-executors 3       \
                --executor-cores 1      \
                --driver-memory 512m    \
                fnl.q2.py
exit 0
~
