#!/bin/bash
echo === Cancelled Flights Bonus ===
spark-submit	--master yarn-client	\
		--executor-memory 512m	\
		--num-executors 3	\
		--executor-cores 1	\
		--driver-memory 512m	\
		fnl.bn.py
exit 0
