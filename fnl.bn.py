import sys
from pyspark import SparkConf, SparkContext
import pyspark_csv as pycsv
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main(sc):

  sc.addPyFile("file:///usr/lib/python2.6/site-packages/pyspark_csv.py")

  # BGN
  # insert your code here
  rawdata = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/lab/carrier-performance.csv")
  header = rawdata.first()
  headercontext = sc.parallelize([header])
  data = rawdata.subtract(headercontext).map(lambda x: x.split(','))

  table = data.map(lambda x:Row(Origin=x[7], OrginCityName=x[8], CancelationCode=x[11], cancelation=x[10], combine=x[7]+x[11])).toDF()



  table.registerTempTable("tablecarrier")

  query = sqlContext.sql("SELECT Origin, OrginCityName, CancelationCode, COUNT(cancelation) FROM tablecarrier GROUP BY combine HAVING cancelation == 1")

  for line in query.show(40): 
	print line

  # END

if __name__ == "__main__":
  conf = SparkConf().setAppName("CancelledFlights")
  sc  = SparkContext(conf = conf)
  main(sc)
  sc.stop()

