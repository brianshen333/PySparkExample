import sys
from pyspark import SparkConf, SparkContext

def main(sc):

  # BGN
  # insert your code here
  rawdata = sc.textFile("hdfs://sandbox.hortonworks.com:8020/user/lab/carrier-performance.csv")
  header = rawdata.first()
  headercontext = sc.parallelize([header])
  data = rawdata.subtract(headercontext)

  result = data.map(lambda x: x.split(','))\
        .filter(lambda x:x[10] == "1")\
        .map(lambda x: x[7]+"\\"+x[11]+","+x[10])\
        .map(lambda x: x.split(','))\
        .map(lambda x: (x[0], int(float(x[1]))))\
        .reduceByKey(lambda a,b: a+b)\
        .map(lambda(k,y):(y,k)) \
        .sortByKey(0)\
        .map(lambda(k,y):(y,k))

  # END

  for line in result.take(40):
        print line

if __name__ == "__main__":
  conf = SparkConf().setAppName("CancelledFlights")
  sc  = SparkContext(conf = conf)
  main(sc)
  sc.stop()

