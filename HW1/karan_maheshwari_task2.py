import json
from pyspark import SparkContext
import time
import sys

sc = SparkContext()

inputfile = sys.argv[1]
outputfile = sys.argv[2]

n = int(sys.argv[3])

l = sc.textFile(inputfile)
l = l.map(lambda x: json.loads(x))
l = l.map(lambda x: (str(x['business_id']), 1)).persist()
l.take(1)

def hasher(x):
    return hash(x)

start = time.time()
l.reduceByKey(lambda a, b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)
end = time.time()

default = {"n_partition": l.getNumPartitions(), "n_items" : l.glom().map(lambda x: len(x)).collect(), "exe_time": end-start}

l = l.partitionBy(n, hasher).persist()
l.take(1)

start = time.time()
l.reduceByKey(lambda a, b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)
end = time.time()

customized = {"n_partition": l.getNumPartitions(), "n_items" : l.glom().map(lambda x: len(x)).collect(), "exe_time": end-start}

answers = {"default": default, "customized": customized, "explanation":"Since we partition the RDD using 'business_id', we are reducing the amount of shuffling that would be required to perform the query with default partitioning. This reduction in shuffling time in turn minimizes the execution time."}

with open(outputfile, 'w+') as fp:
    json.dump(answers, fp)
