import json
from pyspark import SparkContext
import sys

inputfile = sys.argv[1]
outputfile = sys.argv[2]

answers = {}

sc = SparkContext()

l = sc.textFile(inputfile)

l = l.map(lambda x: json.loads(x))

answers["n_review"] = l.count()

def is2018(x):
    return '2018' in x['date']

l1 = l.filter(is2018).count()

answers["n_review_2018"] = l1

l2 = l.map(lambda x: (x['user_id'], 1)).persist()

answers["n_user"] = l2.distinct().count()

answers["top10_user"] = l2.reduceByKey(lambda a,b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)

l4 = l.map(lambda x: (x['business_id'], 1)).persist()

answers["n_business"] = l4.distinct().count()

answers["top10_business"] = l4.reduceByKey(lambda a,b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)

with open(outputfile, 'w+') as fp:
    json.dump(answers, fp)
