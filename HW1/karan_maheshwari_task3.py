import json
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import time
import sys

sc = SparkContext()

review = sys.argv[1]
business = sys.argv[2]

output1 = sys.argv[3]
output2 = sys.argv[4]

review = sc.textFile(review)
review = review.map(lambda x: json.loads(x))

business = sc.textFile(business)
business = business.map(lambda x: json.loads(x))

review = review.map(lambda x: (x['business_id'], x['stars']))
business = business.map(lambda x: (x['business_id'], x['city']))

j = business.join(review).map(lambda x: x[1]).aggregateByKey((0, 0), lambda a, b: (a[0]+b, a[1]+1), lambda a, b: (a[0]+b[0], a[1]+b[1]))
j = j.map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x: (-x[1], x[0]))

start = time.time()
answer = j.collect()
method1_answer = answer[0:10]
end = time.time()

answers = {"m1": end-start}

start = time.time()
method2_answer = j.take(10)
end = time.time()

with open(output1, 'wb+') as fp1:
    fp1.write('city,stars\n'.encode('utf-8'))
    for row in answer:
        fp1.write((row[0]+','+str(row[1])+'\n').encode('utf-8'))

answers["m2"] = end-start

answers["explanation"] = "Since .collect() collects the data from all the partitions into one place and then selects 10 items, it takes more time. When we perform .take(), we are putting a cap to the number of items we collect. Hence, it takes lesser time."

with open(output2, 'w+') as fp2:
    json.dump(answers, fp2)
