import os
from graphframes import GraphFrame
from pyspark.sql import SQLContext
from pyspark import SparkContext
import sys
import time

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")


filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
output_file = sys.argv[3]

sc = SparkContext()

sqlContext = SQLContext(sc)

data = sc.textFile(input_file).map(lambda x: (x.split(",")[0], x.split(",")[1])).groupByKey().filter(
	lambda x: len(x[1]) >= filter_threshold).map(lambda x: (x[0], set(x[1]))).collectAsMap()

keys = list(data.keys())

tuples = []
for i in range(len(keys)):
	for j in range(i + 1, len(keys)):
		if len(data[keys[i]].intersection(data[keys[j]])) >= filter_threshold:
			tuples.append((keys[i], keys[j]))
			tuples.append((keys[j], keys[i]))

nodes = list(map(lambda x: [x], list(dict(tuples).keys())))
edges = sqlContext.createDataFrame(tuples).toDF("src", "dst")
vertices = sqlContext.createDataFrame(nodes).toDF("id")

g = GraphFrame(vertices, edges)
result = g.labelPropagation(maxIter=5)
ordered = result.groupBy("label").count().orderBy("count").collect()

answers = []

for row in ordered:
	community = result.select("id").where(result.label == row[0]).collect()
	temp = []
	for row_ in community:
		temp.append(str(row_[0]))
	answers.append(temp)

answers_dict = {}


for i in range(len(answers)):
	try:
		answers_dict[len(answers[i])].append(sorted(answers[i]))
	except:
		answers_dict[len(answers[i])] = [sorted(answers[i])]


f = open(output_file, "w+")

for key in sorted(answers_dict.keys()):
	answers = answers_dict[key]
	for i in range(len(answers)):
		answers[i] = sorted(answers[i])
	answers.sort()
	for row in answers:
		for user in row:
			f.write("'"+str(user) + "', ")
		f.seek(f.tell() - 2, os.SEEK_SET)
		f.truncate()
		f.write("\n")


f.close()
