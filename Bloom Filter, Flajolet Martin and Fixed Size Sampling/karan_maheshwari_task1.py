from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import json
import binascii
import datetime
import sys

filter_array = [0]*200

present = dict()

fp = 0
tn = 0

filename = sys.argv[2]
port = int(sys.argv[1])


def hashes(x, modder=200):

	return [
		(433*x + 173) % modder
	]


def hash(x, city):

	global filter_array, present

	c = []

	for i in hashes(x):
		c.append(filter_array[i])

	if all(c) == 1:
		try:
			if present[city] == 1:
				return True, 0
		except KeyError:
			return False, 1

	else:
		for i in hashes(x):
			filter_array[i] = 1
		present[city] = 1
		return True, 1


def test(x):

	f = open(filename, "a+")

	global fp
	global tn

	x = x.collect()
	for i in x:
		business = json.loads(i)
		truth_value, count = hash(int(binascii.hexlify(business['city'].encode('utf8')), 16), business['city'])
		if truth_value:
			tn += count
		else:
			fp += count

	fpr = 0

	try:
		fpr = fp/(fp+tn)
	except ZeroDivisionError:
		pass

	f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(fpr) + "\n")

	f.close()


if __name__ == "__main__":

	sc = SparkContext()
	sc.setLogLevel(logLevel="ERROR")

	scc = StreamingContext(sc, 10)
	streaming_c = scc.socketTextStream("localhost", port)

	f = open(filename, "w+")
	f.write("Time,FPR\n")
	f.close()

	streaming_c.foreachRDD(test)

	scc.start()
	scc.awaitTermination()
