from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import json
import binascii
import datetime
import sys

output_file = sys.argv[2]
number_of_hashes = 14


def flajolet_martin(rdd):

	f = open(output_file, "a+")

	maxs = [0]*number_of_hashes

	len_trailing_zeros = [0]*number_of_hashes

	unique = {}

	rdd = rdd.collect()

	for record in rdd:

		business = json.loads(record)

		hash_ = int(binascii.hexlify(business['city'].encode('utf8')), 16)

		hash_ = [hash_ % 47, hash_ % 97, hash_ % 37, hash_ % 237, hash_ % 10, hash_ % 67, hash_ % 23, hash_ % 331, hash_ % 29, hash_ % 251, hash_ % 43, hash_ % 87]

		bin_strings = [format(i, '016b') for i in hash_]

		for i in range(len(bin_strings)):

			if len(bin_strings[i]) != len(bin_strings[i])-len(bin_strings[i].rstrip('0')):
				len_trailing_zeros[i] = len(bin_strings[i])-len(bin_strings[i].rstrip('0'))

		for i in range(len(len_trailing_zeros)):
			if len_trailing_zeros[i] > maxs[i]:
				maxs[i] = len_trailing_zeros[i]

		unique[business['city']] = 1

	a = []

	for i in range(0, len(maxs), 2):
		a.append(int(2**maxs[i]+2**maxs[i+1]/2))

	a = sorted(a)

	median = str(a[3])

	f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(len(unique)) + "," + median + "\n")

	f.close()


if __name__ == "__main__":

	f = open(output_file, "w+")
	f.write("Time,Gound	Truth,Estimation\n")
	f.close()

	sc = SparkContext()
	sc.setLogLevel(logLevel="ERROR")

	port = int(sys.argv[1])
	scc = StreamingContext(sc, 5)
	streaming_c = scc.socketTextStream("localhost", port)

	windowed_streaming_c = streaming_c.window(30, 10)
	windowed_streaming_c.foreachRDD(flajolet_martin)

	scc.start()
	scc.awaitTermination()
