from pyspark import SparkContext
import sys


sc = SparkContext()
business_id = 0
business = {}
user_id = 0
users = {}

inputfile = sys.argv[1]
outputfile = sys.argv[2]

def customHasher2(x):
	l = 12000
	return [(20*x + 174) % l,
	(158*x + 163) % l,
	(37*x + 135) % l,
	(27*x + 150) % l,
	(127*x + 5) % l,
	(191*x + 99) % l,
	(173*x + 157) % l,
	(154*x + 152) % l,
	(173*x + 56) % l,
	(184*x + 166) % l,
	(103*x + 132) % l,
	(50*x + 146) % l,
	(24*x + 17) % l,
	(190*x + 92) % l,
	(125*x + 164) % l,
	(119*x + 135) % l,
	(1*x + 25) % l,
	(100*x + 21) % l,
	(184*x + 61) % l,
	(197*x + 114) % l,
	(192*x + 60) % l,
	(44*x + 102) % l,
	(22*x + 177) % l,
	(142*x + 165) % l,
	(11*x + 188) % l,
	(109*x + 160) % l,
	(2*x + 187) % l,
	(3*x + 104) % l,
	(14*x + 186) % l,
	(26*x + 116) % l,
	(111*x + 141) % l,
	(121*x + 12) % l,
	(48*x + 0) % l,
	(43*x + 76) % l,
	(144*x + 96) % l,
	(157*x + 122) % l,
	(45*x + 65) % l,
	(134*x + 174) % l,
	(86*x + 101) % l,
	(29*x + 88) % l,
	(78*x + 127) % l,
	(155*x + 145) % l,
	(41*x + 106) % l,
	(79*x + 160) % l,
	(157*x + 161) % l,
	(49*x + 165) % l,
	(181*x + 163) % l,
	(120*x + 163) % l,
	(6*x + 53) % l,
	(188*x + 116) % l,
	(42*x + 34) % l,
	(88*x + 1) % l,
	(95*x + 33) % l,
	(52*x + 171) % l,
	(120*x + 79) % l,
	(127*x + 70) % l,
	(13*x + 122) % l,
	(59*x + 9) % l,
	(22*x + 162) % l,
	(3*x + 86) % l,
	(119*x + 168) % l,
	(1*x + 118) % l,
	(196*x + 94) % l,
	(152*x + 93) % l,
	(117*x + 43) % l,
	(158*x + 4) % l,
	(5*x + 66) % l,
	(91*x + 72) % l,
	(189*x + 160) % l,
	(89*x + 20) % l,
	(155*x + 8) % l,
	(167*x + 106) % l,
	(15*x + 75) % l,
	(75*x + 194) % l,
	(165*x + 179) % l]

def binHasher(x):
	x = hash(tuple(x))
	return x

n = 75

def createSignatures(x):

	x = list(x)

	sigMatrix = [(j, [99999999999 for i in range(len(business))]) for j in range(n)]

	for row in x:
		ones = [i for i, val in enumerate(row[1]) if val == 1]

		for i in range(len(sigMatrix)):
			for j in ones:
				if row[2][i] < sigMatrix[i][1][j]:
					sigMatrix[i][1][j] = row[2][i]

	for row in sigMatrix:
		yield row

def minner(x):
	min = [9999] * len(x[1][0])
	for i in range(len(x[1])):
		for j in range(len(x[1][i])):
			if x[1][i][j]<min[j]:
				min[j] = x[1][i][j]
	return (x[0], min)


data = sc.textFile(inputfile).map(lambda x: (x.split(",")[0], x.split(",")[1])).filter(lambda x: "user" not in x[0]).groupByKey().persist()


for b in set(data.flatMap(lambda x: list(x[1])).collect()):
	business[b] = business_id
	business_id += 1

for u in data.keys().collect():
	users[u] = user_id
	user_id += 1

changed_data = sc.textFile(inputfile).map(lambda x: (x.split(",")[1], x.split(",")[0])).filter(lambda x: "user" not in x[1]).groupByKey().map(lambda x: (business[x[0]], list(x[1]))).mapValues(lambda x: [users[i] for i in x]).collectAsMap()

def makeMatrix(x):

	x = list(x)

	for user, business_vector in x:
		bool_vector = [0 for i in range(len(business))]
		for b in business_vector:
			bool_vector[business[b]] = 1

		yield (users[user], bool_vector)


n = 75
b = 25
r = 3

def rangePartitioner(x):
	i = 0
	while True:
		if i*r<= x <(i+1)*(r):
			return i
		else:
			i += 1

def invert(x):

	x = list(x)
	m = []
	for row in x:
		m.append(row[1])
	m = [[m[j][i] for j in range(len(m))] for i in range(len(m[0]))]
	for i in range(len(m)):
		yield (i, m[i])

def calculate_jaccard(b1, b2):

	b1 = set(changed_data[b1])
	b2 = set(changed_data[b2])

	return len(b1.intersection(b2))/len(b1.union(b2))


def compare_vectors_in_bands(partNum, x):

	x = list(x)

	bins = {}

	for i in range(len(x)):
		try:
			bins[binHasher(x[i][1])%10000].append(x[i])
		except:
			bins[binHasher(x[i][1])%10000] = [x[i]]

	for key in bins:
		entries = bins[key]

		for i in range(len(entries)):

			for j in range(i+1, len(entries)):

				js = calculate_jaccard(entries[i][0], entries[j][0])

				if js >= 0.5:

					yield(entries[i][0], entries[j][0], js)


data = data.mapPartitions(makeMatrix)

data = data.map(lambda x: (x[0], x[1], customHasher2(x[0])))

data = data.mapPartitions(createSignatures)

data = data.groupByKey().map(lambda x: (x[0], list(x[1]))).map(minner).sortByKey()

data = data.partitionBy(numPartitions=b, partitionFunc=rangePartitioner)

data = data.mapPartitions(invert)

signatures = dict(data.map(lambda x: (x[0], list(x[1]))).collect())

data = data.mapPartitionsWithIndex(compare_vectors_in_bands).distinct().collect()

inverted_dict = dict([[v,k] for k,v in business.items()])

for i in range(len(data)):

	data[i] = list(data[i])
	pair = [data[i][0], data[i][1]]
	pair[0] = inverted_dict[pair[0]]
	pair[1] = inverted_dict[pair[1]]
	pair = sorted(pair)
	data[i][0] = pair[0]
	data[i][1] = pair[1]

data = sorted(data, key = lambda x: x[0])

f = open(outputfile,"w+")
f.write("business_id_1"+","+"business_id_2"+","+"similarity"+"\n")
for i in range(len(data)):
	f.write(data[i][0]+","+data[i][1]+","+str(data[i][2])+"\n")
f.close()
