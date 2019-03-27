from pyspark.mllib.recommendation import ALS, Rating
from pyspark import SparkContext
import sys
import time

start = time.time()

case = int(sys.argv[3])
training_file = sys.argv[1]
testing_file = sys.argv[2]
output_file = sys.argv[4]


# Model Based Collaborative Filtering

if case == 1:
	sc = SparkContext()
	business = {}
	business_id = 0

	user = {}
	user_id = 0

	data = sc.textFile(training_file).map(lambda x: x.split(",")).filter(lambda x: "user" not in x[0]).persist()

	for b in data.map(lambda x: x[1]).collect():
		if b not in business:
			business[b] = business_id
			business_id += 1

	for u in data.map(lambda x: x[0]).collect():
		if u not in user:
			user[u] = user_id
			user_id += 1

	inverted_user = dict([[v, k] for k, v in user.items()])
	inverted_business = dict([[v, k] for k, v in business.items()])

	training_data = data.map(lambda x: Rating(user[x[0]], business[x[1]], float(x[2])))
	rank = 10
	numIterations = 10
	model = ALS.train(training_data, rank, numIterations, lambda_= 0.1)

	user_averages = data.map(lambda x: (x[0], float(x[2]))).groupByKey().map(lambda x: (x[0], sum(x[1]) / len(x[1]))).collectAsMap()

	test_data = sc.textFile(testing_file).map(lambda x: x.split(","))

	exists = test_data.filter(lambda x: ("user" not in x[0]) and (x[1] in business))
	does_not_exist = test_data.filter(lambda x: ("user" not in x[0]) and (x[1] not in business)).map(lambda x: (x[0], x[1], user_averages[x[0]], x[2])).collect()

	preds = model.predictAll(exists.map(lambda x: (user[x[0]], business[x[1]]))).map(lambda r: ((r[0], r[1]), r[2]))

	exists = exists.map(lambda x: ((user[x[0]], business[x[1]]), float(x[2])))
	scores = preds.join(exists).map(lambda x: (inverted_user[x[0][0]], inverted_business[x[0][1]], x[1][0], x[1][1])).collect()

	scores = scores + does_not_exist

	f = open(output_file, "w+")
	f.write("user_id" + "," + "business_id" + "," + "prediction" + "\n")
	for row in scores:
		f.write(row[0] + "," + row[1] + "," + str(row[2]) + "\n")

	f.close()

# User Based Collborative Filtering

if case == 2:

	sc = SparkContext()

	data = sc.textFile(training_file).map(lambda x: x.split(",")).filter(lambda x: "user" not in x[0]).persist()

	grouped_by_business_id = dict(data.map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: (x[0], list(x[1]))).collect())

	grouped_by_user_id = dict(data.map(lambda x: (x[0], (x[1], float(x[2])))).groupByKey().map(lambda x: (x[0], list(x[1]))).collect())

	data1 = sc.textFile(testing_file, 8).map(lambda x: x.split(",")).filter(lambda x: "user" not in x[0])

	def keep_only_commons(common_business, dic, target_business):

		d = {}
		for item in list(common_business):
			try:
				d[item] = dic[item]
			except:
				pass
				#print("Problem: ", common_business, dic.keys())

		target_business_rating = 0

		if target_business in dic.keys():
			target_business_rating = dic[target_business]

		return d, target_business_rating


	def getRatings_userBased(x):

		try:
			users_who_rated_this_business = grouped_by_business_id[x[1]]
		except:
			# Incase a business does not exist, we return the average of the ratings of other business
			#print("business ", x[1], " does not exists")
			test = dict(grouped_by_user_id[x[0]])
			return (x[0], x[1], sum(test.values())/len(test.values()), x[2])


		active_user = x[0]
		target_business = x[1]

		users_who_rated_this_business = list(map(lambda x: (x, grouped_by_user_id[x]), users_who_rated_this_business))

		try:
			business_rated_by_active_user = grouped_by_user_id[active_user]
		except:
			return (x[0], x[1], 0, x[2])

		business_rated_by_active_user = dict(business_rated_by_active_user)

		overall_average_rating_of_active_user = sum(business_rated_by_active_user.values())/len(business_rated_by_active_user.values())

		if sum(business_rated_by_active_user.values()) == 0 or len(business_rated_by_active_user.values())==0:
			return (x[0], x[1], 0, x[2])

		business_rated_by_active_user_keys = set(business_rated_by_active_user.keys())

		sum_of_rating_for_target_business = []
		sum_of_weights = 0

		for other_user, business_ratings_by_other_user_list in users_who_rated_this_business:

			business_rated_by_other_user_dict = dict(business_ratings_by_other_user_list)

			overall_average_rating_of_other_user = sum(business_rated_by_other_user_dict.values())/len(business_rated_by_other_user_dict.values())

			common_business = business_rated_by_active_user_keys.intersection(set(business_rated_by_other_user_dict.keys()))

			if len(common_business) == 0:
				continue

			business_rated_by_active_user_dict, _ = keep_only_commons(common_business, business_rated_by_active_user, target_business)
			business_rated_by_other_user_dict, target_business_rating = keep_only_commons(common_business, business_rated_by_other_user_dict, target_business)

			average_rating_of_active_user = sum(business_rated_by_active_user_dict.values())/len(common_business)
			average_rating_of_other_user = sum(business_rated_by_other_user_dict.values())/len(common_business)

			numerator = 0
			distance_of_active_user = 0
			distance_of_other_user = 0

			for item in common_business:

				numerator += (business_rated_by_active_user_dict[item]-average_rating_of_active_user)*(business_rated_by_other_user_dict[item]-average_rating_of_other_user)
				distance_of_active_user += (business_rated_by_active_user_dict[item]-average_rating_of_active_user)**2
				distance_of_other_user += (business_rated_by_other_user_dict[item]-average_rating_of_other_user)**2

			distance_of_active_user **= 0.5
			distance_of_other_user **= 0.5

			# happens when only 1 item is in common
			if numerator == 0:
				continue

			weight = numerator/(distance_of_active_user*distance_of_other_user)
			sum_of_rating_for_target_business.append((weight, target_business_rating-overall_average_rating_of_other_user))

		sum_of_rating_for_target_business = sorted(sum_of_rating_for_target_business, key=lambda x: x[0])
		sum_of_rating_for_target_business_ = 0
		for weight, rat in sum_of_rating_for_target_business:
			sum_of_weights += abs(weight)
			sum_of_rating_for_target_business_ += rat*weight

		# might happen when no co-rated business exist OR only 1 person has rated the business in question
		if sum_of_weights == 0:

				return (x[0], x[1], overall_average_rating_of_active_user, x[2])
		else:
			final_rating = overall_average_rating_of_active_user+sum_of_rating_for_target_business_/sum_of_weights

			if final_rating > 5:
				return (x[0], x[1], overall_average_rating_of_active_user, x[2])
			else:
				return (x[0], x[1], final_rating, x[2])


	data1 = data1.map(lambda x: getRatings_userBased(x)).collect()

	f = open(output_file, "w+")
	f.write("user_id" + "," + "business_id" + "," + "prediction" + "\n")


	for row in data1:
		f.write(row[0]+","+row[1]+","+str(row[2])+"\n")

	f.close()


# Item Based Collaborative Filtering
if case == 3:

	sc = SparkContext()

	data = sc.textFile(training_file).map(lambda x: x.split(",")).filter(lambda x: "user" not in x[0]).persist()

	grouped_by_business_id = dict(data.map(lambda x: (x[1], (x[0], float(x[2])))).groupByKey().map(lambda x: (x[0], list(x[1]))).collect())

	grouped_by_user_id = dict(data.map(lambda x: (x[0], (x[1], float(x[2])))).groupByKey().map(lambda x: (x[0], list(x[1]))).collect())

	data1 = sc.textFile(testing_file, 8).map(lambda x: x.split(",")).filter(lambda x: "user" not in x[0])

	def getRatings_itemBased(x):

		businesses_ratings_by_active_user = grouped_by_user_id[x[0]]

		active_business = x[1]

		try:
			users_that_have_rated_active_business = set(dict(grouped_by_business_id[active_business]).keys())
		except:
			average_rating_by_this_user = dict(grouped_by_user_id[x[0]])
			average_rating_by_this_user = sum(average_rating_by_this_user.values())/len(average_rating_by_this_user.values())
			return (x[0], x[1], average_rating_by_this_user, x[2])

		businesses_ratings_by_active_user = dict(businesses_ratings_by_active_user)

		businesses_rated_by_active_user = set(businesses_ratings_by_active_user.keys())

		weight_rating = []

		num_sum = 0
		den_sum = 0

		for business in businesses_rated_by_active_user:

			other_who_have_rated_this_business = set(dict(grouped_by_business_id[business]).keys())

			users_who_have_rated_this_and_active_business = other_who_have_rated_this_business.intersection(users_that_have_rated_active_business)


			##SEE WHAT TO DO ABOUT THIS
			if len(users_who_have_rated_this_and_active_business) == 0 or len(users_who_have_rated_this_and_active_business) == 1:
				continue


			#Taking overall average
			other_business_avg = sum(dict(grouped_by_business_id[business]).values())/len(dict(grouped_by_business_id[business]).values())
			active_business_avg = sum(dict(grouped_by_business_id[active_business]).values())/len(dict(grouped_by_business_id[active_business]).values())

			numerator = 0
			distance_of_active_business = 0
			distance_of_other_business = 0

			for user in users_who_have_rated_this_and_active_business:

				ratings_of_this_user = dict(grouped_by_user_id[user])

				numerator += (ratings_of_this_user[business]-other_business_avg)*(ratings_of_this_user[active_business]-active_business_avg)
				distance_of_other_business += (ratings_of_this_user[business]-other_business_avg)**2
				distance_of_active_business += (ratings_of_this_user[active_business]-active_business_avg)**2

			#Find out why

			if distance_of_other_business == 0 or distance_of_active_business == 0:
				continue

			weight = numerator/((distance_of_other_business**0.5) * (distance_of_active_business**0.5))

			num_sum += abs(weight)*businesses_ratings_by_active_user[business]
			den_sum += abs(weight)

			weight_rating.append((weight, businesses_ratings_by_active_user[business]))

		average_rating_by_this_user = dict(grouped_by_user_id[x[0]])
		average_rating_by_this_user = sum(average_rating_by_this_user.values())/len(average_rating_by_this_user.values())

		if weight_rating:
			try:
				return (x[0], x[1], num_sum/den_sum, x[2])
			except:
				#print(x, weight_rating)
				return (x[0], x[1], average_rating_by_this_user, x[2])
		else:
			return (x[0], x[1], average_rating_by_this_user, x[2])

	data1 = data1.map(lambda x: getRatings_itemBased(x)).collect()

	f = open(output_file, "w+")
	f.write("user_id" + "," + "business_id" + "," + "prediction" + "\n")

	for row in data1:
		f.write(str(row[0])+","+str(row[1])+","+str(row[2])+"\n")

	f.close()


if case == 4:
	def getPairs(sc):

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


		dataFile = sc.textFile(training_file).map(lambda x: (x.split(",")[0], x.split(",")[1])).filter(lambda x: "user" not in x[0]).groupByKey().persist()

		business_id = 0
		business = {}
		user_id = 0
		users = {}

		for b in set(dataFile.flatMap(lambda x: list(x[1])).collect()):
			business[b] = business_id
			business_id += 1

		for u in dataFile.keys().collect():
			users[u] = user_id
			user_id += 1

		changed_data = sc.textFile(training_file).map(lambda x: (x.split(",")[1], x.split(",")[0])).filter(lambda x: "user" not in x[1]).groupByKey().map(lambda x: (business[x[0]], list(x[1]))).mapValues(lambda x: [users[i] for i in x]).collectAsMap()

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


		dataFile = dataFile.mapPartitions(makeMatrix)

		dataFile = dataFile.map(lambda x: (x[0], x[1], customHasher2(x[0])))

		dataFile = dataFile.mapPartitions(createSignatures)

		dataFile = dataFile.groupByKey().map(lambda x: (x[0], list(x[1]))).map(minner).sortByKey()

		dataFile = dataFile.partitionBy(numPartitions=b, partitionFunc=rangePartitioner)

		dataFile = dataFile.mapPartitions(invert)

		inverted_dict = dict([[v,k] for k,v in business.items()])

		dataFile = dataFile.mapPartitionsWithIndex(compare_vectors_in_bands).distinct().map(lambda x: (x[0], x[1])).groupByKey().map(lambda x: (inverted_dict[x[0]], [inverted_dict[i] for i in x[1]])).mapValues(set).collectAsMap()

		return dataFile

	sc = SparkContext()

	dataFile = getPairs(sc)

	data = sc.textFile(training_file).map(lambda x: x.split(",")).filter(lambda x: "user" not in x[0]).persist()

	grouped_by_business_id = dict(data.map(lambda x: (x[1], (x[0], float(x[2])))).groupByKey().map(lambda x: (x[0], list(x[1]))).collect())

	grouped_by_user_id = dict(data.map(lambda x: (x[0], (x[1], float(x[2])))).groupByKey().map(lambda x: (x[0], list(x[1]))).collect())

	data1 = sc.textFile(testing_file, 8).map(lambda x: x.split(",")).filter(lambda x: "user" not in x[0])

	def getRatings(x):

		businesses_ratings_by_active_user = grouped_by_user_id[x[0]]

		active_business = x[1]

		try:
			users_that_have_rated_active_business = set(dict(grouped_by_business_id[active_business]).keys())
		except:
			average_rating_by_this_user = dict(grouped_by_user_id[x[0]])
			average_rating_by_this_user = sum(average_rating_by_this_user.values())/len(average_rating_by_this_user.values())
			return (x[0], x[1], average_rating_by_this_user, x[2])

		businesses_ratings_by_active_user = dict(businesses_ratings_by_active_user)

		businesses_rated_by_active_user = set(businesses_ratings_by_active_user.keys())

		try:
			businesses_rated_by_active_user = dataFile[active_business].union(businesses_rated_by_active_user)
			#businesses_rated_by_active_user = dataFile[active_business].union(businesses_rated_by_active_user)
		except:
			pass

		weight_rating = []

		num_sum = 0
		den_sum = 0

		for business in businesses_rated_by_active_user:

			other_who_have_rated_this_business = set(dict(grouped_by_business_id[business]).keys())

			users_who_have_rated_this_and_active_business = other_who_have_rated_this_business.intersection(users_that_have_rated_active_business)


			##SEE WHAT TO DO ABOUT THIS
			if len(users_who_have_rated_this_and_active_business) == 0 or len(users_who_have_rated_this_and_active_business) == 1:
				continue

			active_business_avg = 0
			other_business_avg = 0

			#Taking corated average


			#Taking overall average
			other_business_avg = sum(dict(grouped_by_business_id[business]).values())/len(dict(grouped_by_business_id[business]).values())
			active_business_avg = sum(dict(grouped_by_business_id[active_business]).values())/len(dict(grouped_by_business_id[active_business]).values())

			#print("==", other_business_avg, active_business_avg)

			#print(other_business_avg, active_business_avg)

			numerator = 0
			distance_of_active_business = 0
			distance_of_other_business = 0

			for user in users_who_have_rated_this_and_active_business:

				ratings_of_this_user = dict(grouped_by_user_id[user])

				numerator += (ratings_of_this_user[business]-other_business_avg)*(ratings_of_this_user[active_business]-active_business_avg)
				#print("=", numerator, ratings_of_this_user[business], ratings_of_this_user[active_business])
				distance_of_other_business += (ratings_of_this_user[business]-other_business_avg)**2
				distance_of_active_business += (ratings_of_this_user[active_business]-active_business_avg)**2

			#print(numerator, distance_of_other_business, distance_of_active_business)

			#Find out why

			if distance_of_other_business == 0 or distance_of_active_business == 0:
				continue

			weight = numerator/((distance_of_other_business**0.5) * (distance_of_active_business**0.5))

			try:
				num_sum += abs(weight)*businesses_ratings_by_active_user[business]
			except:
				average_rating_by_this_user = dict(grouped_by_user_id[x[0]])
				average_rating_by_this_user = sum(average_rating_by_this_user.values())/len(average_rating_by_this_user.values())
				num_sum += abs(weight)*average_rating_by_this_user
			den_sum += abs(weight)
			try:
				weight_rating.append((weight, businesses_ratings_by_active_user[business]))
			except:
				average_rating_by_this_user = dict(grouped_by_user_id[x[0]])
				average_rating_by_this_user = sum(average_rating_by_this_user.values())/len(average_rating_by_this_user.values())
				weight_rating.append((weight, average_rating_by_this_user))

		average_rating_by_this_user = dict(grouped_by_user_id[x[0]])
		average_rating_by_this_user = sum(average_rating_by_this_user.values())/len(average_rating_by_this_user.values())

		if weight_rating:
			try:
				return (x[0], x[1], num_sum/den_sum, x[2])
			except:
				#print(x, weight_rating)
				return (x[0], x[1], average_rating_by_this_user, x[2])
		else:
			return (x[0], x[1], average_rating_by_this_user, x[2])



	data1 = data1.map(lambda x: getRatings(x)).collect()

	f = open(output_file, "w+")
	f.write("user_id"+","+"business_id"+","+"prediction"+"\n")


	for row in data1:
		f.write(str(row[0])+","+str(row[1])+","+str(row[2])+"\n")
	f.close()
