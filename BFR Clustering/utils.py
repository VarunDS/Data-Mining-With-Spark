import numpy as np
from sklearn.cluster import KMeans


def get_points(input_file):
	f = open(input_file, "r+")
	points = []
	for line in f:
		line_ = line.replace("\n", "")
		line_ = line_.split(",")
		points.append([int(line_[0]), int(line_[1]), [float(d) for d in line_[2:]]])
	f.close()
	return np.array(points)


def update_stats(s, label, point):
	stats = s[label]
	stats[0] += 1
	stats[1] = [stats[1][i] + point[2][i] for i in range(len(point[2]))]
	stats[2] = [stats[2][i] + point[2][i]**2 for i in range(len(point[2]))]
	s[label] = stats
	return s


def initialize_stats(s, label, point):
	stats = []
	stats.append(1)
	stats.append(point[2])
	stats.append([d**2 for d in point[2]])
	s[label] = stats
	return s


def form_cluster(kmeans, data, k):
	clusters = {}
	for i in range(len(kmeans.labels_)):
		try:
			clusters[kmeans.labels_[i]].append(data[i])
		except KeyError:
			clusters[kmeans.labels_[i]] = [data[i]]
	return clusters


def	bfr_form_rs(clusters):
	clustered_set = []
	retain_set = []
	for key in clusters:
		if len(clusters[key]) == 1:
			retain_set.append(clusters[key][0])
		else:
			for point in clusters[key]:
				clustered_set.append(point)

	return retain_set, clustered_set


def bfr_form_rs_and_cs(clusters, form_track=False):
	retain_set = []
	keys_to_be_discarded = []
	for key in clusters:
		if len(clusters[key]) == 1:
			retain_set.append(clusters[key][0])
			keys_to_be_discarded.append(key)

	for key in keys_to_be_discarded:
		del(clusters[key])

	cs = {}
	cs_track = {}

	for key in clusters:
		for point in clusters[key]:
			try:
				cs = update_stats(cs, key, point)
				cs_track[key].append(point[0])
			except KeyError:
				cs = initialize_stats(cs, key, point)
				cs_track[key] = [point[0]]

	if form_track:
		return retain_set, cs, cs_track

	return retain_set, cs


def bfr_init(data, k):

	ds_track = {}

	initial_data_points = data[:int(len(data)*0.2), :]

	kmeans = KMeans(n_clusters=k*10, random_state=0).fit(np.ndarray.tolist(initial_data_points[:, 2]))

	clusters = form_cluster(kmeans, initial_data_points, k*10)

	retain_set, clustered_set = bfr_form_rs(clusters)

	clustered_set = np.array(clustered_set)

	kmeans = KMeans(n_clusters=k, random_state=0).fit(np.ndarray.tolist(clustered_set[:, 2]))

	ds = {}

	for i in range(len(clustered_set)):
		point = clustered_set[i]
		try:
			ds = update_stats(ds, kmeans.labels_[i], point)
			ds_track[kmeans.labels_[i]].append(point[0])
		except KeyError:
			ds = initialize_stats(ds, kmeans.labels_[i], point)
			ds_track[kmeans.labels_[i]] = [point[0]]
	return ds, np.array(retain_set), ds_track


def bfr_form_cs(retain_set, cs_track):

	if (len(retain_set)) == 1 or (len(retain_set)) == 0:
		return retain_set, {}, {}

	kmeans = KMeans(n_clusters=int(len(retain_set)*0.9), random_state=0).fit(np.ndarray.tolist(retain_set[:, 2]))
	clusters = form_cluster(kmeans, retain_set, int(len(retain_set)*0.9))

	retain_set, cs, cs_track = bfr_form_rs_and_cs(clusters, True)
	return retain_set, cs, cs_track


def calculate_mahalanobis_distance_and_get_cluster(s, point, merge=False):

	dist_dict = {}
	for key in sorted(s.keys()):
		sd = []
		mean = []
		feature_vector = s[key]
		for i in range(len(feature_vector[1])):
			sd.append((feature_vector[2][i]/feature_vector[0] - (feature_vector[1][i]/feature_vector[0])**2)**0.5)
			mean.append(feature_vector[1][i]/feature_vector[0])

		y = [((point[2][i]-mean[i])/sd[i])**2 for i in range(len(point[2]))]
		dist_dict[key] = sum(y)**0.5

	cluster = -1
	distance = max(dist_dict.values())
	for key in dist_dict:
		if dist_dict[key] < distance:
			distance = dist_dict[key]
			cluster = key

	if merge:
		return cluster

	if distance < 2*((len(point[2]))**0.5):
		return cluster
	else:
		return -1


def merge_cs(cs, new_cs, cs_track, new_cs_track):

	if len(new_cs) == 0:
		return cs, cs_track

	new_keys = []
	tbd = []

	for key_ in new_cs:
		point = [0, 0, [i/new_cs[key_][0] for i in new_cs[key_][1]]]
		label = calculate_mahalanobis_distance_and_get_cluster(cs, point)
		if label != -1:
			cs[label][0] += new_cs[key_][0]
			cs[label][1] = [cs[label][1][i]+new_cs[key_][1][i] for i in range(len(new_cs[key_][1]))]
			cs[label][2] = [cs[label][2][i]+new_cs[key_][2][i] for i in range(len(new_cs[key_][2]))]
			for index in new_cs_track[key_]:
				cs_track[label].append(index)
			tbd.append(key_)
		else:
			new_keys.append(key_)

	max_clus_number = max(sorted(cs.keys()))

	for i in range(len(tbd)):
		del(new_cs[tbd[i]])
		del(new_cs_track[tbd[i]])

	for i in range(len(new_keys)):
		cs[max_clus_number+i+1] = new_cs[new_keys[i]]
		cs_track[max_clus_number+i+1] = [p for p in new_cs_track[new_keys[i]]]

	return cs, cs_track


def bfr_main(ds, cs, retain_set, data, ds_track, cs_track):

	consider_for_cs = []

	for i in range(len(data)):
		label = calculate_mahalanobis_distance_and_get_cluster(ds, data[i])
		if label == -1:
			consider_for_cs.append(data[i])
		else:
			ds = update_stats(ds, label, data[i])
			ds_track[label].append(data[i][0])

	consider_for_rs = []

	for i in range(len(consider_for_cs)):
		label = calculate_mahalanobis_distance_and_get_cluster(cs, consider_for_cs[i])
		if label == -1:
			consider_for_rs.append(consider_for_cs[i])
		else:
			cs = update_stats(cs, label, consider_for_cs[i])
			cs_track[label].append(consider_for_cs[i][0])


	if len(consider_for_rs) > 0:
		retain_set = np.concatenate((retain_set, np.array(consider_for_rs)), axis=0)

	retain_set, new_cs, new_cs_track = bfr_form_cs(retain_set, {})

	cs, cs_track = merge_cs(cs, new_cs, cs_track, new_cs_track)

	return ds, cs, retain_set, ds_track, cs_track


def merge_ds_cs(ds, cs, ds_track, cs_track):

	for key_ in cs:
		point = [0, 0, [i/cs[key_][0] for i in cs[key_][1]]]
		label = calculate_mahalanobis_distance_and_get_cluster(ds, point, True)
		ds[label][0] += cs[key_][0]
		ds[label][1] = [ds[label][1][i]+cs[key_][1][i] for i in range(len(cs[key_][1]))]
		ds[label][2] = [ds[label][2][i]+cs[key_][2][i] for i in range(len(cs[key_][2]))]
		for p in cs_track[key_]:
			ds_track[label].append(p)

	return ds, ds_track


def get_number_of_points_in_ds(s):
	count = 0
	for key in s:
		count += s[key][0]

	return count
