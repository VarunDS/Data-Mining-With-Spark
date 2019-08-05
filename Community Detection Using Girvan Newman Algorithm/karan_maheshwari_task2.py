from pyspark import SparkContext
import time
import sys
import os
from operator import itemgetter

start = time.time()

sc = SparkContext()

filter_threshold = int(sys.argv[1])
input_file_path = sys.argv[2]
task2_1_output_file = sys.argv[3]
task2_2_output_file = sys.argv[4]

data = sc.textFile(input_file_path).map(lambda x: (x.split(",")[0], x.split(",")[1])).groupByKey().filter(lambda x: len(x[1]) >= filter_threshold).map(lambda x: (x[0], set(x[1]))).collectAsMap()

keys = list(data.keys())

tuples = []
for i in range(len(keys)):
	for j in range(i+1, len(keys)):
		if len(data[keys[i]].intersection(data[keys[j]])) >= filter_threshold:
			tuples.append((keys[i], keys[j]))
			tuples.append((keys[j], keys[i]))

data = sc.parallelize(tuples).groupByKey()

graph_as_dic = data.mapValues(list).collectAsMap()

total_number_of_edges = len(tuples)/2
adjacency_matrix = graph_as_dic

data = data.keys()


def formBFS(start_node):

	level = 1

	bfs = [(start_node, level)]

	to_visit = [start_node]

	visited = [start_node]

	get_nodes_at_level = {1: [start_node]}

	while len(to_visit) > 0:

		level += 1
		get_nodes_at_level[level] = []

		new_nodes = []

		for node_ in to_visit:

			connected_nodes = graph_as_dic[node_]

			for c_node in connected_nodes:

				if c_node not in visited:

					visited.append(c_node)

					bfs.append((c_node, level))
					get_nodes_at_level[level].append(c_node)

					if c_node not in new_nodes:

						new_nodes.append(c_node)

		to_visit = new_nodes
		if len(get_nodes_at_level[level]) == 0:
			del(get_nodes_at_level[level])

	return bfs, get_nodes_at_level


def calculate_betweenness(bfs_list, get_nodes_at_level):

	edge_scores = {}
	node_scores = {}

	get_level = dict(bfs_list)

	for level in sorted(list(get_nodes_at_level.keys()), reverse=True):

		nodes = get_nodes_at_level[level]

		for node in nodes:

			try:
				node_scores[node] += 1.0
			except:
				node_scores[node] = 1.0

			nodes_connected_to_this_node = graph_as_dic[node]

			distribute_score = []

			final_count = 0

			for c_node in nodes_connected_to_this_node:

				if get_level[c_node] < level:

					count = 0

					level_of_c_node_1 = get_level[c_node]-1

					try:
						in_nodes = get_nodes_at_level[level_of_c_node_1]

						for i_node in in_nodes:

							if i_node in graph_as_dic[c_node]:

								count += 1

						distribute_score.append((c_node, count))
						final_count += count
					except:
						distribute_score.append((c_node, 1))
						final_count += 1

			# if main node is reached, len(distribute_score) will be 0, this will avoid ZeroDivisionError error
			try:
				d_score = node_scores[node]/final_count
			except:
				continue

			for c_node in distribute_score:

				try:
					node_scores[c_node[0]] += d_score*c_node[1]
				except:
					node_scores[c_node[0]] = d_score*c_node[1]

				try:
					edge_scores[tuple(sorted([c_node[0], node]))] += d_score*c_node[1]
				except:
					edge_scores[tuple(sorted([c_node[0], node]))] = d_score*c_node[1]

	return edge_scores


def process_node(node):

	bfs, get_nodes_at_level = formBFS(node)
	edge_scores = calculate_betweenness(bfs, get_nodes_at_level)
	return edge_scores.items()


def check_modularity(graph_as_dic_, starting_points_):

	partition_number = 1

	membership_dic = {}

	for start_point in starting_points_:

		if start_point in membership_dic:
			continue

		else:
			membership_dic[start_point] = partition_number
			visited = [start_point]
			while len(visited) > 0:
				node_ = visited.pop(0)
				try:
					connected_nodes = graph_as_dic_[node_]
				except KeyError:
					continue
				for c_node in connected_nodes:
					if c_node not in membership_dic:
						membership_dic[c_node] = partition_number
						visited.append(c_node)
			partition_number += 1

	disjoint_points = set(graph_as_dic_.keys()).difference(set(membership_dic.keys()))

	for start_point in disjoint_points:

		if start_point in membership_dic:
			continue
		else:
			membership_dic[start_point] = partition_number
			visited = [start_point]
			while len(visited) > 0:
				node_ = visited.pop(0)
				try:
					connected_nodes = graph_as_dic_[node_]
				except KeyError:
					continue
				for c_node in connected_nodes:
					if c_node not in membership_dic:
						membership_dic[c_node] = partition_number
						visited.append(c_node)
			partition_number += 1



	#print()

	community = {}

	for k, v in membership_dic.items():
		try:
			community[v].append(k)
		except KeyError:
			community[v] = [k]

	#print(community.keys())

	modularity = 0.0

	for community_num, members in community.items():
		for i in range(len(members)):
			for j in range(i+1, len(members)):
				a_ij = 0
				if members[j] in adjacency_matrix[members[i]]:
					a_ij = 1
				modularity += a_ij - len(adjacency_matrix[members[i]])*len(adjacency_matrix[members[j]])/(2*total_number_of_edges)
	return len(community), community, modularity/(2*total_number_of_edges)

# task 2.1
#def task2_1():


f = open(task2_1_output_file, "w+")
betweenness = data.flatMap(process_node).groupByKey().map(lambda x: (x[0], sum(list(x[1]))/2)).collect()
betweenness = sorted(betweenness, key=lambda x: x[1], reverse=True)
for edge, b in betweenness:
	edge = sorted(edge)
	f.write('(\''+edge[0]+'\', \''+edge[1]+'\'), '+str(b)+'\n')
f.close()


# task 2.2

# move declaration above while loop
#def task2_2():
starting_points = []

final_modularity = -1
final_len_of_community = -1
c = []

while True:

	betweenness = data.flatMap(process_node).groupByKey().map(lambda x: (x[0], sum(list(x[1]))/2)).collect()
	edge_to_be_removed = sorted(betweenness, key=lambda x: x[1], reverse=True)[0][0]
	inverted_edge_to_be_removed = (edge_to_be_removed[1], edge_to_be_removed[0])
	tuples.remove(edge_to_be_removed)
	tuples.remove(inverted_edge_to_be_removed)
	if len(tuples) == 0:
		break
	if edge_to_be_removed[0] not in starting_points:
		starting_points.append(edge_to_be_removed[0])
	if edge_to_be_removed[1] not in starting_points:
		starting_points.append(edge_to_be_removed[1])
	data = sc.parallelize(tuples).groupByKey()
	graph_as_dic = data.mapValues(list).collectAsMap()
	data = data.keys()
	len_of_community, c_, mod = check_modularity(graph_as_dic, starting_points)
	if mod > final_modularity:
		final_modularity = mod
		final_len_of_community = len_of_community
		c = c_


f = open(task2_2_output_file, "w+")
answers = list(c.values())
answers_dict = {}


for i in range(len(answers)):
	try:
		answers_dict[len(answers[i])].append(sorted(answers[i]))
	except:
		answers_dict[len(answers[i])] = [sorted(answers[i])]

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

