from pyspark import SparkContext
from itertools import combinations
from operator import add
import sys
import time
import os
start = time.time()

def writeOutputToFile(listToWrite, f):
    length = 1
    i = 0
    while i < len(listToWrite):
        item = listToWrite[i]
        if len(item) == length:
            if length == 1:
                f.write('(\''+ str(item[0])+'\'),')
            else:
                f.write(str(item)+',')
            i += 1
        else:
            length += 1
            f.seek(f.tell() - 1, os.SEEK_SET)
            f.truncate()
            f.write("\n")
            f.write("\n")

def getCandidatesOfSizeK(frequentItems, k):

    candidateOfSizeK = []

    if k == 2:
        for i in range(len(frequentItems)):
            for j in range(len(frequentItems)):
                if j>i:
                    newItem = (frequentItems[i], frequentItems[j])
                    candidateOfSizeK.append(tuple(sorted(newItem)))

    else:
        for i in range(len(frequentItems)):
            for j in range(len(frequentItems)):
                if j>i:
                    newItem = set(frequentItems[i]) | set(frequentItems[j])
                    if len(newItem) == k:
                        newItem = tuple(sorted(newItem))
                        if newItem not in candidateOfSizeK:
                            subItems = list(combinations(newItem, k-1))
                            if all(item in frequentItems for item in subItems):
                                candidateOfSizeK.append(tuple(sorted(newItem)))
    return candidateOfSizeK


def apriori(x):
    
    baskets = list(x)
    supportForThisPartition = len(baskets)/totalNumberOfBaskets*support
    counter = {}
    frequentItems = {1:[]}

    for basket in baskets:
        for item in basket:
            try:
                counter[item] += 1
            except:
                counter[item] = 1
                
    for candidateItem in counter:
        if counter[candidateItem] >= supportForThisPartition:
            frequentItems[1].append(candidateItem)

    k = 2

    while len(frequentItems[k-1]) > 0:

        frequentItems[k] = []

        candidateItems = getCandidatesOfSizeK(frequentItems[k-1], k)

        for i in range(len(candidateItems)):
            possibility = candidateItems[i]
            item_count = 0
            for basket in baskets:
                if set(possibility).issubset(basket):
                    item_count += 1
            if item_count >= supportForThisPartition:
                frequentItems[k].append(possibility)

        k += 1

    for key in frequentItems:
        if key != 1:
            for frequentItem in frequentItems[key]:
                yield(tuple(sorted(frequentItem)), 1)
        else:
            for frequentItem in frequentItems[key]:
                yield(tuple([frequentItem]), 1)


def simpleCounter(x):

    baskets = list(x)
    frequent = []

    for candidateItem in candidateItemSet:
        count = 0
        for basket in baskets:
            if set(candidateItem).issubset(basket):
                count += 1
        frequent.append((candidateItem, count))

    for candidateItem in frequent:
        yield candidateItem


sc = SparkContext()

case = int(sys.argv[1])
support = int(sys.argv[2])
inputfile = sys.argv[3]
outputfile = sys.argv[4]


data = sc.textFile(inputfile)

if case == 1:
    data = data.map(lambda x: (x.split(',')[0], x.split(',')[1]))
elif case == 2:
    data = data.map(lambda x: (x.split(',')[1], x.split(',')[0]))

header = data.first()

data = data.filter(lambda x: x != header)

data = data.groupByKey().map(lambda x: set(x[1])).persist()
totalNumberOfBaskets = data.count()

candidateItemSet = data.mapPartitions(apriori).groupByKey().keys().collect()
candidateItemSet = sorted(candidateItemSet)
candidateItemSet = sorted(candidateItemSet, key=len)

frequentItemSet = data.mapPartitions(simpleCounter).reduceByKey(add).filter(lambda x: x[1] >= support).keys().collect()
frequentItemSet = sorted(frequentItemSet)
frequentItemSet = sorted(frequentItemSet, key=len)

f = open(outputfile, "w+")
f.write("Candidates:\n")
writeOutputToFile(candidateItemSet, f)
f.write("\n")
f.write("\n")
f.write("Frequent Itemsets:\n")
writeOutputToFile(frequentItemSet, f)
f.close()

end = time.time()
print("Duration:", end-start)
