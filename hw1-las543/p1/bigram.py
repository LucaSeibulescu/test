import sys

import re
 
from pyspark import SparkContext, SparkConf
 
conf = SparkConf()
sc = SparkContext(conf=conf)

words = sc.textFile(sys.argv[1])

wordCounts = words.flatMap(lambda line: re.compile(r'[^a-zA-Z ]').sub("", line.lower()).split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a,b: a + b)

bigrams = words.map(lambda line: re.compile(r'[^a-zA-Z ]').sub("", line.lower()).split(" ")).flatMap(lambda word: [((word[i], word[i + 1]), 1) for i in range (0, len(word) - 1)])

bigramCounts = bigrams.reduceByKey(lambda x,y: x + y)
bigramCounts = bigramCounts.map(lambda x: (x[0][0], (x[0][1], x[1])))

finalCounts = bigramCounts.join(wordCounts)
finalCounts = finalCounts.map(lambda x: ((x[0], x[1][0][0]), x[1][0][1]/x[1][1]))

finalCounts.coalesce(1, shuffle=True).saveAsTextFile(sys.argv[2])

sc.stop()
