# coding= utf-8

"""
PrefixSpan: on RDDs 
"""

# only included in pyspark.mllib.fpm

from pyspark import SparkContext
from pyspark.mllib.fpm import PrefixSpan
import pyspark

sc = SparkContext(master="local", appName="PrefixSpan app") # launch a JVM and creates a JavaSparkContext


# load data set 
"""
data set 
list of list 
items: a b c d e
elements: [a b] [c] [c b] ...
sequence: [[a b] [c]]
"""
data = [
        [["a", "b"], ["c"]],
        [["a"], ["c", "b"], ["a", "b"]],
        [["a", "b"], ["e"]],
        [["f"]]
        ]  # list of list 
 
# data pre-process
# form a distributed dataset that can be operated on in parallel
sequences = sc.parallelize(data, 2)   # 'pyspark.rdd.RDD'

# storage level 
# cache: persist with memory 
"""
sequences.cache() # Persist this RDD with the default storage level
caching = sequences.persist().is_cached
print(f"sequences got chached: {caching}")
"""

# storage level: memory and disk  
"""
class pyspark.StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication = 1)
MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)
"""
sequences.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
print(f"storage level: {sequences.getStorageLevel()}")


# load & train model 
# classmethod train(data, minSupport=0.1, maxPatternLength=10, maxLocalProjDBSize=32000000)
# data - The input data set, each element contains a sequence of itemsets.
# minSupport - The minimal support level of the sequential pattern, any pattern that appears more than (minSupport * size-of-the-dataset) times will be output. (default: 0.1)
# maxPatternLength - The maximal length of the sequential pattern, any pattern that appears less than maxPatternLength will be output. (default: 10)
# maxLocalProjDBSize - The maximum number of items (including delimiters used in the internal storage format) allowed in a projected database before local processing. If a projected database exceeds this size, another iteration of distributed prefix growth is run. (default: 32000000)

model = PrefixSpan.train(sequences, minSupport=0.5)
"""
4 sequences in the data set 
minSupport=0.5
pattern more than 4*0.5=2 times will be output 
FreqSequence(sequence=[['b']], freq=3)
FreqSequence(sequence=[['c']], freq=2)
FreqSequence(sequence=[['a']], freq=3)
FreqSequence(sequence=[['b', 'a']], freq=3)
FreqSequence(sequence=[['a'], ['c']], freq=2)
"""

# results 
result = model.freqSequences().collect()  # list 
"""
# model.freqSequences() --> pyspark.rdd.PipelinedRDD
"""

for f in result:
    print(f)     