"""
# FPGrowth
# method 1: spark.mllib contains the original API built on top of RDDs.

https://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.fpm.FPGrowth
"""



from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth

sc = SparkContext(master="local", appName="FPGrowth") # launch a JVM and creates a JavaSparkContext

# load data set 
path_data = "./data/sample_fpgrowth.txt"
data = sc.textFile(path_data)   # 'pyspark.rdd.RDD'

# data pre-process 
transactions = data.map(lambda line: line.strip().split(' '))  # pyspark.rdd.PipelinedRDD

# load & train model 
# classmethod train(data, minSupport=0.3, numPartitions=-1)
# data - The input data set, each element contains a transaction.
# minSupport - The minimal support level. (default: 0.3)
# numPartitions - The number of partitions used by parallel FP-growth. A value of -1 will use the same number as input data. (default: -1)

model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
# results 
result = model.freqItemsets().collect()

for f in result:
    print(f)