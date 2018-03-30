"""
all the elements in the RDD are returned 
"""

from pyspark import SparkContext 

sc = SparkContext('local', 'Collect app')

words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)

coll = words.collect()
print(f"Elements in RDD -> {coll}")