"""
A new RDD is returned containing the elements, which satisfies the function inside the filter. 
"""

from pyspark import SparkContext

sc = SparkContext("local", "Filter app")
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

words_filter = words.filter(lambda x: 'spark' in x)  # a new RDD  
filtered = words_filter.collect()  # All the elements in the RDD are returned.

print(f"Fitered RDD -> {filtered}")