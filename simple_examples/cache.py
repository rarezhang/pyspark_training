"""
Persist this RDD with the default storage level (MEMORY_ONLY). You can also check if the RDD is cached or not.

"""

from pyspark import SparkContext 

sc = SparkContext("local", "Cache app") 


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


words.cache() 
caching = words.persist().is_cached 
print(f"Words got chached: {caching}")