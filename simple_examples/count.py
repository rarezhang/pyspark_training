"""
number of elements in the RDD is returned  
"""


from pyspark import SparkContext

# SparkContext as sc 
sc = SparkContext("local", "count app")

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


counts = words.count()
print(f"number of elements in RDD: {counts}")