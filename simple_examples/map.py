"""
A new RDD is returned by applying a function to each element in the RDD. In the following example
"""


from pyspark import SparkContext


sc = SparkContext("local", "Map app")
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

# form a key value pair and map every string with a value of 1.
words_map = words.map(lambda x: (x, 1))  # return new RDD 


mapping = words_map.collect()  # all the elements in the RDD are returned
print(f"Key value pair -> {mapping}")

