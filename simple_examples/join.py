"""
returns RDD with a pair of elements with the matching keys and all the values for that particular key
"""

from pyspark import SparkContext
sc = SparkContext("local", "Join app")

# there are two pair of elements in two different RDDs. 
x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])

# After joining these two RDDs, we get an RDD with elements having matching keys and their values.
joined = x.join(y)
final = joined.collect()

print(f"Join RDD -> {final}")