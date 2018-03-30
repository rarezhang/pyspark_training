"""
After performing the specified commutative and associative binary operation, the element in the RDD is returned
"""

from pyspark import SparkContext
from operator import add

sc = SparkContext("local", "Reduce app")
nums = sc.parallelize([1, 2, 3, 4, 5])

# importing add package from the operator and applying it on 'num' to carry out a simple addition operation.
adding = nums.reduce(add)
print(f"Adding all the elements: {adding}")