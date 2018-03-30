"""
serialize the data using MarshalSerializer
"""

from pyspark.context import SparkContext
from pyspark.serializers import MarshalSerializer

sc = SparkContext("local", "serialization app", serializer = MarshalSerializer())

print(sc.parallelize(list(range(1000))).map(lambda x: 2 * x).take(10))
# take(10) -> first 10 items 

sc.stop()
