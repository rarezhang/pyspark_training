"""
Storage Level  


MEMORY_AND_DISK_2

MEMORY_AND_DISK_2 = StorageLevel(True, True, False, False, 2)

class pyspark.StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication = 1)

useDisk = True 
useMemory = True
replication = 2  # RDD partitions will have replication of 2
"""


from pyspark import SparkContext
import pyspark

sc = SparkContext (
   "local", 
   "storagelevel app"
)

rdd1 = sc.parallelize([1,2])

rdd1.persist( pyspark.StorageLevel.MEMORY_AND_DISK_2 )

rdd1.getStorageLevel()

print(rdd1.getStorageLevel())