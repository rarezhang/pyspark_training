"""
how to use a Broadcast variable
Broadcast variable: save the copy of data across all nodes

A Broadcast variable has an attribute called value, which stores the data and is used to return a broadcasted value.
"""

from pyspark import SparkContext 
sc = SparkContext("local", "Broadcast app") 


words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"]) 
data = words_new.value 
print(f"Stored data -> {data}") 

elem = words_new.value[2]  # return "hadoop"
print(f"Printing a particular element in RDD -> {elem}")  