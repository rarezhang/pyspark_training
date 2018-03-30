"""
how to use an Accumulator variable. 

An Accumulator variable has an attribute called value  
Stores the data and is used to return the accumulator's value, but usable only in a driver program.
"""

from pyspark import SparkContext 
sc = SparkContext("local", "Accumulator app") 

num = sc.accumulator(10) 


def f(x): 
   global num 
   num+=x 
   
rdd = sc.parallelize([20,30,40,50]) 
rdd.foreach(f)  # # Returns only those elements which meet the condition of the function inside foreach  

final = num.value  # return 150

print(f"Accumulated value is -> {final}")
