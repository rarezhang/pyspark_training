"""
Returns only those elements which meet the condition of the function inside foreach. 


"""

from pyspark import SparkContext

sc = SparkContext("local", "ForEach app")

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

def f(x): 
    """
    call a print function in foreach, which prints all the elements in the RDD
    """
    print(x)
    
    
fore = words.foreach(f)    
