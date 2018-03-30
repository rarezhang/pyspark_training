# PySpark  
[Tutorial](https://www.tutorialspoint.com/pyspark/index.htm)  
[Introduction](https://www.tutorialspoint.com/pyspark/pyspark_quick_guide.htm)  
[pyspark examples](https://github.com/apache/spark/tree/master/examples/src/main/python)  


## Introduction
- fast real-time processing framework  
    + use HDFS (Hadoop Distributed File system)  
    + can run on YARN  
- in-memory computations  
- perform:
    + stream processing in real-time  
    + batch processing  
    + interactive queries & interactive algorithms  
- written in Scala & support Python with PySpark     


## Configurations and parameters 
- provides configurations to run a Spark application  
```python
class pyspark.SparkConf (
   loadDefaults = True, 
   _jvm = None, 
   _jconf = None
)
```
- setter methods: ```e.g., conf.setAppName("PySpark App").setMaster("local")```  
    + set(key, value) # To set a configuration property  
    + setMaster(value) # To set the master URL  
    + setAppName(value) # To set an application name  
    + get(key, defaultValue=None) # To get a configuration value of a key  
    + setSparkHome(value) # To set Spark installation path on worker nodes  
    
```python
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("PySpark App").setMaster("spark://master:7077")
sc = SparkContext(conf=conf)
```

## SparkContext  
- the main function and SparkContext gets initiated  
- when we run any Spark application - a driver program starts  
- **SparkContext** uses Py4J to launch a JVM and creates a JavaSparkContext  
- **PySpark** has SparkContext available as **'sc'**
```python
class pyspark.SparkContext (
   master = None,  # the url of the cluster it connects to  
   appName = None,  # name of the job  
   sparkHome = None,  # spark installation directory  
   pyFiles = None,  # the .zip or .py files to send to the cluster and add to the PYTHONPATH  
   environment = None,  # worker nodes environment variables  
   batchSize = 0,  # The number of Python objects represented as a single Java object. Set 1 to disable batching, 0 to automatically choose the batch size based on object sizes, or -1 to use an unlimited batch size  
   serializer = PickleSerializer(),  # RDD serializer  
   conf = None,  # an object of L{SparkConf} to set all the Spark properties
   gateway = None,  # use an existing gateway and JVM, otherwise initializing a new JVM  
   jsc = None,  # the JavaSparkContext instance
   profiler_cls = <class 'pyspark.profiler.BasicProfiler'>  # a class of custom Profiler used to do profiling (the default is pyspark.profiler.BasicProfiler)  
)
```
[spark_context.py](simple_examples/spark_context.py)


## RDD  
- Resilient Distributed Dataset  
- RDD: Elements that run and operate on multiple **nodes** to do parallel processing on a cluster  
- to apply any operation in PySpark, need to create a PySpark RDD first  

2 ways: apply operations on RDD:  
### Transformation  
create a new RDD  
e.g., filter, groupBy and map  

### Action  
operations applied on RDD  
instructs Spark to perform computation and send the result back to the driver  

[count.py](simple_examples/count.py)  # number of elements in the RDD is returned  
[collect.py](simple_examples/collect.py)  # all the elements in the RDD are returned  
[foreach](simple_examples/foreach.py)  # Returns only those elements which meet the condition of the function inside foreach  
[filter.py](simple_examples/filter.py)  # a new RDD is returned containing the elements, which satisfies the function inside the filter  
[map.py](simple_examples/map.py)  # a new RDD is returned: applying a function to each element in the RDD  
[reduce.py](simple_examples/reduce.py)  # After performing the specified commutative and associative binary operation, the element in the RDD is returned  
[join.py](simple_examples/join.py)  # returns RDD with a pair of elements with the matching keys and all the values for that particular key  
[cache.py](simple_examples/cache.py)  # Persist this RDD with the default storage level (MEMORY_ONLY). You can also check if the RDD is cached or not.  


## parallel processing  
- Spark uses shared variables for **parallel processing**  

2 types of shared variables:  
### broadcast  
- save the copy of data across all nodes  
- cached on all the machines and not sent on machines with tasks   
```python
class pyspark.Broadcast (
   sc = None, 
   value = None, 
   pickle_registry = None, 
   path = None
)
```
[broadcast.py](simple_examples/broadcast.py)  # how to use a Broadcast variable  

### accumulator  
- aggregating the information through associative and commutative operations  
- use an accumulator for a sum operation or counters (in MapReduce)  
```python 
class pyspark.Accumulator(aid, value, accum_param)
```
[accumulator.py](simple_examples/accumulator.py)


## files  
- ```sc.addFile(path_to_file)``` : upload files (sc = SparkContext)  
- ```SparkFiles.get(file_name)``` : get the path on a worker  
- ```SparkContext.addFile(path_to_file)``` : resolve the paths to files  
    + ```get(filename)```  # specifies the path of the file that is added through SparkContext.addFile()  
    + ```getRootDirectory()```  : specifies the path to the root directory, which contains the file that is added through the SparkContext.addFile()  
[sparkfile.py](simple_examples/sparkfile.py)

## storage
- how RDD should be stored:  
    + memory  
    + disk  
    + both  
    + whether to **serialize** RDD and whether to **replicate** RDD partitions  
    
```python 
class pyspark.StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication = 1)
```  
    
- storage level:  
    DISK_ONLY = StorageLevel(True, False, False, False, 1)  
    DISK_ONLY_2 = StorageLevel(True, False, False, False, 2)  
    MEMORY_AND_DISK = StorageLevel(True, True, False, False, 1)  
    MEMORY_AND_DISK_2 = StorageLevel(True, True, False, False, 2)  
    MEMORY_AND_DISK_SER = StorageLevel(True, True, False, False, 1)  
    MEMORY_AND_DISK_SER_2 = StorageLevel(True, True, False, False, 2)  
    MEMORY_ONLY = StorageLevel(False, True, False, False, 1)  
    MEMORY_ONLY_2 = StorageLevel(False, True, False, False, 2)  
    MEMORY_ONLY_SER = StorageLevel(False, True, False, False, 1)  
    MEMORY_ONLY_SER_2 = StorageLevel(False, True, False, False, 2)  
    OFF_HEAP = StorageLevel(True, True, True, False, 1)  
    
[storagelevel.py](simple_examples/storagelevel.py)  


## Machine Learning API  
- MLlib  
    **mllib.classification** - supports various methods for binary classification, multiclass classification and regression analysis. e.g., Random Forest, Naive Bayes, Decision Tree, etc.  
    **mllib.clustering** - unsupervised learning problem: group subsets of entities with one another based on some notion of similarity.  
    **mllib.fpm** - Frequent pattern matching: mining frequent items, itemsets, subsequences or other substructures that are usually among the first steps to analyze a large-scale dataset.  
    **mllib.linalg** - MLlib utilities for linear algebra.  
    **mllib.recommendation** - Collaborative filtering: fill in the missing entries of a user item association matrix.  
    **spark.mllib** - It ¬currently supports model-based collaborative filtering, in which users and products are described by a small set of latent factors that can be used to predict missing entries. spark.mllib uses the Alternating Least Squares (ALS) algorithm to learn these latent factors.  
    **mllib.regression** - Regression algorithms: find relationships and dependencies between variables. The interface for working with linear regression models and model summaries is similar to the logistic regression case.  

[recommend.py](simple_examples/recommend.py)
    
## Serializers  
- used for performance tuning (plays an important role in costly operations)  
- all data that is sent over the network or written to the disk or persisted in the memory should be serialized  

2 serializer:  
### MarshalSerializer  
- Python’s Marshal Serializer  
- faster than PickleSerializer  
- supports fewer data types  
```python 
class pyspark.MarshalSerializer  
```

### PickleSerializer  
- Python’s Pickle Serializer  
- supports nearly any Python object  
- __not__ be as fast as more specialized serializers  
```python
class pyspark.PickleSerializer
```

[serializing.py](simple_examples/serializing.py)  # serialize the data using MarshalSerializer