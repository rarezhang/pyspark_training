"""
FPGrowth
method 2: provides higher-level API built on top of DataFrames for constructing ML pipelines. 

https://spark.apache.org/docs/latest/mllib-frequent-pattern-mining.html#prefixspan
"""

from pyspark import SparkContext

from pyspark.sql.functions import split
from pyspark.sql.session import SparkSession

from pyspark.ml.fpm import FPGrowth


sc = SparkContext(master="local", appName="FPGrowth") # launch a JVM and creates a JavaSparkContext
spark = SparkSession(sc)


# load data
path_data = "./data/sample_fpgrowth.txt"
data = (spark.read.text(path_data)
        .select(split("value", " ")
        .alias("items")))  # 'pyspark.sql.dataframe.DataFrame'
        
print("show data:")
data.show(truncate=False)
"""
+------------------------+
|items                   |
+------------------------+
|[r, z, h, k, p]         |
|[z, y, x, w, v, u, t, s]|
|[s, x, o, n, r]         |
|[x, z, y, m, t, s, q, e]|
|[z]                     |
|[x, z, y, r, q, t, p]   |
+------------------------+
"""
        
# load model 
# pyspark.ml.fpm.FPGrowth(minSupport=0.3, minConfidence=0.8, itemsCol='items', predictionCol='prediction', numPartitions=None) 
model_fp = FPGrowth(minSupport=0.2, minConfidence=0.7)

# train model 
fpm = model_fp.fit(data)

# show results 
top_n = 5

# freqItemsets
fpm.freqItemsets.show(top_n)
"""
+---------+----+
|    items|freq|
+---------+----+
|      [s]|   3|
|   [s, x]|   3|
|[s, x, z]|   2|
|   [s, z]|   2|
|      [r]|   3|
+---------+----+
"""

# association rules 
fpm.associationRules.show(top_n)
"""
+----------+----------+----------+
|antecedent|consequent|confidence|
+----------+----------+----------+
|    [t, s]|       [y]|       1.0|
|    [t, s]|       [x]|       1.0|
|    [t, s]|       [z]|       1.0|
|       [p]|       [r]|       1.0|
|       [p]|       [z]|       1.0|
+----------+----------+----------+
"""

# prediction based on association rules
new_data = spark.createDataFrame([(["t", "s"], )], ["items"])  # pyspark.sql.dataframe.DataFrame

p = fpm.transform(new_data).first().prediction # ['y', 'x', 'z']
print(sorted(p)) # ['x', 'y', 'z']