from pyspark import SparkContext

logFile = "notes_pyspark.md"  

sc = SparkContext(master="local", appName="first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print(f"Lines with a: {numAs}, lines with b: {numBs}")