"""
spark files 
"""

from pyspark import SparkContext
from pyspark import SparkFiles


sc = SparkContext("local", "SparkFile App")

finddistance = "notes_pyspark.md"
finddistancename = "notes_pyspark.md"


sc.addFile(finddistance)  
print(f"Absolute Path -> {SparkFiles.get(finddistancename)}")  # specifies the path of the file that is added through SparkContext.addFile()

print(f"Root Directory -> {SparkFiles.getRootDirectory()}")  # specifies the path to the root directory, which contains the file that is added through the SparkContext.addFile()