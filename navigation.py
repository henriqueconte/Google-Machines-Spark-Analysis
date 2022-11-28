from html import entities
import sys
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import time

# Finds out the index of "name" in the array firstLine 
# returns -1 if it cannot find it
def findCol(firstLine, name):
	if name in firstLine:
		return firstLine.index(name)
	else:
		return -1


#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

### Columns name:

timeCol = 1  
machineIDCol = 2  
eventTypeCol = 3
platformIDCol = 4  
cpuCol = 5  
memoryCol = 6  

# read the input file into an RDD[String]
machineEvents = sc.textFile("./data/part-00000-of-00001.csv")

entries = machineEvents.map(lambda x: x.split(','))

## Gets column with cpu capacity
cpu = entries.map(lambda x: x[cpuCol]).filter(lambda x: x != "").map(lambda x: float(x))


### Testing elements
# for element in cpu.sortBy(lambda x: x).take(10):
# 	print("element: " + str(element) + ".")


### Exercise 1: What is the distribution of the machines according to their CPU capacity?

# Builds histogram: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.histogram.html#pyspark.RDD.histogram
cpuHist = cpu.histogram([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])

print(cpuHist)





