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

cpu = entries.map(lambda x: x[cpuCol]).filter(lambda x: x != "")

for element in cpu.sortBy(lambda x: x).take(10):
	print("element: " + element + ".")

# for element in entries.take(10):
# 	print(element)

# print(entries.groupBy(lambda x: x % 2).collect())

### Exercise 1: What is the distribution of the machines according to their CPU capacity?





