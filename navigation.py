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

timeCol = 0  
machineIDCol = 1  
eventTypeCol = 2
platformIDCol = 3  
cpuCol = 4  
memoryCol = 5  

# read the input file into an RDD[String]
machineEvents = sc.textFile("./data/part-00000-of-00001.csv")

entries = machineEvents.map(lambda x: x.split(','))
# df = entries.toDF(["A","B", "C","D", "E", "F"])

entries.cache()

## Gets column with cpu capacity, filters empty values and removes machines ID duplicates
distinctCpus = entries.filter(lambda x: x[cpuCol] != "").map(lambda x: [x[i] for i in [machineIDCol, cpuCol]]).groupByKey().map(lambda x: float(list(x[1])[0]))

cpu = entries.map(lambda x: x[cpuCol]).filter(lambda x: x != "").map(lambda x: float(x))

### Testing elements
print(distinctCpus.take(10))


### Exercise 1: What is the distribution of the machines according to their CPU capacity?

# Builds histogram: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.histogram.html#pyspark.RDD.histogram
cpuHist = cpu.histogram([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
distinctCpuHist = distinctCpus.histogram([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])

# Note how different it is the amount with and without removing distinct CPU's.
print("Cpu size: ", cpu.count())
print("Distinct cpu size: ", distinctCpus.count())

print(cpuHist)
print(distinctCpuHist)


### Exercise 2: What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?
# Group by machineID,
# For every machine, calculate the time spent offline (map!). To do this, get event 0 (add)

# Filtering machinesID that have events 0, 1 and 0 again.


# group by machineID
# 



