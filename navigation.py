from html import entities
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

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
sparkSession = SparkSession.builder \
                    .master('local[1]') \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

sc.setLogLevel("ERROR")

### Columns name:
columnsNames = ["Timestamp", "MachineID", "EventType", "PlatformID", "CPU", "Memory"]

### Columns indexes:
timeCol = 0  
machineIDCol = 1  
eventTypeCol = 2
platformIDCol = 3  
cpuCol = 4  
memoryCol = 5  

# read the input file into an RDD[String]
machineEvents = sc.textFile("./data/part-00000-of-00001.csv")

entries = machineEvents.map(lambda x: x.split(','))

df = entries.toDF(columnsNames)

entries.cache()

def exercise_1():
	## Gets column with cpu capacity, filters empty values and removes machines ID duplicates
	distinctCpus = entries \
		.filter(lambda x: x[cpuCol] != "") \
		.map(lambda x: [x[i] for i in [machineIDCol, cpuCol]]) \
		.groupByKey() \
		.map(lambda x: float(list(x[1])[0]))

	cpu = entries \
		.map(lambda x: x[cpuCol]) \
		.filter(lambda x: x != "") \
		.map(lambda x: float(x))

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

def exercise_2():

	# Groups events by machine ID
	# machinesById = entries \
	# 	.map(lambda x: [x[i] for i in [timeCol, machineIDCol, eventTypeCol, cpuCol]]) \
	# 	.groupBy(lambda x: x[1])

	# Gets the full timestamp (gets timestamp from last element) - PROBLEM: It is taking another element instead.
	# lastTimestamp = entries.top(1)

	# For each machine ID, calculate the amount of time it spent offline ()

	# 0 (add)
	# 3 (rem)
	# 5 (add)
	# 11 (rem)
	# 19 (add)
	# 25 - end

	# time online: 3 + 6 + 6 = 15
	# time offline: 2 + 8 = 10

	# 0 + 5 + 19 = 24
	# 3 + 11 = 14


	# 0 (add)
	# 18 (rem)
	# 20 - end

	# 0 = 0 (18)
	# 18 = 18 (2)
	# end = 0

	# print("Result: ", machinesById.take(10))

	# print("Last timestamp: ", lastTimestamp)

	# print(df.select('Timestamp').collect())

	positiveTimestamp = df \
		.select(
			f.round((df.Timestamp / 1000000)).alias('Timestamp'),
			"MachineID",
			"EventType"
		) \
		.where(df.EventType == 0) \
		.groupBy("MachineID") \
		.sum("Timestamp") \
		.show(truncate=True)

	# print(machineById.count())

### Exercise 3
# Get Job event table, group by scheduling class (field 5), count number of elements per class, maybe histogram?

def exercise_3():
	jobEvents = sc.textFile("./data/part-00000-of-00500.csv")
	jobsEntries = jobEvents.map(lambda x: x.split(','))
	jobsColumnsNames = ["Timestamp", "MissingInfo", "JobID", "EventType", "Username", "SchedulingClass", "JobName", "LogicalJobName"]

	jobsDf = jobsEntries.toDF(jobsColumnsNames)

	jobsDf.select("SchedulingClass") \
		.groupBy("SchedulingClass") \
		.count() \
		.sort("SchedulingClass") \
		.show(truncate=False)

# group by machineID
# 

exercise_3()



