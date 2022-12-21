from html import entities
import sys
import time

from pyspark import SparkContext
from pyspark import pandas as ps
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import matplotlib.pyplot as plt

# ps.options.plotting.backend = "plotly"
# ps.set_option("plotting.backend", "plotly")

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

# What is the distribution of the machines according to their CPU capacity?
def exercise_1():
	sectionsDistribution = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]

	# Gets column with cpu capacity, filters empty values and removes machines ID duplicates
	distinctCpus = entries \
		.filter(lambda x: x[cpuCol] != "") \
		.map(lambda x: [x[i] for i in [machineIDCol, cpuCol]]) \
		.groupByKey() \
		.map(lambda x: float(list(x[1])[0]))

	# Builds histogram: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.histogram.html#pyspark.RDD.histogram
	distinctCpuHist = distinctCpus.histogram(sectionsDistribution)

	# Note how different it is the amount with and without removing distinct CPU's.

	print("\nMachines analysed:", distinctCpus.count()) 
	print("CPU capacity distribution: \n ([DISTRIBUTION], [AMOUNT_OF_VALUES] \n", distinctCpuHist)

	# print("Array: ", distinctCpuHist[1])
	# pandasDf = ps.DataFrame({'CPU capacity': sectionsDistribution, 'Machine Count': distinctCpuHist[1]})
	pandasDf = ps.DataFrame({'CPU Capacity': ['0.2', '0.4', '0.6', '0.8', '1.0'], 'Machine count': distinctCpuHist[1]})
	pandasDf.plot.bar(x='CPU Capacity', y='Machine count', backend='matplotlib')

	plt.show()


### Exercise 2: What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?
# 
def exercise_2():
	pass
	# print(machineById.count())

### Exercise 3: What is the distribution of the number of jobs/tasks per scheduling class?
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

	# New part: getting distribution of tasks.

### Exercise 4: Do tasks with a low scheduling class have a higher probability of being evicted?
# Eviction event type is 2
# TODO: Add taskEvents table analysis.
def exercise_4():
	jobEvents = sc.textFile("./data/allJobEvents.csv")
	# jobEvents = sc.textFile("./data/part-00000-of-00500.csv")
	jobsEntries = jobEvents.map(lambda x: x.split(','))
	jobsColumnsNames = ["Timestamp", "MissingInfo", "JobID", "EventType", "Username", "SchedulingClass", "JobName", "LogicalJobName"]
	jobsDf = jobsEntries.toDF(jobsColumnsNames)


	eventsCount = jobsDf.select("SchedulingClass", "EventType") \
		.groupBy("SchedulingClass") \
		.count() \
		.withColumnRenamed("count", "Events") \
		.sort("SchedulingClass")

	evictionEventCount = jobsDf.select("SchedulingClass", "EventType") \
		.where(jobsDf.EventType == 2) \
		.groupBy("SchedulingClass") \
		.count() \
		.withColumnRenamed("count", "Evictions") \
		.sort("SchedulingClass")

	jointEvents = eventsCount \
		.join(evictionEventCount, ["SchedulingClass"], "left")

	jointEvents = jointEvents \
		.withColumn("Evictions", f.when(jointEvents.Evictions.isNull(), 0).otherwise(jointEvents.Evictions)) \
		.show(truncate=True)

# In general, do tasks from the same job run on the same machine?
# Use fields jobID (2), and task index(3) machineID(4).
# Group by jobID, show machines
def exercise_5():
	taskEvents = sc.textFile("./data/taskEvents/part-00000-of-00500.csv")
	taskEntries = taskEvents.map(lambda x: x.split(','))
	taskColumnsNames = ["Timestamp", "MissingInfo", "JobID", "TaskIndex", "MachineID", "EventType", "Username", "SchedulingClass", "Priority", "CPUCores", "RAM", "Disk", "Constraint"]
	tasksDf = taskEntries.toDF(taskColumnsNames)

	# First, we can take a quick look at the data and see that the first job
	# has 10 different tasks, and each task is executed in a different machine.
	tasksDf \
		.select("JobID", "TaskIndex","MachineID") \
		.groupBy("JobID") \
		.agg(f.countDistinct("TaskIndex"), f.countDistinct("MachineID")) \
		.sort("JobID") \
		.show(truncate=True)

	# Now let's see how this applies to the rest of the data
	# We will get the percentage of jobs that are executed in multiple machines

	# Gets all the jobs count
	jobsCount = tasksDf \
		.groupBy("JobID") \
		.count() \
		.count()

	# Gets all the jobs that run in more than one machine
	jobsRunningInMultipleMachines = tasksDf \
		.groupBy("JobID") \
		.agg(f.countDistinct("TaskIndex").alias("AmountOfTasks"), f.countDistinct("MachineID").alias("AmountOfMachines")) \
		.sort("JobID")

	jobsWithMultipleTasks = jobsRunningInMultipleMachines \
		.filter(jobsRunningInMultipleMachines.AmountOfTasks > 1) \
		.count()

	jobsRunningInMultipleMachinesCount = jobsRunningInMultipleMachines \
		.filter(jobsRunningInMultipleMachines.AmountOfTasks > 1) \
		.filter(jobsRunningInMultipleMachines.AmountOfMachines > 1) \
		.count()

	multipleMachinesRatio = '%.2f'%(100 * jobsRunningInMultipleMachinesCount / jobsCount)
	multiTaskRatio = '%.2f'%(100 * jobsRunningInMultipleMachinesCount / jobsWithMultipleTasks)

	print(str(multipleMachinesRatio) + "% of the jobs run in multiple machines.")
	print("Among jobs with multiple tasks, " + str(multiTaskRatio) + "% of them run in multiple machines.")
	

# Are the tasks that request the more resources the one that consume the more resources?
# Use taskEvents table to get JobID(2) and resource request for CPUCores(9).
# Then, use taskUsage table to get JobID(2) and MeanCPUUsage(5).
# Join using jobID and compare CPU cores and meanCPUUsage.

# Maybe only use "AssignedMemUsage"(7)
def exercise_6():
	taskUsageColumns = ["StartTime", "EndTime", "JobID", "TaskIndex", "MachineID", "MeanCPUUsage", "CanonicalMemUsage", "AssignedMemUsage", "CacheUsage", "TotalCacheUsage", "MaxMemUsage", "MeanDiskTime", "MeanDiskSpace", "MaxCPUUsage", "MaxDiskTime", "CPI", "MAI", "SamplePortion", "AggType", "SampledCPUUsage"]
	taskUsageEvents = sc.textFile("./data/taskUsage/part-00000-of-00500.csv")
	taskEventEntries = taskUsageEvents.map(lambda x: x.split(','))
	usageDf = taskEventEntries.toDF(taskUsageColumns)

	taskEvents = sc.textFile("./data/taskEvents/part-00000-of-00500.csv")
	taskEntries = taskEvents.map(lambda x: x.split(','))
	taskColumnsNames = ["Timestamp", "MissingInfo", "JobID", "TaskIndex", "MachineID", "EventType", "Username", "SchedulingClass", "Priority", "CPUCores", "RAM", "Disk", "Constraint"]
	tasksDf = taskEntries.toDF(taskColumnsNames)

	# Create two lists: one with the (jobID, taskID, CPUCores, RAM),
	# another with (jobID, taskID, MeanCPUUsage, AssignedMemUsage, MaxMemUsage, MaxCPUUsage).
	# Then, plot 4 graphs with requestedParameter vs usedParameter
	tasksDf = tasksDf \
		.select("JobID", "TaskIndex", f.col("CPUCores").cast("float"), f.col("RAM").cast("float")) \
		.filter((tasksDf.CPUCores != "") & (tasksDf.RAM != "")) \
		.groupBy("JobID", "TaskIndex") \
		.avg("CPUCores", "RAM")

	usageDf = usageDf \
		.select("JobID", "TaskIndex", f.col("MeanCPUUsage").cast("float"), f.col("AssignedMemUsage").cast("float"), f.col("MaxMemUsage").cast("float"), f.col("MaxCPUUsage").cast("float")) \
		.groupBy("JobID", "TaskIndex") \
		.avg("MeanCPUUsage", "AssignedMemUsage", "MaxMemUsage", "MaxCPUUsage")

	mergedDf = tasksDf \
		.join(usageDf, ["JobID", "TaskIndex"])

	pandasDf = ps.DataFrame(mergedDf)

	# tasksDf.show(truncate=True)
	# usageDf.show(truncate=True)
	# mergedDf.show(truncate=True)

	pandasDf.plot.scatter(x="avg(CPUCores)", y="avg(MeanCPUUsage)", backend='matplotlib')
	plt.title("CPU Cores requested x Mean CPU usage")
	plt.show()

	pandasDf.plot.scatter(x="avg(CPUCores)", y="avg(MaxCPUUsage)", backend='matplotlib')
	plt.title("CPU Cores requested x Max CPU usage")
	plt.show()

	pandasDf.plot.scatter(x="avg(RAM)", y="avg(AssignedMemUsage)", backend='matplotlib')
	plt.title("RAM requested x Assigned memory usage")
	plt.show()

	pandasDf.plot.scatter(x="avg(RAM)", y="avg(MaxMemUsage)", backend='matplotlib')
	plt.title("RAM Requested x Max memory usage")
	plt.show()

### Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?
# Use taskEvents table to get all evicted events (events with eventType(5) == 2).
# Join the jobID + taskIndex from taskEvents table with taskUsage table.
# From those evicted events, get the average of the Max CPU consumption, Max Mem consumption and Max Disk Consumption.
# From all events that weren't evicted, get the average of the Max CPU consumption.
# Compare both numbers!


# Show that machines that have higher resource consumption on a period are the ones with more evicted tasks.

# If I group by timestamp, machineID. Sum max cpu/memory usage. 
	# We will have the consumption for each machine in each timestamp.
# Join with taskEvents using timestamp and machineID, and get their event type.
	# We will have the the events for each machine in each timestamp.
# Build graph showing how the number of evictions increases related to the power consumption.
# Or, use histogram to get only 10% higher resource consumption periods/events, and get the amount of evictions.
# Then, compare with the other 90%.


# Use taskEvents table to get all events 
# Join the jobID + taskIndex from taskEvents table with taskUsage table.
# 

def exercise_7():
	taskUsageColumns = ["StartTime", "EndTime", "JobID", "TaskIndex", "MachineID", "MeanCPUUsage", "CanonicalMemUsage", "AssignedMemUsage", "CacheUsage", "TotalCacheUsage", "MaxMemUsage", "MeanDiskTime", "MeanDiskSpace", "MaxCPUUsage", "MaxDiskTime", "CPI", "MAI", "SamplePortion", "AggType", "SampledCPUUsage"]
	taskUsageEvents = sc.textFile("./data/taskUsage/part-00000-of-00500.csv")
	# taskUsageEvents = sc.textFile("./data/taskUsage/taskUsageCombined.csv")
	taskEventEntries = taskUsageEvents.map(lambda x: x.split(','))
	usageDf = taskEventEntries.toDF(taskUsageColumns)

	taskEvents = sc.textFile("./data/taskEvents/part-00000-of-00500.csv")
	taskEntries = taskEvents.map(lambda x: x.split(','))
	taskColumnsNames = ["Timestamp", "MissingInfo", "JobID", "TaskIndex", "MachineID", "EventType", "Username", "SchedulingClass", "Priority", "CPUCores", "RAM", "Disk", "Constraint"]
	tasksDf = taskEntries.toDF(taskColumnsNames)
	
	taskByTimestamp = tasksDf \
		.select("TimeStamp", "MachineID","EventType") \
		.where(tasksDf.EventType == 2)

	machinesAndTasks = usageDf \
		.select("StartTime", "EndTime", "MachineID", f.col("MaxMemUsage").cast("float"), f.col("MaxCPUUsage").cast("float")) \
		.join(taskByTimestamp, [(taskByTimestamp.TimeStamp > usageDf.StartTime), (taskByTimestamp.TimeStamp < usageDf.EndTime), (taskByTimestamp.MachineID == usageDf.MachineID)]) \
		.drop(taskByTimestamp.MachineID) \
		.drop(taskByTimestamp.EventType)

	evictedPerTimestamp = machinesAndTasks \
		.groupBy("StartTime", "EndTime", "MachineID") \
		.agg(f.count("TimeStamp").alias("EvictedEvents"), f.avg("MaxCPUUsage"), f.avg("MaxMemUsage")) \
		.sort(f.desc("EvictedEvents"))
	
	evictedPerCPUAndMem = evictedPerTimestamp \
		.select("EvictedEvents", "avg(MaxCPUUsage)", "avg(MaxMemUsage)") \
		.groupBy("EvictedEvents") \
		.agg(f.avg("avg(MaxCPUUsage)"), f.avg("avg(MaxMemUsage)"))

	# evictedPerCPUAndMem.show(truncate=False)

	# taskByTimestamp.show(truncate=True)
	# machinesAndTasks.show(truncate=True)
	# evictedPerTimestamp.show(truncate=True)

	pandasDf = ps.DataFrame(evictedPerCPUAndMem)

	pandasDf.plot(x="EvictedEvents", y="avg(avg(MaxMemUsage))", backend="matplotlib")
	plt.show()


# def exercise_8():


exercise_6()



