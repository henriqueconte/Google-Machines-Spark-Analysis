from html import entities
import sys
import time

from pyspark import SparkContext
from pyspark import pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
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

# What is the distribution of the machines according to their CPU capacity?
def exercise_1():

	### Columns indexes:
	machineIDCol = 1  
	cpuCol = 4 

	# read the input file into an RDD[String]
	machineEvents = sc.textFile("./data/machineEvents/part-00000-of-00001.csv")
	entries = machineEvents.map(lambda x: x.split(','))
	entries.cache()

	# We will distribute the machines in 5 different sections based on their CPU capacity.
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
# I solved this question with help from Pedro Henrique Pons Fiorentin and Guilherme Klein Kern.
def exercise_2():
	machineEvents = sc.textFile("./data/machineEvents/part-00000-of-00001.csv")
	eventsEntries = machineEvents.map(lambda x: x.split(','))
	eventsEntries.cache()

	columnsNames = ["Timestamp", "MachineID", "EventType", "PlatformID", "CPU", "Memory"]
	eventsDf = eventsEntries.toDF(columnsNames)

	eventsDf = eventsDf.select(f.col("Timestamp").cast("float"), "MachineID", "EventType", f.col("CPU").cast("float"))

	# Creates a partition rule
	timeStampWindow = Window.partitionBy("machineID").orderBy("Timestamp")

	# Gets only connect/disconnect events that happened after the initial timestamp, and creates a column 
	# with the previous timestamp of the column
	laggedDf = eventsDf \
		.where(eventsDf.EventType != 2) \
		.where(eventsDf.Timestamp != 0) \
		.withColumn("timestampLag", f.lag("Timestamp", 1).over(timeStampWindow))

	# Gets only the valid timestamps
	laggedDf = laggedDf \
		.where(laggedDf.timestampLag.isNull() == False) \
		.where(laggedDf.EventType == 0) \
		.withColumn("lagDifference", f.col("Timestamp") - f.col("timestampLag"))
		
	# Creates a column to get the computational power lost
	laggedDf = laggedDf \
		.withColumn("compLost", f.col("lagDifference") * f.col("CPU"))

	# Sums the computational power lost
	lostCpu = laggedDf \
		.agg(f.sum("compLost")).collect()[0][0]

	# Gets the final timestamp
	fullTime = eventsDf.agg(f.max("Timestamp")).collect()[0][0]

	# Gets the total computational power
	totalCpu = eventsDf \
		.groupBy("CPU", "MachineID") \
		.count() \
		.agg(f.sum("CPU")).collect()[0][0]

	workingCpu = (fullTime * totalCpu) - lostCpu

	print("Computational power lost due to maintenance(a machine went offline and reconnected later):")
	print('%.2f'%(100 * lostCpu / workingCpu), "%")

### Exercise 3: What is the distribution of the number of jobs/tasks per scheduling class?
# Get Job event table, group by scheduling class (field 5), count number of elements per class, maybe histogram?
def exercise_3():

	# First, let's see the jobs distribution.
	jobEvents = sc.textFile("./data/jobEvents/mergedJobEvents.csv")
	jobsEntries = jobEvents.map(lambda x: x.split(','))
	jobsColumnsNames = ["Timestamp", "MissingInfo", "JobID", "EventType", "Username", "SchedulingClass", "JobName", "LogicalJobName"]
	jobsDf = jobsEntries.toDF(jobsColumnsNames)

	print("Jobs distribution by scheduling class")
	jobsDf.select("SchedulingClass") \
		.groupBy("SchedulingClass") \
		.count() \
		.sort("SchedulingClass") \
		.show(truncate=True)

	# Now, let's see the tasks distribution.
	taskEvents = sc.textFile("./data/taskEvents/mergedTaskEvents.csv")
	taskEntries = taskEvents.map(lambda x: x.split(','))
	taskColumnsNames = ["Timestamp", "MissingInfo", "JobID", "TaskIndex", "MachineID", "EventType", "Username", "SchedulingClass", "Priority", "CPUCores", "RAM", "Disk", "Constraint"]
	tasksDf = taskEntries.toDF(taskColumnsNames)

	print("Tasks distribution by scheduling class")
	tasksDf.select("SchedulingClass") \
		.groupBy("SchedulingClass") \
		.count() \
		.sort("SchedulingClass") \
		.show(truncate=True)

### Exercise 4: Do tasks with a low scheduling class have a higher probability of being evicted?
def exercise_4():
	jobEvents = sc.textFile("./data/jobEvents/mergedJobEvents.csv")
	# jobEvents = sc.textFile("./data/part-00000-of-00500.csv")
	jobsEntries = jobEvents.map(lambda x: x.split(','))
	jobsColumnsNames = ["Timestamp", "MissingInfo", "JobID", "EventType", "Username", "SchedulingClass", "JobName", "LogicalJobName"]
	jobsDf = jobsEntries.toDF(jobsColumnsNames)

	# Gets the events per scheduling class
	eventsCount = jobsDf.select("SchedulingClass", "EventType") \
		.groupBy("SchedulingClass") \
		.count() \
		.withColumnRenamed("count", "Events") \
		.sort("SchedulingClass")

	# Gets the evicted events per scheduling class
	evictionEventCount = jobsDf.select("SchedulingClass", "EventType") \
		.where(jobsDf.EventType == 2) \
		.groupBy("SchedulingClass") \
		.count() \
		.withColumnRenamed("count", "Evictions") \
		.sort("SchedulingClass")

	# Joins both tables
	jointEvents = eventsCount \
		.join(evictionEventCount, ["SchedulingClass"], "left")

	# Creates column evictions 
	jointEvents = jointEvents \
		.withColumn("Evictions", f.when(jointEvents.Evictions.isNull(), 0).otherwise(jointEvents.Evictions))

	# Creates column with the probability of each scheduling class to be evicted
	jointEvents = jointEvents \
		.withColumn("Probability", jointEvents.Evictions / jointEvents.Events)

	jointEvents \
	.sort("SchedulingClass") \
	.show(truncate=True)

	# Now, let's see the tasks distribution.
	taskEvents = sc.textFile("./data/taskEvents/mergedTaskEvents.csv")
	taskEntries = taskEvents.map(lambda x: x.split(','))
	taskColumnsNames = ["Timestamp", "MissingInfo", "JobID", "TaskIndex", "MachineID", "EventType", "Username", "SchedulingClass", "Priority", "CPUCores", "RAM", "Disk", "Constraint"]
	tasksDf = taskEntries.toDF(taskColumnsNames)

	tasksCount = tasksDf.select("SchedulingClass", "EventType") \
		.groupBy("SchedulingClass") \
		.count() \
		.withColumnRenamed("count", "Events") \
		.sort("SchedulingClass")

	tasksEvictionCount = tasksDf.select("SchedulingClass", "EventType") \
		.where(tasksDf.EventType == 2) \
		.groupBy("SchedulingClass") \
		.count() \
		.withColumnRenamed("count", "Evictions") \
		.sort("SchedulingClass")

	jointTasksEvents = tasksCount \
		.join(tasksEvictionCount, ["SchedulingClass"], "left")

	jointTasksEvents = jointTasksEvents \
		.withColumn("Evictions", f.when(jointTasksEvents.Evictions.isNull(), 0).otherwise(jointTasksEvents.Evictions))
		# .show(truncate=True)

	jointTasksEvents = jointTasksEvents \
		.withColumn("Probability", jointTasksEvents.Evictions / jointTasksEvents.Events)

	jointTasksEvents \
		.sort("SchedulingClass") \
		.show(truncate=True)

# In general, do tasks from the same job run on the same machine?
# Use fields jobID (2), and task index(3) machineID(4).
# Group by jobID, show machines
def exercise_5():
	taskEvents = sc.textFile("./data/taskEvents/mergedTaskEvents.csv")
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

	# Gets the average CPU and memory consumption per task of each job
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
		.select("TimeStamp", "MachineID","EventType")

	# Joining evicted tasks that happened in a timeframe
	machinesAndTasks = usageDf \
		.select("StartTime", "EndTime", "MachineID", f.col("MaxMemUsage").cast("float"), f.col("MaxCPUUsage").cast("float")) \
		.join(taskByTimestamp, [(taskByTimestamp.TimeStamp > usageDf.StartTime), (taskByTimestamp.TimeStamp < usageDf.EndTime), (taskByTimestamp.MachineID == usageDf.MachineID)]) \
		.drop(taskByTimestamp.MachineID)

	# Gets the amount of events and their cpu and memory usage
	eventsPerTimestamp = machinesAndTasks \
		.groupBy("StartTime", "EndTime", "MachineID") \
		.agg(f.count("TimeStamp").alias("EvictedEvents"), f.avg("MaxCPUUsage"), f.avg("MaxMemUsage")) \
		.sort(f.desc("EvictedEvents"))
	
	evictedPerCPUAndMem = eventsPerTimestamp \
		.select("EvictedEvents", "avg(MaxCPUUsage)", "avg(MaxMemUsage)") \
		.groupBy("EvictedEvents") \
		.agg(f.avg("avg(MaxCPUUsage)"), f.avg("avg(MaxMemUsage)"))

	pandasDf = ps.DataFrame(evictedPerCPUAndMem)

	pandasDf.plot(x="EvictedEvents", y="avg(avg(MaxMemUsage))", backend="matplotlib")
	plt.show()

	pandasDf.plot(x="EvictedEvents", y="avg(avg(MaxCPUUsage))", backend="matplotlib")
	plt.show()

# Do tasks that have a higher CPU usage also have a higher memory usage? 
def exercise_8():
	taskUsageColumns = ["StartTime", "EndTime", "JobID", "TaskIndex", "MachineID", "MeanCPUUsage", "CanonicalMemUsage", "AssignedMemUsage", "CacheUsage", "TotalCacheUsage", "MaxMemUsage", "MeanDiskTime", "MeanDiskSpace", "MaxCPUUsage", "MaxDiskTime", "CPI", "MAI", "SamplePortion", "AggType", "SampledCPUUsage"]
	# taskUsageEvents = sc.textFile("./data/taskUsage/part-00000-of-00500.csv")
	taskUsageEvents = sc.textFile("./data/taskUsage/taskUsageCombined.csv")
	taskEventEntries = taskUsageEvents.map(lambda x: x.split(','))
	usageDf = taskEventEntries.toDF(taskUsageColumns)

	usageDf = usageDf \
		.select("JobID", "TaskIndex", f.col("MeanCPUUsage").cast("float"), f.col("AssignedMemUsage").cast("float"), f.col("MaxCPUUsage").cast("float"), f.col("MaxMemUsage").cast("float"))

	# Gets the CPU and memory usage per task of the jobs
	cpuByMemoryMean = usageDf \
		.select("JobID", "TaskIndex", "MeanCPUUsage", "AssignedMemUsage", "MaxCPUUsage", "MaxMemUsage") \
		.groupBy("JobID", "TaskIndex") \
		.avg("MeanCPUUsage", "AssignedMemUsage", "MaxCPUUsage", "MaxMemUsage")

	# Creates correlation of cpu and memory usage
	cpuMemMeanCorr = usageDf \
		.stat.corr("MeanCPUUsage", "AssignedMemUsage")

	cpuMemMaxCorr = usageDf \
		.stat.corr("MaxCPUUsage", "MaxMemUsage")

	print("Correlation between mean CPU and Mean memory usage:", cpuMemMeanCorr)

	print("Correlation between max CPU and max memory usage:", cpuMemMaxCorr)

	cpuByMemoryMeanPandas = ps.DataFrame(cpuByMemoryMean)

	cpuByMemoryMeanPandas.plot.scatter(x="avg(MeanCPUUsage)", y="avg(AssignedMemUsage)", backend="matplotlib")
	plt.title("MeanCPUUsage x AssignedMemUsage")
	plt.show()

	cpuByMemoryMeanPandas.plot.scatter(x="avg(MaxCPUUsage)", y="avg(MaxMemUsage)", backend="matplotlib")
	plt.title("MaxCPUUsage x MaxMemUsage")
	plt.show()


# How many submitted tasks actually finish without being evicted, failing, being killed or lost? 
def exercise_9():
	# taskEvents = sc.textFile("./data/taskEvents/part-00000-of-00500.csv")
	taskEvents = sc.textFile("./data/taskEvents/mergedTaskEvents.csv")
	taskEntries = taskEvents.map(lambda x: x.split(','))
	taskColumnsNames = ["Timestamp", "MissingInfo", "JobID", "TaskIndex", "MachineID", "EventType", "Username", "SchedulingClass", "Priority", "CPUCores", "RAM", "Disk", "Constraint"]
	tasksDf = taskEntries.toDF(taskColumnsNames)

	# Gets the amount of groups of tasks
	allTasksCount = tasksDf \
		.select("JobID", "TaskIndex", "EventType") \
		.groupBy("JobID", "TaskIndex") \
		.count() \
		.count()

	# Gets all the tasks that were interrupted or failed to complete.
	unsucessfulTasksCount = tasksDf \
		.select("JobID", "TaskIndex", "EventType") \
		.filter((tasksDf.EventType == 2) | (tasksDf.EventType == 3) | (tasksDf.EventType == 5) | (tasksDf.EventType == 6)) \
		.groupBy("JobID", "TaskIndex") \
		.count() \
		.count()

	print("Amount of tasks:", allTasksCount)
	print("Amount of tasks that were either evicted, killed, lost or failed:", unsucessfulTasksCount)
	print()
	print(str('%.2f'%(100*unsucessfulTasksCount/allTasksCount)) + "% of the tasks were evicted, killed, lost or failed at some point.")


# Extending the work: seeing the average number of CPU cores and memory per machine from Alibaba dataset
def exercise_10():
	machineEvents = sc.textFile("./data/alibaba.csv")
	machineEntries = machineEvents.map(lambda x: x.split(','))
	machineColumnsNames = ["MachineID", "Timestamp", "FailureDomain1", "FailureDomain2", "CPUCount", "MemorySize", "Status"]
	machinesDf = machineEntries.toDF(machineColumnsNames)

	cpuCountDf = machinesDf.select(f.col("CPUCount").cast("int"))
	cpuPandasDf = ps.DataFrame(cpuCountDf)

	plt.hist(cpuPandasDf, density=True, bins=100)
	plt.title("Number of cpus per machine")
	plt.show()

	memoryDf = machinesDf.select(f.col("MemorySize").cast("float"))
	memoryPandasDf = ps.DataFrame(memoryDf)

	plt.hist(memoryPandasDf, density=True, bins=100)
	plt.title("Memory size distribution")
	plt.show()	
	
# Change method according to the exercise you want to execute
exercise_2()



