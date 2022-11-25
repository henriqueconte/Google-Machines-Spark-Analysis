# Google-Machines-Spark-Analysis

# RDD Pyspark documentation: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html

## Plan: 
• What is the distribution of the machines according to their CPU capacity?  
Use gs://clusterdata-2011-2/machine_events/ to get information about machines CPU. This table has the following information:  

Fields:  
1. time  
2. machine ID  
3. event type  
4. platform ID  
5. CPUs  
6. Memory  

We will use the field 5 (CPUs) to get information about the CPUs. I will divide the machines in 10 groups (0-0.1, 0.1-0.2 etc) and get the amount of machines that have that amount of CPU. Then, build graph with it.  

• What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?  
Use gs://clusterdata-2011-2/machine_events/ to get the events that made machine go offline. Get each different machine and calculate the total amount of time they spent offline. Multiply the (offlineAmount/totalAmount) for CPU to get the final CPU power. Sum all CPU's again both with and without CPU power lost. Get (afterOffline/totalCPUPower). Show graph. 

• What is the distribution of the number of jobs/tasks per scheduling class?  
• Do tasks with a low scheduling class have a higher probability of being evicted?  
• In general, do tasks from the same job run on the same machine?  
• Are the tasks that request the more resources the one that consume the more resources?  
• Can we observe correlations between peaks of high resource consumption on some ma- chines and task eviction events?  