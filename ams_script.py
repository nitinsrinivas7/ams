import json
import pprint
import numpy as np
import requests
import csv
import time

#List of metrics
metrics=['cpu_system','cpu_user']

#Precision [seconds / minutes / hours]
precision="seconds"

#Ambari Server URL and port
ambari_url="AMBARI URL"

#Ambari Metrics Service URL and port
ams_url="AMS URL"

#Cluster Name
cluster_name="CLUSTER NAME"

#Ambari Username
ambari_username="AMBARI USERNAME"

#Ambari Password
ambari_password="AMBARI PASSWORD"

#Benchmark output log file [ CSV foramt ]
benchmark_output_file="/test.log"

q=[]

#Convert Date-Time to Epoch
def date_to_epoch(date_time):
    print(date_time)
    pattern = '%m/%d/%Y %H:%M:%S'
    epoch = int(time.mktime(time.strptime(date_time, pattern)))
    print epoch
    return epoch


#Get all hosts in the cluster
def get_hosts():
    new_data=requests.get(ambari_url+'/api/v1/clusters/'+cluster_name+'/hosts/',auth=(ambari_username, ambari_password),verify=False)

    json_data=new_data.json()

    for i in range(0, len(json_data['items'])):
        q.append((json_data['items'][i]['Hosts']['host_name']))
        print(q[i])

get_hosts()


#Obtain the system and hadoop metrics for the hosts in the cluster
def get_metrics(start_time_epoch,end_time_epoch,benchmark_type):

    list_of_dicts=[]

    for i in range(0, len(q)):
        for j in range(0, len(metrics)):
            metrics_data=requests.get(ams_url+'/ws/v1/timeline/metrics?metricNames='+metrics[j]+'&appid=HOST&hostname='+q[i]+'&precision='+precision+'&startTime='+start_time_epoch+'&endTime='+end_time_epoch, auth=(ambari_username, ambari_password),verify=False)
            list_of_dicts.append(metrics_data.json())

            a = list_of_dicts[0]['metrics'][0]['metrics'].values()

            #Mean calculation
            mean = np.mean(a)
            #Median calculation
            median = np.median(a)
            #90 th Percentile calculation
            percentile = np.percentile(a,90)

            #Write to output File
            #print("Hostname "+q[i]+", Metric"+metrics[j]+",Mean"+mean+",Median"+median+",90th Percentile"+percentile)
            with open("test.csv", "a") as myfile:
                myfile.write("Hostname "+q[i]+",Benchmark Type "+benchmark_type+", Metric "+str(metrics[j])+",Mean "+str(mean)+",Median "+str(median)+",90th Percentile ,"+str(percentile)+"\n")
            del list_of_dicts[:]

#Calling get_metics function for different Start and End times
def calling_function():
    with open(benchmark_output_file) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            #Ignoring blank values for Start-time and End-time
            if not (row[4]) or not (row[5]):
                continue
            #Type of benchmark executed
            benchmark_type=row[0]

            #Start-time and End-time in Date format
            start_time=row[4]
            end_time=row[5]

            #Converting Start-time adn End-time to epoch
            start_time_epoch=date_to_epoch(start_time)

            end_time_epoch=date_to_epoch(end_time)

            #Function call - get_metrics
            get_metrics(start_time_epoch,start_time_epoch,benchmark_type)

calling_function()
