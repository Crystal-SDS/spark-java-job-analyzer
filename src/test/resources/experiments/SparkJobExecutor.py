'''
Created on Mar 13, 2017

@author: Raul Gracia
'''

from subprocess import PIPE, STDOUT, Popen
import json
from StringIO import StringIO
import requests
import keystoneclient.v2_0.client as keystone_client
import subprocess
import time
import sys
import os
import re


URL_CRYSTAL_API = 'http://10.30.230.217:8000/'
AUTH_URL='http://10.30.230.217:5000/v2.0'
USERNAME='admin'
PASSWORD='admin'
TENANT='crystaltest'
EXECUTOR_LOCATION = '/home/user/Desktop/'
JAVAC_PATH = '/usr/bin/javac'
SPARK_FOLDER = '/home/user/workspace/spark-2.1.0-bin-hadoop2.7/'
SPARK_LIBS_LOCATION = SPARK_FOLDER + 'jars/'
SPARK_MASTER = '10.30.102.186:7077'
LAMBDA_PUSHDOWN_FILTER = 'lambdapushdown-1.0.jar'
AVAILABLE_RAM = '1G'
AVAILABLE_CPUS = '2'
DEVELOPMENT_MODE = True

#These variables should be set in non-development mode
HDFS_LOCATION = '/something/hadoop-2.7.3/bin/hdfs dfs '
HDFS_IP_PORT = 'IP:9000'
      
      
def main(argv=None):
    
    if argv is None:
        argv = sys.argv 
        
    print argv
    '''STEP 1: Execute the JobAnalyzer'''
    spark_job_path = argv[1]
    spark_job_name = spark_job_path[spark_job_path.rfind('/')+1:spark_job_path.rfind('.')]
    
    '''STEP 5: Compile pushdown/original job'''
    m = re.search('package\s*(\w\.?)*\s*;', jobToCompile)
    jobToCompile = jobToCompile.replace(m.group(0), 
                'package ' + EXECUTOR_LOCATION.replace('/','.')[1:-1] + ';')    
    jobToCompile = jobToCompile.replace(spark_job_name, "SparkJobMigratory")
    
    jobFile = open(EXECUTOR_LOCATION + '/SparkJobMigratory.java', 'w')
    print >> jobFile, jobToCompile
    jobFile.close()
    time.sleep(1) 
    
    print "Starting compilation"
    cmd = JAVAC_PATH + ' -cp \"'+ SPARK_LIBS_LOCATION + '*\" '
    cmd += EXECUTOR_LOCATION + 'SparkJobMigratory.java' 
    proc = subprocess.Popen(cmd, shell=True)
    print ">> EXECUTING: " + cmd
           
    '''STEP 6: Package the Spark Job class as a JAR and set the manifest'''
    print "Starting packaging"
    time.sleep(3)
    cmd = 'jar -cfe ' + EXECUTOR_LOCATION + 'SparkJobMigratory.jar ' + \
                       EXECUTOR_LOCATION.replace('/','.')[1:] + 'SparkJobMigratory ' + \
                       EXECUTOR_LOCATION + 'SparkJobMigratory.class'
    print ">> EXECUTING: " + cmd
    proc = subprocess.Popen(cmd, shell=True)
    time.sleep(3)
       
    '''STEP 7: In cluster mode, we need to store the produced jar in HDFS to make it available to workers'''    
    if (not DEVELOPMENT_MODE):
        print "Starting to store the JAR in HDFS"
        cmd = HDFS_LOCATION + ' -put -f ' + EXECUTOR_LOCATION + 'SparkJobMigratory.jar ' + ' /SparkJobMigratory.jar'
        print ">> EXECUTING: " + cmd
        proc = subprocess.Popen(cmd, shell=True)
        time.sleep(3) 

 
    print "Starting execution"
    '''STEP 7: Execute the job against Swift'''
    cmd = 'bash ' + SPARK_FOLDER+ 'bin/spark-submit --deploy-mode cluster --master spark://' + SPARK_MASTER + ' '+ \
            '--class ' + EXECUTOR_LOCATION.replace('/','.')[1:] + 'SparkJobMigratory ' + \
            '--driver-class-path ' + SPARK_FOLDER + 'jars/stocator-1.0.9.jar '
    if (DEVELOPMENT_MODE):
        cmd += EXECUTOR_LOCATION + 'SparkJobMigratory.jar '        
    else:
        cmd += '--conf spark.extraListeners=ch.cern.sparkmeasure.FlightRecorderStageMetrics,ch.cern.sparkmeasure.FlightRecorderTaskMetrics ' + \
            '--executor-cores ' + AVAILABLE_CPUS + ' --executor-memory ' + AVAILABLE_RAM + \
            ' hdfs://' + HDFS_IP_PORT + '/SparkJobMigratory.jar ' 
            
    cmd += ' --jars ' + SPARK_FOLDER + 'jars/*.jar'
    print ">> EXECUTING: " + cmd
    proc = subprocess.Popen(cmd, shell=True)
    
    '''STEP 8: Clean files'''
    #time.sleep(1)
    #os.remove(EXECUTOR_LOCATION + 'SparkJobMigratory.java')
    #os.remove(EXECUTOR_LOCATION + 'SparkJobMigratory.class')
    #os.remove(EXECUTOR_LOCATION + spark_job_name + 'Java8Translated.java')
    
    
if __name__ == "__main__":
    sys.exit(main())     
