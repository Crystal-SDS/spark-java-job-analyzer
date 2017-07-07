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


valid_token = None



def update_filter_params(lambdasToMigrate):
    token = get_or_update_token()
    headers = {}
    
    url = URL_CRYSTAL_API + "controller/static_policy/"
    
    headers["X-Auth-Token"] = str(token)
    headers['Content-Type'] = "application/json"
    
    r = requests.get(str(url), {}, headers=headers)
    json_data = json.loads(r.content)
    
    print r, json_data
    
    policy_id = None
    
    '''We assume that a single tenant/container only has one pushdown filter'''
    for policy in json_data:
        if policy['filter_name'] == LAMBDA_PUSHDOWN_FILTER:
            policy_id = policy['target_id'] + ':' + policy['id'] 
        
    if policy_id==None:
        print "ERROR: No lambda filter found for " + policy['target_id']
        return None
    

    url = URL_CRYSTAL_API + "controller/static_policy/" + str(policy_id)
    print 'Update filter URL: ' + url

    headers["X-Auth-Token"] = str(token)
    headers['Content-Type'] = "application/json"
    
    lambdas_as_string = ''
    index = 0
    for x in lambdasToMigrate:
        lambdas_as_string+= str(index) + "-lambda=" + str(x) + ","
        index+=1

    r = requests.put(str(url), json.dumps({'params': lambdas_as_string[:-1]}), headers=headers)
    
    return r.status_code
    
    
def get_keystone_admin_auth():
    admin_project = TENANT
    admin_user = USERNAME
    admin_passwd = PASSWORD
    keystone_url = AUTH_URL

    keystone = None
    try:
        keystone = keystone_client.Client(auth_url=keystone_url,
                                          username=admin_user,
                                          password=admin_passwd,
                                          tenant_name=admin_project)
    except Exception as exc:
        print(exc)

    return keystone

def get_or_update_token():
    global valid_token
    
    if valid_token == None:
        keystone = get_keystone_admin_auth()
        valid_token = keystone.auth_token
        print "Auth token to be used: ", valid_token
        
    return valid_token  
      
      
def main(argv=None):
    
    'Pushdown for Q1'
    toMigrate = ["java.util.function.Function<java.lang.String' java.util.List<java.lang.String>>|"
                + "map(s -> { java.util.List<String> l $ new java.util.ArrayList<String>(); String[] a $ s.split(\"'\"); "
                + "l.add(a[0]); l.add(a[1]); l.add(a[5]); l.add(a[7]);",
                "java.util.function.Predicate<java.util.List<java.lang.String>>|" 
                + "filter(s -> (s.get(0).startsWith(\"2015-01\") && s.get(3).equals(\"Paris\")) || s.get(0).startsWith(\"date\"))",
                "java.util.function.Function<java.util.List<java.lang.String>' java.lang.String>|"
                    + "map(l -> l.toString().replace(\"[\", \"\").replace(\"]\", \"\"))"]
    str(update_filter_params(toMigrate))
    
    
if __name__ == "__main__":
    sys.exit(main())     
