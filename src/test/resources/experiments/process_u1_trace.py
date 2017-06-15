'''
Created on Sep 29, 2014

@author: user
'''
import time
import gzip
import gc
from _ctypes import sizeof
from decimal import DivisionByZero


'''Parse a raw trace entry'''
def parse_line(l):
    l = l.replace("\"\"","").replace("b\'","").replace("\'","").replace("\n","").replace("\r","")          
    while "\"" in l:
        first_ocurrence = l.index("\"")
        second_ocurrence = l[first_ocurrence+1:].index("\"")+2
        l = l[0:first_ocurrence] + l[second_ocurrence+first_ocurrence:]        
    return l.split(",")
    
DATASET_PATH = "/home/user/dump16.csv.tar.gz"

first_line = True 
parsing_time = time.time()
processed = 0
lines = 0


CHUNK_SIZE_IN_MB = 1
DATASET_SIZE_IN_MB = 10
trace_chunk = open('./1_u1.csv', 'w')
chunk_counter = 1

with gzip.open(DATASET_PATH, 'rt') as f:
    '''Process raw trace lines'''
    old_size_module = 0
    for l in f:
        '''Output some information about the processing performance'''
        lines+=1
        processed+=len(l)
        
        size_module = int(processed/(1024*1024))        
        if size_module>0 and size_module%CHUNK_SIZE_IN_MB==0 and size_module>old_size_module:
            gc.collect() 
            trace_chunk.close()
            if DATASET_SIZE_IN_MB < (chunk_counter*CHUNK_SIZE_IN_MB):
                break            
            
            chunk_counter+=1
            trace_chunk = open('./' + str(chunk_counter) + '_u1.csv', 'w') 
            old_size_module = size_module
            print "Data processed: ", size_module, "MB trace lines: ", lines
            
        if not first_line:            
            print >> trace_chunk, l[:-1]
                
        else: first_line = False
        