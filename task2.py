#!/usr/bin/env python
# coding: utf-8

# In[6]:


from subprocess import run, PIPE, Popen
from pathlib import Path
import subprocess
import os
from os.path import isfile, join
import argparse
import fnmatch
import json 
import pandas as pd, requests
import numpy as np
from pandas.io.json import json_normalize
import shutil
import time

start_time = time.time()

checksums = {}
duplicates = []

parser = argparse.ArgumentParser()

parser.add_argument("dir", help = "Enter Directory to be listed")

parser.add_argument("-u", "--unix", action="store_true", dest="unix", default=False, help="This option maintain unix time")

args = parser.parse_args()


scanned_files = os.scandir(args.dir)
for filename in scanned_files:
        if filename.is_file():
            if fnmatch.fnmatch(filename, '*.json'):

                with Popen(["md5sum", filename], stdout=PIPE) as proc:
                    checksum = proc.stdout.read().split()[0]

                    if checksum in checksums:
                        duplicates.append(filename)
                    checksums[checksum]= filename

                    if filename in duplicates :
                        print(f"this file: {filename} is duplicated")
                    else :

                        json_list=[json.loads(line) for line in open(os.path.join(args.dir, filename),'rb')]
                        jsond=json.loads(json.dumps(json_list))
                        df= pd.DataFrame(jsond)
                        df=df[['a','tz','r','u','t','ll','hc','cy']]
                        pd.options.mode.chained_assignment = None
                        df['web_browser']= df.a.str.extract(r'^([\w\-]+)', expand=True)
                        df['operating_sys']= df.a.str.extract(r'\(([^)]+)\)', expand=True)
                        df['operating_sys']= df.operating_sys.str.extract(r'^([\w\-]+)', expand=True)
                        df['from_url']= df.r.str.extract(r'([\w_-]+(?:(?:\.[\w_-]+)+))', expand=True)
                        df['to_url']= df.u.str.extract(r'([\w_-]+(?:(?:\.[\w_-]+)+))', expand=True)
                        df.rename(columns={'tz':'time_zone'}, inplace=True)
                        df.rename(columns={'t':'time_in'}, inplace=True)
                        df.rename(columns={'hc':'time_out'}, inplace=True)
                        df.rename(columns={'cy':'city'}, inplace=True)
                        df['ll']=df['ll'].fillna('')
                        df[['longitude','latitude']] = pd.DataFrame(df['ll'].values.tolist(), index=df.index)
                        df.replace('', np.nan, inplace=True)
                        df = df.dropna(axis=0)
                        if not args.unix :
                            creation_timestamp=[]
                            for i, row in df.iterrows():
                                stamp = pd.to_datetime(row['time_in'], unit = 's').tz_localize(row['time_zone']).tz_convert('UTC')
                                creation_timestamp.append(stamp)
                            df['time_in'] =creation_timestamp

                        if not args.unix:
                            creation_timestamp2=[]
                            for i, row in df.iterrows():
                                stamp2 = pd.to_datetime(row['time_out'], unit = 's').tz_localize(row['time_zone']).tz_convert('UTC')
                                creation_timestamp2.append(stamp2)
                            df['time_out'] =creation_timestamp2
                        df = df[['web_browser','operating_sys','from_url','to_url','city','longitude','latitude','time_zone','time_in','time_out']]
                        file_name,file_extension = os.path.splitext(filename)
                        file_name= file_name+".csv"

                        df.to_csv(file_name, index = False)
                        #src = file_name
                        #dst = '/home/hihsma/Desktop/Target'
                        #shutil.move(src, dst)
                        print(f"number of rows has bees transformed :  {df['operating_sys'].count()}")
                        print(f"The file path : {file_name}")
print("total time of script execution =  %s seconds " % (time.time() - start_time))







# In[ ]:





