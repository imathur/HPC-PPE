import os
import numpy as np
import pandas as pd
import re
import itertools

# log.EVNTtoHITS

df_orig = pd.DataFrame(columns=['trfExe.execute', 'merging', 'validation', 'total'])
df_orig.to_csv('stdout_stageTiming.csv', columns=['trfExe.execute', 'merging', 'validation', 'total'])

filecount = 0
included = 0

for subdir, dirs, files in os.walk('/work/d60/d60/shared/optimisation/benchmark/test1/output'):
    for file in files:
        #print os.path.join(subdir, file)
        filepath = subdir + os.sep + file

        if filepath.endswith('stdout.txt'):
            filecount = filecount + 1
            
            df = pd.DataFrame()
            df = pd.read_csv('stdout_stageTiming.csv')
            df = df.drop(df.columns[[0]], axis=1)
            
            open('temp.txt','w').writelines([ line for line in open(filepath) if \
                                             ('Starting execution of EVNTtoHITS' in line or \
                                              'EVNTtoHITS executor returns 0' in line or \
                                              'Starting execution of HITSMergeAthenaMP' in line or \
                                              'HITSMergeAthenaMP executor returns 0' in line or \
                                              'Executor HITSMergeAthenaMP validated successfully' in line) ])
            
            with open(filepath,'rb') as source, open('temp.txt','a') as output:
                source.seek(-2, 2)
                while source.read(1) != b"\n":
                    source.seek(-2, 1)
                output.writelines(str(source.readline()))
               
            with open('temp.txt',"r") as source:
                timelist = [re.findall('\d\d:\d\d:\d\d', str(source.read()))]
                #for line in source:
                	#print(source.readline())
            
            timelist = list(itertools.chain(*timelist))
            del timelist[-1]
            #print(timelist)
            
            ftr = [3600,60,1]
            timelist = map(lambda x: sum([a*b for a,b in zip(ftr, [int(i) for i in x.split(":")])]), timelist)
            
            #print(len(timelist))
            
            if len(timelist) == 6:
                with open(filepath,'rb') as source:
                    alltimestamps = [re.findall('\d\d:\d\d:\d\d', str(source.read()))]
                    
                alltimestamps = list(itertools.chain(*alltimestamps))
                timerange = [alltimestamps[0], alltimestamps[-1]]
                timerange = list(map(lambda x: sum([a*b for a,b in zip(ftr, [int(i) for i in x.split(":")])]), timerange))
                
                timelist2 = [timelist[1]-timelist[0], timelist[3]-timelist[2], timelist[5]-timelist[4], timerange[1] - timerange[0]]
                #print(timelist2)
                
                included = included + 1
                print (filepath)
                df = df.append(pd.Series(timelist2, index=['trfExe.execute', 'merging', 'validation', 'total']), \
                               ignore_index=True)
                df.to_csv('stdout_stageTiming.csv')

os.remove('temp.txt')
print ("\nFinished scanning %d of %d log files\n") % (included, filecount)


