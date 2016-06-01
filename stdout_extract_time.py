import os
import numpy as np
import pandas as pd
import re
import itertools

# log.EVNTtoHITS

df_orig = pd.DataFrame(columns=['start', 'end', 'total time'])
df_orig.to_csv('totalTime.csv', columns=['start', 'end', 'total time'])

filecount = 0
included = 0

for subdir, dirs, files in os.walk('/work/d60/d60/shared/optimisation/benchmark/test1/output'):
    for file in files:
        #print os.path.join(subdir, file)
        filepath = subdir + os.sep + file

        if filepath.endswith('stdout.txt'):
            #print (filepath)
            filecount = filecount + 1
            
            df = pd.DataFrame()
            df = pd.read_csv('totalTime.csv')
            df = df.drop(df.columns[[0]], axis=1)
            
            with open(filepath,'rb') as source:
                timelist = [re.findall('\d\d:\d\d:\d\d', str(source.read()))]
                
            timelist = list(itertools.chain(*timelist))
            #print(timelist)
            
            timelist2 = [timelist[0], timelist[-1]]
            #print(timelist2)
            
            ftr = [3600,60,1]
            timelist2 = list(map(lambda x: sum([a*b for a,b in zip(ftr, [int(i) for i in x.split(":")])]), timelist2))
            
            timelist2.append(timelist2[1] - timelist2[0])
            #print(timelist2)
            
            if timelist2[2] > 60:
                included = included + 1
                print (filepath)
                df = df.append(pd.Series(timelist2, index=['start', 'end', 'total time']), ignore_index=True)
                df.to_csv('totalTime.csv')

print ("\nFinished scanning %d of %d log files\n") % (included, filecount)



