import os
import numpy as np
import pandas as pd

# log.EVNTtoHITS

df_orig = pd.DataFrame(columns=['pre-job input', 'opening input file', 'upto appmgr start', 'initialisation', 'event loop', 'ending'])
df_orig.to_csv('csvfiles/stageTiming.csv', columns=['pre-job input', 'opening input file', 'upto appmgr start', 'initialisation', 'event loop', 'ending'])

filecount = 0
included = 0

for subdir, dirs, files in os.walk('/work/d60/d60/shared/optimisation/benchmark/test1/output'):
    for file in files:
        #print os.path.join(subdir, file)
        filepath = subdir + os.sep + file

        if filepath.endswith('.EVNTtoHITS'):
            #print (filepath)
            filecount = filecount + 1
            
            df = pd.DataFrame()
            df = pd.read_csv('csvfiles/stageTiming.csv')
            df = df.drop(df.columns[[0]], axis=1)
            
            open('temp.txt','w').writelines([ line for line in open(filepath) if ('Setting up DBRelease' in line or 'in ISF_Input' in line or 'Welcome to ApplicationMgr' in line or 'Event Counter process created' in line or 'Statuses of sub-processes' in line) ])
            with open(filepath,'rb') as source, open('temp.txt','a') as output:
                #source.seek(-75, 2)
                #output.writelines(source.read())
                source.seek(-2, 2)
                while source.read(1) != b"\n":
                    source.seek(-2, 1)
                output.writelines(str(source.readline()))
            
            with open('temp.txt',"r") as f:
                timelist = [r.split()[0] for r in f]
            
            ftr = [3600,60,1]
            timelist = map(lambda x: sum([a*b for a,b in zip(ftr, [int(i) for i in x.split(":")])]), timelist)
            
            timelist2 = []
            timelist2 = np.diff(timelist)
            '''
            if timelist2.size < 6:
                timelist2 = np.append(timelist2, [999], axis=0)
		        #timelist2.resize(6)
                #timelist2 = np.insert(timelist2, -6, 9999)
            '''
            if timelist2.size == 6:
                included = included + 1
                print (filepath)
                
                df = df.append(pd.Series(timelist2, index=['pre-job input', 'opening input file', 'upto appmgr start', 'initialisation', 'event loop', 'ending']), ignore_index=True)
                #df = df.append(pd.Series(timelist2), ignore_index=True)
                df.to_csv('csvfiles/stageTiming.csv')

os.remove('temp.txt')
print ("\nFinished scanning %d of %d log files. Output: csvfiles/stageTiming.csv\n") % (included, filecount)


# 
