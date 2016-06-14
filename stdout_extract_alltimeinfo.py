import os
import numpy as np
import pandas as pd
import re
import itertools

# Create dataframe and CSV file in which dataset will be stored

df = pd.DataFrame(columns=['trfExe.execute', 'merging', 'validation', 'total'])

# Initialize counting variables

filecount = 0
included = 0

# Loop through all files in directory test1/output/ and search for log.EVNTtoHITS files for each Athena job

for subdir, dirs, files in os.walk('/work/d60/d60/shared/optimisation/benchmark/test1/output'):
    for file in files:
        filepath = subdir + os.sep + file

        if filepath.endswith('stdout.txt'):
            filecount = filecount + 1
            
            # Extract lines containing certain strings from the log file and write the lines to a list
            
            linelist = [ line.rstrip('\n') for line in open(filepath) if ('Starting execution of EVNTtoHITS' in line or \
                                                                          'EVNTtoHITS executor returns 0' in line or \
                                                                          'Starting execution of HITSMergeAthenaMP' in line or \
                                                                          'HITSMergeAthenaMP executor returns 0' in line or \
                                                                          'Executor HITSMergeAthenaMP validated successfully' in line) ]
            
            # Extract last line of log file and append it to the list
            
            with open(filepath,'rb') as source:
                source.seek(-2, 2)
                while source.read(1) != b"\n":
                    source.seek(-2, 1)
                linelist.append(str(source.readline()))
               
            # Extract all timestamps matching regular expression from the list 'linelist' and write the strings to a list
            
            timelist = [re.findall('\d\d:\d\d:\d\d', line) for line in linelist]
                
            timelist = list(itertools.chain(*timelist))
            del timelist[-1] # Delete last element as it is a repetition of the previous element
            
            # Convert each timestamp string element in the list to its equivalent value in seconds
            
            ftr = [3600,60,1]
            timelist = map(lambda x: sum([a*b for a,b in zip(ftr, [int(i) for i in x.split(":")])]), timelist)
            
            # If the list 'timelist2' has 6 elements (i.e., if the job finished execution and wasn't stopped prematurely),
            
            if len(timelist) == 6:
            
            	# Extract all timestamps matching regular expression from the log file and write the strings to a list
            	
                with open(filepath,'rb') as source:
                    alltimestamps = [re.findall('\d\d:\d\d:\d\d', str(source.read()))]
                    
                alltimestamps = list(itertools.chain(*alltimestamps))
                
                # Save the first and last timestamp strings to a new list, and convert each timestamp string element in the list to its equivalent value in seconds
                
                timerange = [alltimestamps[0], alltimestamps[-1]]
                timerange = list(map(lambda x: sum([a*b for a,b in zip(ftr, [int(i) for i in x.split(":")])]), timerange))
                
                # Calculate required time intervals from extracted timestamps
                
                timelist2 = [timelist[1]-timelist[0], timelist[3]-timelist[2], timelist[5]-timelist[4], timerange[1] - timerange[0]]
                
                # Append the list 'timelist2' as a new row to the dataframe
                
                included = included + 1
                print (filepath)
                df = df.append(pd.Series(timelist2, index=['trfExe.execute', 'merging', 'validation', 'total']), \
                               ignore_index=True)

# Write dataframe back to CSV file and print confirmation of completion of program.
                
df.to_csv('csvfiles/stdout_stageTiming.csv')
print ("\nFinished scanning %d of %d log files\n") % (included, filecount)


