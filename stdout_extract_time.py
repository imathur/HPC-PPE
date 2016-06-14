import os
import numpy as np
import pandas as pd
import re
import itertools

# Create dataframe and CSV file in which dataset will be stored

df = pd.DataFrame(columns=['start', 'end', 'total time'])

# Initialize counting variables

filecount = 0
included = 0

# Loop through all files in directory test1/output/ and search for log.EVNTtoHITS files for each Athena job

for subdir, dirs, files in os.walk('/work/d60/d60/shared/optimisation/benchmark/test1/output'):
    for file in files:
        filepath = subdir + os.sep + file

        if filepath.endswith('stdout.txt'):
            filecount = filecount + 1
            
            # Extract all timestamps matching regular expression from the log file and write the strings to a list
            
            with open(filepath,'rb') as source:
                timelist = [re.findall('\d\d:\d\d:\d\d', str(source.read()))]
                
            timelist = list(itertools.chain(*timelist))
            
            # Save the first and last timestamp strings to a new list
            
            timelist2 = [timelist[0], timelist[-1]]
            
            # Convert each timestamp string element in the list to its equivalent value in seconds
            
            ftr = [3600,60,1]
            timelist2 = list(map(lambda x: sum([a*b for a,b in zip(ftr, [int(i) for i in x.split(":")])]), timelist2))
            
            # Calculate time interval between first and last timestamp and add to the list 'timelist2'
            
            timelist2.append(timelist2[1] - timelist2[0])
            
            if timelist2[2] > 60:
                included = included + 1
                print (filepath)
                df = df.append(pd.Series(timelist2, index=['start', 'end', 'total time']), ignore_index=True)
                
# Write dataframe back to CSV file and print confirmation of completion of program.

df.to_csv('csvfiles/totalTime.csv')
print ("\nFinished scanning %d of %d log files\n") % (included, filecount)



