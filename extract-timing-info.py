import os
import numpy as np
import pandas as pd

# Create dataframe in which dataset will be stored

df = pd.DataFrame(columns=['pre-job input', 'opening input file', 'upto appmgr start', 'initialisation', 'event loop', 'ending'])

# Initialize counting variables

filecount = 0
included = 0

# Loop through all files in directory test1/output/ and search for log.EVNTtoHITS files for each Athena job

for subdir, dirs, files in os.walk('/work/d60/d60/shared/optimisation/benchmark/test1/output'):
    for file in files:
        filepath = subdir + os.sep + file

        if filepath.endswith('.EVNTtoHITS'):
            filecount = filecount + 1
            
            # Extract lines containing certain strings from the log file and write the lines to a list
            
            linelist = [ line.rstrip('\n') for line in open(filepath) if ('Setting up DBRelease' in line or \
                                                                          'in ISF_Input' in line or \
                                                                          'Welcome to ApplicationMgr' in line or \
                                                                          'Event Counter process created' in line or \
                                                                          'Statuses of sub-processes' in line) ]
            
            # Extract last line of log file and append it to the list
            
            with open(filepath,'rb') as source:
                source.seek(-2, 2)
                while source.read(1) != b"\n":
                    source.seek(-2, 1)
                linelist.append(str(source.readline()))
            
            # Create a list 'timelist' of the first word (string containing timestamp) on each line in the temporary file
            
            timelist = [line.split()[0] for line in linelist]
            
            # Convert each timestamp string element in the list to its equivalent value in seconds
            
            ftr = [3600,60,1]
            timelist = map(lambda x: sum([a*b for a,b in zip(ftr, [int(i) for i in x.split(":")])]), timelist)
            
            # Create a new list 'timelist2' containing the difference of each consecutive pair of elements from 'timelist'
            
            timelist2 = []
            timelist2 = np.diff(timelist)
            
            # If the list 'timelist2' has 6 elements (i.e., if the job finished execution and wasn't stopped prematurely), append the list as a new row to the dataframe
            
            if timelist2.size == 6:
                included = included + 1
                print (filepath)
                df = df.append(pd.Series(timelist2, index=['pre-job input', 'opening input file', 'upto appmgr start', 'initialisation', 'event loop', 'ending']), ignore_index=True)
	            

# Write dataframe back to CSV file and print confirmation of completion of program.
                
df.to_csv('csvfiles/stageTiming.csv')
print ("\nFinished scanning %d of %d log files. Output: csvfiles/stageTiming.csv\n") % (included, filecount)


