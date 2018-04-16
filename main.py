import sys
import os
import csv
import json
import datetime

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"

for filename in os.listdir(DEFAULT_FILE_INPUT):
    if filename.endswith(".csv"):
        
        print("Outputting: {0}".format(filename))

        with open(DEFAULT_FILE_INPUT+filename, mode='rt', encoding='utf-8') as in_file, open(DEFAULT_FILE_DESTINATION+filename, mode='wt', encoding='utf-8') as out_file:
            #now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")+" +0000"
            lazy_lines = (line.replace('\0', '') for line in in_file)
            reader = csv.DictReader(lazy_lines, lineterminator='\n')
            writer = csv.DictWriter(out_file, fieldnames=['data', 'time', 'file'], lineterminator='\n')
            writer.writeheader()
            
            print("Rows: {0}".format(filename))
            
            for row in reader:
                writer.writerow({ "data": json.dumps(row), "time": now, "file": filename })

print("Done!")
