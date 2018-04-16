import pip
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'logging_gelf'])

import sys
import os
import logging
import csv
import json
import datetime
import pandas as pd
import logging_gelf.formatters
import logging_gelf.handlers
from keboola import docker

### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

### Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")
"""
logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
    )
logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)
# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])
"""

### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
outputBucket = cfg.get_parameters()["outputBucket"]

### Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


def get_tables(in_tables):
    """
    Evaluate input table names.
    """
    input_list = []

    ### input file
    for table in in_tables:
        in_name = table["full_path"]
        in_destination = table["destination"]
        logging.info("Data table: " + str(in_name))
        logging.info("Input table source: " + str(in_destination))
        input_list.append(in_name)
    
    return input_list

def get_output_tables(out_tables):
    """
    Evaluate output table names.
    """

    ### input file
    table = out_tables[0]
    in_name = table["full_path"]
    in_destination = table["source"]
    logging.info("Data table: " + str(in_name))
    logging.info("Input table source: " + str(in_destination))

    return in_name

def produce_manifest(file_name):
    """
    Dummy function to return header per file type.
    """

    file = "/data/out/tables/"+str(file_name)+".manifest"
    destination_part = file_name.split(".csv")[0]

    manifest_template = {#"source": "myfile.csv"
                         "destination": "in.c-mybucket.table"
                         #"incremental": True
                         #,"primary_key": ["VisitID","Value","MenuItem","Section"]
                         #,"columns": [""]
                         #,"delimiter": "|"
                         #,"enclosure": ""
                        }

    column_header = []

    manifest = manifest_template
    #manifest["columns"] = column_header
    #manifest["source"] = str(file_name)
    manifest["destination"] = outputBucket+str(destination_part)

    try:
        with open(file, 'w') as file_out:
            json.dump(manifest, file_out)
            logging.info("Output manifest file produced.")
    except Exception as e:
        logging.error("Could not produce output file manifest.")
        logging.error(e)
    
    return

def main():
    """
    Main execution script.
    """

    table_list = get_tables(in_tables)

    for i in table_list:
        filename = i.split("/data/in/tables/")[1]
        
        #with open(i, mode='rt', encoding='utf-8') as in_file, open(output_table, mode='wt', encoding='utf-8') as out_file:
        with open(i, mode='rt', encoding='utf-8') as in_file, open(DEFAULT_FILE_DESTINATION+filename, mode='wt', encoding='utf-8') as out_file:
            #now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")+" +0000"
            lazy_lines = (line.replace('\0', '') for line in in_file)
            reader = csv.DictReader(lazy_lines, lineterminator='\n')
            writer = csv.DictWriter(out_file, fieldnames=['data', 'time', 'file'], lineterminator='\n')
            writer.writeheader()
            logging.info("Outputting: {0}".format(filename))
            for row in reader:
                writer.writerow({ "data": json.dumps(row), "time": now, "file": filename })
        produce_manifest(filename)

    return


if __name__ == "__main__":

    main()

    logging.info("Done.")