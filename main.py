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

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"

def main():
    """
    Main execution script.
    """

    for filename in os.listdir(DEFAULT_FILE_INPUT):
        
        #with open(i, mode='rt', encoding='utf-8') as in_file, open(output_table, mode='wt', encoding='utf-8') as out_file:
        with open(DEFAULT_FILE_INPUT+filename, mode='rt', encoding='utf-8') as in_file, open(DEFAULT_FILE_DESTINATION+filename, mode='wt', encoding='utf-8') as out_file:
            #now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")+" +0000"
            lazy_lines = (line.replace('\0', '') for line in in_file)
            reader = csv.DictReader(lazy_lines, lineterminator='\n')
            writer = csv.DictWriter(out_file, fieldnames=['data', 'time', 'file'], lineterminator='\n')
            writer.writeheader()
            logging.info("Outputting: {0}".format(filename))
            for row in reader:
                writer.writerow({ "data": json.dumps(row), "time": now, "file": filename })

    return


if __name__ == "__main__":

    main()

    logging.info("Done.")