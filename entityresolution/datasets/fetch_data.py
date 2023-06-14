# Imports
import pandas as pd

from ._utils import (
    download_and_extract_zip, 
    download_and_extract_tar, 
    load_dataframes_from_csv, 
    delete_directory
    )

"""
    Dataset source: https://dbs.uni-leipzig.de/research/projects/object_matching/benchmark_datasets_for_entity_resolution
"""

DBLP_ACM_DETAILS = {
    "url": "https://dbs.uni-leipzig.de/file/DBLP-ACM.zip",
    "checksum": "e37e7ed8e06722499e6d9e94583fdec279207b13ce6270a37595b6a5d594bc40",
}

DBLP_SCHOLAR_DETAILS = {
    "url": "https://dbs.uni-leipzig.de/file/DBLP-Scholar.zip",
    "checksum": "80b9ddbe89d6b78ab583f801e86a8c451e94dbf1a57ba88dfbd180245cff138b"
}

# ...

NORTH_CAROLINA_VOTERS_10M_DETAILS = {
    "url": "https://www.informatik.uni-leipzig.de/~saeedi/10Party-ocp20.tar.gz",
    "checksum": "8ee9c723e9b74470e828a66608669bdc6213b29df603488eb203e7b8202fe007"
}

def fetch_DBLP_ACM():
    file_list = download_and_extract_zip(url=DBLP_ACM_DETAILS["url"], checksum=DBLP_ACM_DETAILS["checksum"])
    dblp_acm = load_dataframes_from_csv(file_list, "ISO-8859-1")
    delete_directory() # Delet TAMP_PATH
    return dblp_acm

def fetch_DBLP_SCHOLAR():
    file_list = download_and_extract_zip(url=DBLP_SCHOLAR_DETAILS["url"], checksum=DBLP_SCHOLAR_DETAILS["checksum"])
    print(file_list)
    dblp_scholar = load_dataframes_from_csv(file_list, "ISO-8859-1")
    delete_directory() # Delet TAMP_PATH
    return dblp_scholar

def fetch_NORTH_CAROLINA_VOTERS_10M():
    file_list = download_and_extract_tar(url=NORTH_CAROLINA_VOTERS_10M_DETAILS["url"], checksum=NORTH_CAROLINA_VOTERS_10M_DETAILS["checksum"])
    print(file_list)
    dblp_scholar = load_dataframes_from_csv(file_list)
    # delete_directory() # Delet TAMP_PATH
    return dblp_scholar