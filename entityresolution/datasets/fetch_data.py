# Imports
import pandas as pd

from ._utils import (
    download_and_extract_zip, 
    download_and_extract_tar, 
    download_text_file,
    load_dataframes_from_csv, 
    delete_directory,
    )

"""
    TODO:
    - Create brief documentation
    - Remainding datasets to include
        - Uni-Leipzig
                - Amazone-GoogleProducts
                - Abt-Buy
                - Affiliations
                - Geographical Settlements
                - Music Brainz 200K
                - Music Brainz 2M
                - North Carolina Voters 5M
        - ...
    - Move meta data? Python module
    - Remember to credit and License
    - Update code using `import tempfile`

    Resources:
    - Dataset source: https://dbs.uni-leipzig.de/research/projects/object_matching/benchmark_datasets_for_entity_resolution

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

MUSIC_BRAINZ_20K_DETAILS = {
    "url": "https://www.informatik.uni-leipzig.de/~saeedi/musicbrainz-20-A01.csv.dapo",
    "checksum": "527a94f24f7e813a9bc3fef35a635f13e195516966b308140a0dd2926afbb97d"
}

# ...

MUSIC_BRAINZ_20M_DETAILS = {
    "url": "https://www.informatik.uni-leipzig.de/~saeedi/musicbrainz-20000-A01.csv.dapo",
    "checksum": "6da9d92d65b55f65cc7b3cdd7acfe38765f25dec388526ddab83b4b8ccfb6ad8"
}

# ...

NORTH_CAROLINA_VOTERS_10M_DETAILS = {
    "url": "https://www.informatik.uni-leipzig.de/~saeedi/10Party-ocp20.tar.gz",
    "checksum": "8ee9c723e9b74470e828a66608669bdc6213b29df603488eb203e7b8202fe007"
}

def fetch_DBLP_ACM():
    file_list = download_and_extract_zip(url=DBLP_ACM_DETAILS["url"], checksum=DBLP_ACM_DETAILS["checksum"])
    df = load_dataframes_from_csv(file_list, encoding="ISO-8859-1")
    delete_directory() # Delet TAMP_PATH
    return df

def fetch_DBLP_SCHOLAR():
    file_list = download_and_extract_zip(url=DBLP_SCHOLAR_DETAILS["url"], checksum=DBLP_SCHOLAR_DETAILS["checksum"])
    df = load_dataframes_from_csv(file_list, encoding="ISO-8859-1")
    delete_directory() # Delet TAMP_PATH
    return df

def fetch_MUSIC_BRAINZ_20K():
    # file_list = download_and_extract_zip(url=MUSIC_BRAINZ_20M_DETAILS["url"], checksum=MUSIC_BRAINZ_20M_DETAILS["checksum"])
    # df = load_dataframes_from_csv(file_list, encoding="ISO-8859-1")
    df = download_text_file(url=MUSIC_BRAINZ_20K_DETAILS["url"], checksum=MUSIC_BRAINZ_20K_DETAILS["checksum"])
    delete_directory() # Delet TAMP_PATH
    return df

def fetch_MUSIC_BRAINZ_20M():
    # file_list = download_and_extract_zip(url=MUSIC_BRAINZ_20M_DETAILS["url"], checksum=MUSIC_BRAINZ_20M_DETAILS["checksum"])
    # df = load_dataframes_from_csv(file_list, encoding="ISO-8859-1")
    df = download_text_file(url=MUSIC_BRAINZ_20M_DETAILS["url"], checksum=MUSIC_BRAINZ_20M_DETAILS["checksum"])
    delete_directory() # Delet TAMP_PATH
    return df

def fetch_NORTH_CAROLINA_VOTERS_10M():
    file_list = download_and_extract_tar(url=NORTH_CAROLINA_VOTERS_10M_DETAILS["url"], checksum=NORTH_CAROLINA_VOTERS_10M_DETAILS["checksum"])

    # Remove elements that are not paths to a '.csv' file
    file_list = [file_name for file_name in file_list if file_name.endswith(".csv")]
    df = load_dataframes_from_csv(file_list)
    delete_directory() # Delet TAMP_PATH
    return df