import io
import os
import shutil
import requests
import hashlib
import zipfile
import tarfile

import pandas as pd
from tqdm.notebook import tqdm

CURRENT_PATH = os.path.abspath(__file__)
TEMP_PATH = os.path.join(os.path.dirname(CURRENT_PATH), "temp_directory")

# def download_and_extract_zip(url, checksum):
#     response = requests.get(url)  
#     response.raise_for_status()
#     zip_content = io.BytesIO(response.content)

#     hasher = hashlib.sha256() # Calculate the SHA256 checksum of downloaded zip file
#     hasher.update(response.content)
#     calculated_checksum = hasher.hexdigest()

#     if calculated_checksum != checksum:
#         raise ValueError(f"Checksum does not match. Downloaded file may be corrupted.\n Checksum should be: '{checksum}' but was '{calculated_checksum}'.")

#     zip_file = zipfile.ZipFile(zip_content)  # Create a ZipFile object 
#     zip_file.extractall(path=TEMP_PATH)  # Extract the contents
#     zip_file.close()  # Close the ZipFile object
#     extracted_files = zip_file.namelist()  

#     return extracted_files

def download_and_extract_zip(url, checksum):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 KB
    progress_bar = tqdm(total=total_size, unit='B', unit_scale=True)
    zip_content = io.BytesIO()
    
    hasher = hashlib.sha256() # Calculate the SHA256 checksum of downloaded zip file

    for data in response.iter_content(block_size):
        progress_bar.update(len(data))
        zip_content.write(data)
        hasher.update(data)
    
    progress_bar.close()

    calculated_checksum = hasher.hexdigest()
    if calculated_checksum != checksum:
        raise ValueError(f"Checksum does not match. Downloaded file may be corrupted.\n Checksum should be: '{checksum}' but was '{calculated_checksum}'.")
    
    zip_content.seek(0)
    zip_file = zipfile.ZipFile(zip_content)  # Create a ZipFile object 
    extracted_files = zip_file.namelist()
    zip_file.extractall(path=TEMP_PATH)  # Extract the contents
    zip_file.close()  # Close the ZipFile object

    return extracted_files

def download_and_extract_tar(url, checksum):
    response = requests.get(url)  
    response.raise_for_status()  

    hasher = hashlib.sha256() # Calculate the SHA256 checksum of the downloaded tar.gz file
    hasher.update(response.content)
    calculated_checksum = hasher.hexdigest()

    if calculated_checksum != checksum:
        raise ValueError(f"Checksum does not match. Downloaded file may be corrupted.\n Checksum should be: '{checksum}' but was '{calculated_checksum}'.")

    tar_content = io.BytesIO(response.content)  # Read the tar.gz file content from the response
    tar = tarfile.open(fileobj=tar_content, mode="r")  # Open the tar.gz file for reading

    tar.extractall(path=TEMP_PATH)
    extracted_files = tar.getnames()
    tar.close()  # Close the tarfile object

    return extracted_files

def download_text_file(url, checksum, encoding="utf-8"):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 KB
    progress_bar = tqdm(total=total_size, unit='B', unit_scale=True)
    content = b''

    hasher = hashlib.sha256()

    for data in response.iter_content(block_size):
        progress_bar.update(len(data))
        hasher.update(data)
        content += data

    progress_bar.close()

    calculated_checksum = hasher.hexdigest()
    if calculated_checksum != checksum:
        raise ValueError(f"Checksum does not match. Downloaded file may be corrupted.\n Checksum should be: '{checksum}' but was '{calculated_checksum}'.")

    file_data = content.decode('utf-8')
    dataframe = pd.read_csv(io.StringIO(file_data), delimiter=',')  # Update delimiter if needed

    return dataframe

def load_dataframes_from_csv(file_list, encoding="utf-8", extra_path=""):
    dataframes = []
    for i, file in enumerate(file_list):
        # print(i, file)
        df = pd.read_csv(TEMP_PATH + "/" + file, encoding=encoding)
        dataframes.append(df)
    return tuple(dataframes)

def delete_directory():
    shutil.rmtree(TEMP_PATH)