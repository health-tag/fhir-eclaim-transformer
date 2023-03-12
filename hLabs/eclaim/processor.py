import os

from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from hLabs.eclaim.file_extractor import open_csv_files, open_dbf_files, save_json_files, json_dump
from hLabs.eclaim.bundle_package import create_bundle_resource_eclaim

load_dotenv()

def process(path_2_files: str ,files_type: str, slash: str, output_folder: Path):
    # Import Environment Variable
    hos_addr = os.getenv('HOS_ADDR')
    hos_name = os.getenv('HOS_NAME')
    set_files_name = os.getenv('SET_FILES_NAME').split(',')
    if files_type == '.dbf':
        print('Reading..dbf..files')
        eclaim_17_df,eclaim_17_name = open_dbf_files(path_2_files,set_files_name, slash)
    else:
        print('Reading..txt..files')
        eclaim_17_df,eclaim_17_name = open_csv_files(path_2_files,set_files_name, slash)
    # Prepare Bundle Resource
    bundle_resource = create_bundle_resource_eclaim(eclaim_17_df,eclaim_17_name,hos_addr,hos_name)
    bundle_resource = json_dump(bundle_resource)
    save_json_files(output_folder,bundle_resource)