import math
import os

from datetime import datetime
from dotenv import load_dotenv
from hLabs.eclaim.file_extractor import open_csv_files, open_dbf_files, save_json_files, json_dump
from hLabs.eclaim.bundle_package import create_bundle_resource_eclaim

load_dotenv()

def process(path_2_files: str ,files_type: str, import_time: datetime, set_no: int, slash: str):
    # Import Environment Variable
    local_output_path = os.getenv('LOCAL_OUTPUT_PATH')
    hos_addr = os.getenv('HOS_ADDR')
    hos_name = os.getenv('HOS_NAME')
    os_vm = os.getenv('OS')
    set_files_name = os.getenv('SET_FILES_NAME').split(',')
    if files_type == 'dbf':
        print('Reading..dbf..files')
        eclaim_17_df,eclaim_17_name = open_dbf_files(path_2_files,set_files_name, slash)
    else:
        print('Reading..txt..files')
        eclaim_17_df,eclaim_17_name = open_csv_files(path_2_files,set_files_name, slash)
    # Prepare Bundle Resource From DBF
    bundle_resource = create_bundle_resource_eclaim(eclaim_17_df,eclaim_17_name,hos_addr,hos_name,os_vm)
    bundle_resource = json_dump(bundle_resource)
    save_json_files(local_output_path,bundle_resource)