import os
from collections.abc import Iterable

from pathlib import Path

from dotenv import load_dotenv
from hLabs.eclaim.file_extractor import open_csv_files, open_dbf_files, save_json_files
from hLabs.eclaim.bundle_package import create_bundle_resource_eclaim

load_dotenv()


def process(files_path: Iterable[Path], files_extension: str, output_folder: Path):
    # Import Environment Variable
    hos_addr = os.getenv('HOS_ADDR')
    hos_name = os.getenv('HOS_NAME')
    set_files_name = os.getenv('SET_FILES_NAME').split(',')
    if files_extension == '.dbf':
        eclaim_17_df, eclaim_17_name = open_dbf_files(files_path, set_files_name)
    else:
        eclaim_17_df, eclaim_17_name = open_csv_files(files_path, set_files_name)
    # Prepare Bundle Resource
    bundle_resource = create_bundle_resource_eclaim(eclaim_17_df, eclaim_17_name, hos_addr, hos_name)
    save_json_files(output_folder, bundle_resource)
