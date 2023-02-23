import argparse
import os
import sys
import time
import uuid
import zipfile
from datetime import datetime
from pathlib import Path

from utilities.work_folder import check_job_folder


def banner():
    print("***********************************")
    print("* FHIR 17 documents Transformer v1*")
    print("*         21 February 2023         *")
    print("***********************************")


if __name__ == '__main__':
    banner()
    parser = argparse.ArgumentParser(description='HealthTAG FHIR Transformer')
    parser.add_argument('--watch', dest='watch_mode', action='store_true',
                        help='Use watch mode. Please read the manual to understand how to use this mode')
    parser.add_argument('--name', dest='folder_name', action='store',
                        help='Specify name of folder inside "workingdir" folder')
    args = parser.parse_args()

    if (args.watch_mode is True):
        path = Path("workingdir")
        Path(path).mkdir(exist_ok=True)
        check_job_folder(path)
        sys.exit(watch_folder(path))

    if (args.folder_name is None):
        print("Please specify --type and --name argument")
        sys.exit(1)
    else:
        sys.exit(run_csop_folder(Path(f"workingdir/{args.folder_name}")))
