import argparse
import sys
from pathlib import Path

from utilities.work_folder import check_job_folder, watch_folder


def banner():
    print("***********************************")
    print("* FHIR 17 documents Transformer v1*")
    print("*           9 March 2023          *")
    print("***********************************")


if __name__ == '__main__':
    banner()
    parser = argparse.ArgumentParser(description='HealthTAG FHIR Transformer')
    parser.add_argument('--watch', dest='watch_mode', action='store_true',
                        help='Use watch mode. Please read the manual to understand how to use this mode')
    parser.add_argument('--path', dest='path', action='store',
                        help='Specify path of folder', required=False)
    args = parser.parse_args()

    if (args.watch_mode is True):
        path = Path("workingdir")
        Path(path).mkdir(exist_ok=True)
        Path("workingdir/dbf").mkdir(exist_ok=True)
        Path("workingdir/txt").mkdir(exist_ok=True)
        check_job_folder(path)
        sys.exit(watch_folder(path))

    if (args.path is None):
        print('Please use --path "path to folder" or use --watch argument to watch folder in workingdir')
        sys.exit(1)
    else:
        check_job_folder(Path(args.path))
