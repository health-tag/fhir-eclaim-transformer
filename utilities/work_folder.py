import os
import uuid
import zipfile
from datetime import datetime
from pathlib import Path, PurePath

import jsonpickle
from watchdog.events import FileSystemEventHandler, EVENT_TYPE_DELETED, EVENT_TYPE_CREATED
from watchdog.observers.polling import PollingObserver

import time


def checkfiles_in_folder(folder_path: Path):
    print(f"-> {folder_path}")

    elaims_checklist = dict[str, bool]()
    elaims_checklist["ins"] = False
    elaims_checklist["pat"] = False
    elaims_checklist["opd"] = False
    elaims_checklist["orf"] = False
    elaims_checklist["odx"] = False
    elaims_checklist["oop"] = False
    elaims_checklist["cht"] = False
    elaims_checklist["dru"] = False

    files = Path(folder_path).glob("*")
    is_done = False
    for file in files:
        lower_name = file.name.lower()
        if "done" in lower_name:
            is_done = True
        if "error" in lower_name:
            is_done = True
        if "working" in lower_name:
            is_done = True

        for key, value in iter(elaims_checklist.items()):
            if key in file.name.lower():
                elaims_checklist[key] = True

    is_eclaims = all(elaims_checklist.values()) and not is_done
    if is_eclaims:
        print(f"{folder_path} will be processed as E-Claims Folders.")
        return "eclaims"
    else:
        return None


def findWorkingFolder(path: PurePath, iteration=0):
    if (iteration > 10):
        return False
    else:
        parent = path.parent
        if (parent.name.lower() == "workingdir"):
            return parent
        else:
            return findWorkingFolder(parent, iteration + 1)


class Handler(FileSystemEventHandler):
    pendingJob = dict[str, bool]()

    @staticmethod
    def on_any_event(event):
        source_path = Path(event.src_path)
        if event.is_directory:
            return
        if event.event_type == EVENT_TYPE_DELETED and not (
                source_path.name.lower() == "done" or source_path.name.lower() == "error"):
            return

        job_folder_path = source_path.parent
        if job_folder_path.name not in Handler.pendingJob or ~Handler.pendingJob[job_folder_path.name]:
            result = checkfiles_in_folder(job_folder_path)
            match result:
                case "eclaims":
                    Handler.pendingJob[job_folder_path.name] = True
                    run_eclaims_folder(job_folder_path)
                    Handler.pendingJob[job_folder_path.name] = False


class WorkingDirWatcher:
    def __init__(self, directory: Path):
        self.observer = PollingObserver()
        self.watchDirectory = directory

    def start(self):
        self.observer = PollingObserver()
        event_handler = Handler()
        self.observer.schedule(event_handler, str(self.watchDirectory), recursive=True)
        self.observer.start()
        print(f'Watching {self.watchDirectory} folder for any changes')

    def stop(self):
        self.observer.stop()
        self.observer.join()

def run_eclaims_folder(folder_path: Path):
    print(f"Converting Eclaim Files in {folder_path.absolute()}")
    files = list(folder_path.glob("*"))
    if len(files) == 0:
        print(f"No file found!")
    else:
        print(f"{len(files)} file found")
        for file in files:
            print(file)
    _1ins_path: Path | None = None # HealthTAG
    _2pat_path: Path | None = None # HealthTAG
    _3opd_path: Path | None = None # HealthTAG
    _4orf_path: Path | None = None # HealthTAG
    _5odx_path: Path | None = None # HealthTAG
    _6oop_path: Path | None = None # HealthTAG
    _7ipd_path: Path | None = None # H-LAB
    _8irf_path: Path | None = None # H-LAB
    _9idx_path: Path | None = None # H-LAB
    _10iop_path: Path | None = None # H-LAB
    _11cht_path: Path | None = None # HealthTAG
    _12cha_path: Path | None = None # HealthTAG
    _13aer_path: Path | None = None # H-LAB
    _14adp_path: Path | None = None # H-LAB
    _16dru_path: Path | None = None # HealthTAG
    #_17lab_fu_path: Path | None = None # H-LAB
    for file in files:
        if "ins" in file.name.lower():
            _1ins_path = file
        if "pat" in file.name.lower():
            _2pat_path = file
        if "opd" in file.name.lower():
            _3opd_path = file
        if "orf" in file.name.lower():
            _4orf_path = file
        if "odx" in file.name.lower():
            _5odx_path = file
        if "oop" in file.name.lower():
            _6oop_path = file
        if "ipd" in file.name.lower():
            _7ipd_path = file
        if "irf" in file.name.lower():
            _8irf_path = file
        if "idx" in file.name.lower():
            _9idx_path = file
        if "iop" in file.name.lower():
            _10iop_path = file
        if "cht" in file.name.lower():
            _11cht_path = file
        if "cha" in file.name.lower():
            _12cha_path = file
        if "aer" in file.name.lower():
            _13aer_path = file
        if "adp" in file.name.lower():
            _14adp_path = file
        if "dru" in file.name.lower():
            _16dru_path = file

    if _1ins_path is None or _2pat_path is None or _3opd_path is None \
            or _4orf_path is None or _5odx_path is None or _6oop_path is None \
            or _7ipd_path is None or _8irf_path is None or _9idx_path is None \
            or _10iop_path is None or _11cht_path is None or _12cha_path is None \
            or _13aer_path is None or _14adp_path is None or _16dru_path is None:
        print("Required files not found!")
        return
    print(
        f"Processing {_1ins_path.name} {_2pat_path.name} {_3opd_path.name} {_4orf_path.name} {_5odx_path.name} {_6oop_path.name} {_11cht_path.name} {_16dru_path.name}")
    eclaims_process(results, _1ins_path=str(_1ins_path), _2pat_path=str(_2pat_path), _3opd_path=str(_3opd_path),
                    _4orf_path=str(_4orf_path), _5odx_path=str(_5odx_path), _6oop_path=str(_6oop_path),
                    _11cht_path=str(_11cht_path), _16dru_path=str(_16dru_path))
    return


def watch_folder(folder_path: Path):
    watcher = WorkingDirWatcher(folder_path)
    watcher.start()
    try:
        while True:
            time.sleep(3)
    except KeyboardInterrupt:
        watcher.stop()
        print(f'Stop watching {folder_path} folder')


def check_job_folder(folder_path: Path, iteration=0):
    sub_paths = list(filter(lambda sp: sp.is_dir, list(folder_path.glob("**"))))
    if len(sub_paths) == 0:
        return
    else:
        if iteration == 0:
            print(f"Checking on {folder_path} folder for any existing work")
        for sub_path in sub_paths:
            if sub_path.is_dir():
                result = checkfiles_in_folder(sub_path)
                match result:
                    case "eclaims":
                        run_eclaims_folder(sub_path)
        if iteration == 0:
            check_job_folder(folder_path, iteration=1)
        if iteration == 1:
            print("Finish running current works")
