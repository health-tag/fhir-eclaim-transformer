from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Optional

from watchdog.events import FileSystemEventHandler, EVENT_TYPE_DELETED
from watchdog.observers.polling import PollingObserver

import time

from utilities.elcaim import process_eclaim_folder


@dataclass
class CheckFilesResult:
    type: str
    folder_path: Path
    files: dict[str, Optional[Path]]


def checkfiles_in_folder(folder_path: Path):
    print(f"-> {folder_path}")

    elaims_checklist = dict[str, Optional[Path]]()
    elaims_checklist["ins"] = None  # 1 HealthTAG
    elaims_checklist["pat"] = None  # 2 HealthTAG
    elaims_checklist["opd"] = None  # 3 HealthTAG
    elaims_checklist["orf"] = None  # 4 HealthTAG
    elaims_checklist["odx"] = None  # 5 HealthTAG
    elaims_checklist["oop"] = None  # 6 HealthTAG
    elaims_checklist["ipd"] = None  # 7 H-LAB
    elaims_checklist["irf"] = None  # 8 H-LAB
    elaims_checklist["idx"] = None  # 9 H-LAB
    elaims_checklist["iop"] = None  # 10 H-LAB
    elaims_checklist["cht"] = None  # 11 HealthTAG
    elaims_checklist["cha"] = None  # 12 HealthTAG
    elaims_checklist["cha"] = None  # 12 HealthTAG
    elaims_checklist["aer"] = None  # 13 H-LAB
    elaims_checklist["adp"] = None  # 14 H-LAB
    elaims_checklist["dru"] = None  # 16 HealthTAG

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
                elaims_checklist[key] = file

    is_eclaims = all([x is not None for x in elaims_checklist.values()]) and not is_done
    if is_eclaims:
        print(f"{folder_path} will be processed as E-Claims Folders.")
        return CheckFilesResult("eclaims", folder_path, elaims_checklist)
    else:
        return None


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
                if result is not None:
                    match result.type:
                        case "eclaims":
                            run_eclaim_folder(result)
        if iteration == 0:
            check_job_folder(folder_path, iteration=1)
        if iteration == 1:
            print("Finish running current works")


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
                    run_eclaim_folder(result)
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


def watch_folder(folder_path: Path):
    watcher = WorkingDirWatcher(folder_path)
    watcher.start()
    try:
        while True:
            time.sleep(3)
    except KeyboardInterrupt:
        watcher.stop()
        print(f'Stop watching {folder_path} folder')


def run_eclaim_folder(result: CheckFilesResult):
    if result is None:
        print("No file found!")
        return
    all_files_available = True
    for key, path in result.files.items():
        if path is None:
            print(f"Required {key} file not found!")
            all_files_available = False
    if not all_files_available:
        return
    print(f"Processing eclaims folder at {result.folder_path.absolute()}")
    process_eclaim_folder(result.files)
    return
