from pathlib import Path

def run_eclaims_folder(folder_path: Path):
    print(f"Converting Eclaim Files in {folder_path.absolute()}")
    files = list(folder_path.glob("*"))
    if len(files) == 0:
        print(f"No file found!")
    else:
        print(f"{len(files)} file found")
        for file in files:
            print(file)
    _1ins_path: Path | None = None
    _2pat_path: Path | None = None
    _3opd_path: Path | None = None
    _4orf_path: Path | None = None
    _5odx_path: Path | None = None
    _6oop_path: Path | None = None
    _11cht_path: Path | None = None
    _16dru_path: Path | None = None
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
        if "cht" in file.name.lower():
            _11cht_path = file
        if "dru" in file.name.lower():
            _16dru_path = file

    if _1ins_path is None or _2pat_path is None or _3opd_path is None or _4orf_path is None or _5odx_path is None or _6oop_path is None or _11cht_path is None or _16dru_path is None:
        print("Requires all of ins pat opd orf odx oop cht dru files")
        return
    print(f"Processing {_1ins_path.name} {_2pat_path.name} {_3opd_path.name} {_4orf_path.name} {_5odx_path.name} {_6oop_path.name} {_11cht_path.name} {_16dru_path.name}")
    healthtag_eclaims_process(results,_1ins_path=str(_1ins_path), _2pat_path=str(_2pat_path), _3opd_path=str(_3opd_path), _4orf_path=str(_4orf_path), _5odx_path=str(_5odx_path), _6oop_path=str(_6oop_path), _11cht_path=str(_11cht_path), _16dru_path=str(_16dru_path))
    return