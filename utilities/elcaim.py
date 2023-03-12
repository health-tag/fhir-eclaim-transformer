import platform
import os
from pathlib import Path
from typing import Optional
from hLabs.eclaim.processor import process as hLabs_process
from healthTag.eclaim.processor import process_all as healthtag_process


def process_eclaim_folder(files: dict[str, Optional[Path]]):
    output = Path(files["ins"].parent, "output")
    output.mkdir(exist_ok=True)
    healthtag_process(_1ins_path=files["ins"], _2pat_path=files["pat"], _3opd_path=files["opd"],
                      _4orf_path=files["orf"], _5odx_path=files["odx"], _6oop_path=files["oop"],
                      _11cht_path=files["cht"], _12cha_path=files["cha"], _16dru_path=files["dru"],
                      output_folder=output
                      )
    files_type = files["ins"].suffix
    hLabs_process(files.values(), files_type,  output_folder=output.joinpath("bundle_2.json"))
    with open(Path(files["ins"].parent,"done"), "w", encoding='utf8') as f:
        pass

