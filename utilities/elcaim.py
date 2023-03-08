import platform
import os
from pathlib import Path
from typing import Optional

from hLabs.eclaim.processor import process as hLabs_process
from healthTag.eclaim.processor import process_all as healthtag_process

def process_eclaim_folder(files: dict[str, Optional[Path]]):
    healthtag_process(_1ins_path= files["ins"], _2pat_path= files["pat"], _3opd_path= files["opd"],
                      _4orf_path= files["orf"], _5odx_path= files["odx"], _6oop_path= files["oop"],
                      _11cht_path= files["cht"], _12cha_path= files["cha"], _16dru_path= files["dru"],
                      output_folder= files["ins"].parent
                      )
    slash = "\\" if platform.system() == "Windows" else "/"
    hLabs_process([os.path.join(f.parent.name , f.name) for f in files.values()], "txt", slash)