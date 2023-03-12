from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import pandas as pd
from dbfread import DBF

'''
hn       |dateopd X|clinic X|refer|refertype|seq
650069735|20220902 |        |10675|2        |065151491

dateopd clinic ไม่ใช้ เพราะใช้จาก 3 OPD ได้
'''


@dataclass
class OrfCsvRow:
    sequence: str = None
    hospital_number: str = None
    refer: str = None
    refer_type: str = None


def open_orf_file(file_path: PathLike) -> list[OrfCsvRow]:
    match Path(file_path).suffix:
        case ".dbf":
            dbf = DBF(file_path)
            df = pd.DataFrame(iter(dbf))
        case ".txt":
            df = pd.read_csv(file_path, encoding="utf8", delimiter="|", dtype=str)
    df.columns = df.columns.str.lower()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    items = list[OrfCsvRow]()
    for row in df.to_dict("records"):
        items.append(OrfCsvRow(sequence=row["seq"],
                               hospital_number=row["hn"],
                               refer=row["refer"],
                               refer_type=row["refertype"]))
    return items
