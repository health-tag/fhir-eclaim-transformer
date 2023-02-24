from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import pandas as pd
from dbfread import DBF

'''
hn|clinic|dateopd|timeopd|seq|uuc
653586040|0121|20220901|0925|065150995|1
'''

@dataclass
class OpdCsvRow:
    sequence: str = None
    hospital_number: str = None
    optype: str = None
    clinic: str = None
    dateopd: str = None
    timeopd: str = None
    uuc: str = None



def open_opd_file(file_path: PathLike) -> list[OpdCsvRow]:
    match Path(file_path).suffix:
        case ".dbf":
            dbf = DBF(file_path)
            df = pd.DataFrame(iter(dbf))
        case ".txt":
            df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    items = list[OpdCsvRow]()
    for i, row in df.iterrows():
        items.append(OpdCsvRow(sequence=row["seq"],
                               hospital_number=row["hn"],
                               clinic=row["clinic"],
                               optype=5,
                               dateopd=row["dateopd"],
                               timeopd=row["timeopd"],
                               uuc=row["uuc"]))
    return items
