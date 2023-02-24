from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import pandas as pd
from dbfread import DBF

'''
hn       |dateopd |clinic|oper   |dropid|person_id    |seq
657201754|20220905|0121  |9007811|2366  |7198786521964|065153022
'''

@dataclass
class OopCsvRow:
    sequence:str
    hospital_number:str
    dateopd:str
    clinic:str
    operation:str
    dropid:str
    citizen_id:str


def open_oop_file(file_path: PathLike) -> list[OopCsvRow]:
    match Path(file_path).suffix:
        case ".dbf":
            dbf = DBF(file_path)
            df = pd.DataFrame(iter(dbf))
        case ".txt":
            df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)

    df.columns = df.columns.str.lower()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    items = list[OopCsvRow]()
    for i, row in df.iterrows():
        items.append(OopCsvRow(sequence=row["seq"],
                                hospital_number=row["hn"],
                                dateopd=row["dateopd"],
                                clinic=row["clinic"],
                                operation=row["oper"],
                                dropid=row["dropid"],
                                citizen_id=row["person_id"]))
    return items