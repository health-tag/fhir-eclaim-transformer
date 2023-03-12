from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import pandas as pd
from dbfread import DBF

"""
hn       |an|date    |chrgitem |amount|person_id    |seq
658574908|  |20220915|31       |645.00|9253080438132|065158037
"""


@dataclass
class ChaCsvRow:
    hospital_number: str
    citizen_id: str
    date: str
    chrgitem: str
    amount: str
    sequence: str


def open_cha_file(file_path: PathLike) -> list[ChaCsvRow]:
    match Path(file_path).suffix:
        case ".dbf":
            dbf = DBF(file_path)
            df = pd.DataFrame(iter(dbf))
        case ".txt":
            df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    items = list[ChaCsvRow]()
    for row in df.to_dict("records"):
        items.append(ChaCsvRow(sequence=row["seq"],
                               hospital_number=row["hn"],
                               citizen_id=row["person_id"],
                               date=row["date"],
                               chrgitem=row["chrgitem"],
                               amount=row["amount"]))
    return items
