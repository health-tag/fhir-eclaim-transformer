from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import pandas as pd
from dbfread import DBF

"""
hn       |an|date    |total |paid|pttype|person_id    |seq
651218347|  |20220912|532.50|0.00|77    |1842228343492|065156262
"""

@dataclass
class ChtCsvRow:
    hospital_number: str
    citizen_id: str
    date: str
    total:str
    paid:str
    patient_type:str
    sequence:str

def open_cht_file(file_path: PathLike) -> list[ChtCsvRow]:
    match Path(file_path).suffix:
        case ".dbf":
            dbf = DBF(file_path)
            df = pd.DataFrame(iter(dbf))
        case ".txt":
            df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    items = list[ChtCsvRow]()
    for row in df.to_dict("records"):
        items.append(ChtCsvRow(sequence=row["seq"],
                               hospital_number=row["hn"],
                               citizen_id=row["person_id"],
                               date=row["date"],
                               total=row["total"],
                               paid=row["paid"],
                               patient_type=row["pttype"]))
    return items