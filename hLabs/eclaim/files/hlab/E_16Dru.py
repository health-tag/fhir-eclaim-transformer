from dataclasses import dataclass
from os import PathLike
from dbfread import DBF
import pandas as pd
import numpy as np
import re
@dataclass
class DruRow:
    hcode: str
    hn: str
    an: str
    clinic: str
    person_id: str
    date_serv: str
    did: str
    didname: str
    amount: str
    drugpric: str
    drugcost: str
    didstd: str
    unit: str
    unit_pack: str
    seq: str
    drugremark: str
    pa_no: str
    totcopay: str
    use_status: str
    total: str
    sigcode: str
    sigtext: str
    provider: str
    def get_dict_from_rows():
        dict = {}
        for key,value in DruRow.__dict__["__annotations__"].items():
            value = str(value).split("'")[1]
            dict[key] = value
        return dict
    def get_arr_from_rows():
        return list(DruRow.__dict__["__annotations__"].keys())
def validate_rows_in_files(df,val_columns):
    for i in list(set(val_columns) - set(df.columns.values)):
        df[i] = df.get(i, None)
    df['date_serv'] = pd.to_datetime(df['date_serv'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df = df.replace(np.nan, None)
    return df
def open_dru_csv(file_path: PathLike):
    df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,DruRow.get_arr_from_rows())
    return df
def open_dru_dbf(file_path: PathLike):
    dbf = DBF(file_path)
    df = pd.DataFrame(iter(dbf))
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,DruRow.get_arr_from_rows())
    return df