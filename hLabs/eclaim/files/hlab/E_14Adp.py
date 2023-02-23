from dataclasses import dataclass
from os import PathLike
from dbfread import DBF
import pandas as pd
import numpy as np
import re
@dataclass
class AdpRow:
    hn: str
    an: str
    dateopd: str
    type: str
    code: str
    qty: str
    rate: str
    seq: str
    cagcode: str
    dose: str
    ca_type: str
    serialno: str
    totcopay: str
    use_status: str
    total: str
    qtyday: str
    tmltcode: str
    status1: str
    bi: str
    clinic: str
    itemsrc: str
    provider: str
    gravida: str
    ga_week: str
    dcip: str
    lmp: str
    def get_dict_from_rows():
        dict = {}
        for key,value in AdpRow.__dict__["__annotations__"].items():
            value = str(value).split("'")[1]
            dict[key] = value
        return dict
    def get_arr_from_rows():
        return list(AdpRow.__dict__["__annotations__"].keys())
def validate_rows_in_files(df,val_columns):
    for i in list(set(val_columns) - set(df.columns.values)):
        df[i] = df.get(i, None)
    df['dateopd'] = pd.to_datetime(df['dateopd'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df = df.replace(np.nan, None)
    return df
def open_adp_csv(file_path: PathLike):
    df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,AdpRow.get_arr_from_rows())
    return df
def open_adp_dbf(file_path: PathLike):
    dbf = DBF(file_path)
    df = pd.DataFrame(iter(dbf))
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,AdpRow.get_arr_from_rows())
    return df