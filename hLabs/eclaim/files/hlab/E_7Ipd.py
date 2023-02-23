from dataclasses import dataclass
from os import PathLike
from dbfread import DBF
import pandas as pd
import numpy as np
import re
@dataclass
class IpdRow:
    hn: str
    an: str
    dateadm: str
    timeadm: str
    datedsc: str
    timedsc: str
    dischs: str
    discht: str
    warddsc: str
    dept: str
    adm_w: str
    uuc: str
    svctype: str
    def get_dict_from_rows():
        dict = {}
        for key,value in IpdRow.__dict__["__annotations__"].items():
            value = str(value).split("'")[1]
            dict[key] = value
        return dict
    def get_arr_from_rows():
        return list(IpdRow.__dict__["__annotations__"].keys())
def validate_rows_in_files(df,val_columns):
    for i in list(set(val_columns) - set(df.columns.values)):
        if i == 'svctype':
            df[i] = df.get(i, 'IMP')
        df[i] = df.get(i, None)
    df['dateadm'] = pd.to_datetime(df['dateadm'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df['datedsc'] = pd.to_datetime(df['datedsc'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df = df.replace(np.nan, None)
    return df
def open_ipd_csv(file_path: PathLike):
    df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,IpdRow.get_arr_from_rows())
    return df
def open_ipd_dbf(file_path: PathLike):
    dbf = DBF(file_path)
    df = pd.DataFrame(iter(dbf))
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,IpdRow.get_arr_from_rows())
    return df