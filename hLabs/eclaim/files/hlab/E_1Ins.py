from dataclasses import dataclass
from os import PathLike
from dbfread import DBF
import pandas as pd
import numpy as np
import re
@dataclass
class InsRow:
    hn: str
    an: str
    inscl: str
    subtype: str
    cid: str
    hcode: str
    datein: str
    dateexp: str
    hospmain: str
    hospsub: str
    govcode: str
    govname: str
    permitno: str
    docno: str
    ownrpid: str
    ownname: str
    htype: str
    seq: str
    def get_dict_from_rows():
        dict = {}
        for key,value in InsRow.__dict__["__annotations__"].items():
            value = str(value).split("'")[1]
            dict[key] = value
        return dict
    def get_arr_from_rows():
        return list(InsRow.__dict__["__annotations__"].keys())
def validate_rows_in_files(df,val_columns):
    for i in list(set(val_columns) - set(df.columns.values)):
        df[i] = df.get(i, None)
    df['datein'] = pd.to_datetime(df['datein'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df['dateexp'] = pd.to_datetime(df['dateexp'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df = df.replace(np.nan, None)
    return df
def open_ins_csv(file_path: PathLike):
    df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,InsRow.get_arr_from_rows())
    return df
def open_ins_dbf(file_path: PathLike):
    dbf = DBF(file_path)
    df = pd.DataFrame(iter(dbf))
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,InsRow.get_arr_from_rows())
    return df