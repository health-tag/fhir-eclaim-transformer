from dataclasses import dataclass
from os import PathLike
from dbfread import DBF
import pandas as pd
import numpy as np
import re
@dataclass
class AerRow:
    hn: str
    an: str
    dateopd: str
    authae: str
    aedate: str
    aetime: str
    aetype: str
    refer_no: str
    refmaini: str
    ireftype: str
    refmaino: str
    oreftype: str
    ucae: str
    emtype: str
    seq: str
    def get_dict_from_rows():
        dict = {}
        for key,value in AerRow.__dict__["__annotations__"].items():
            value = str(value).split("'")[1]
            dict[key] = value
        return dict
    def get_arr_from_rows():
        return list(AerRow.__dict__["__annotations__"].keys())
def validate_rows_in_files(df,val_columns):
    for i in list(set(val_columns) - set(df.columns.values)):
        df[i] = df.get(i, None)
    df['aedate'] = pd.to_datetime(df['aedate'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df['dateopd'] = pd.to_datetime(df['dateopd'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df = df.replace(np.nan, None)
    return df
def open_aer_csv(file_path: PathLike):
    df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,AerRow.get_arr_from_rows())
    return df
def open_aer_dbf(file_path: PathLike):
    dbf = DBF(file_path)
    df = pd.DataFrame(iter(dbf))
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,AerRow.get_arr_from_rows())
    return df