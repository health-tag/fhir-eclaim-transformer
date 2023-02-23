from dataclasses import dataclass
from os import PathLike
from dbfread import DBF
import pandas as pd
import numpy as np
import re
@dataclass
class PatRow:
    h_code: str
    hn: str
    changwat: str
    amphur: str
    dob: str
    sex: str
    marriage: str
    occupa: str
    nation: str
    person_id: str
    namepat: str
    title: str
    fname: str
    lname: str
    idtype: str
    def get_dict_from_rows():
        dict = {}
        for key,value in PatRow.__dict__["__annotations__"].items():
            value = str(value).split("'")[1]
            dict[key] = value
        return dict
    def get_arr_from_rows():
        return list(PatRow.__dict__["__annotations__"].keys())
def validate_rows_in_files(df,val_columns):
    for i in list(set(val_columns) - set(df.columns.values)):
        df[i] = df.get(i, None)
    df['dob'] = pd.to_datetime(df['dob'], errors = 'coerce').dt.strftime("%Y-%m-%d")
    df = df.replace(np.nan, None)
    return df
def open_pat_csv(file_path: PathLike):
    df = pd.read_csv(file_path, encoding="utf8", delimiter="|",dtype=str)
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,PatRow.get_arr_from_rows())
    return df
def open_pat_dbf(file_path: PathLike):
    dbf = DBF(file_path)
    df = pd.DataFrame(iter(dbf))
    df.columns = df.columns.str.lower()
    df = validate_rows_in_files(df,PatRow.get_arr_from_rows())
    return df