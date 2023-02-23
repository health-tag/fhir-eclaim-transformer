import encodings
import xmltodict
import sys
import re
import pandas as pd
import numpy as np
import json
from dbfread import DBF
from fhir_transformer.eclaim.files.hlab.E_1Ins import open_ins_csv, open_ins_dbf
from fhir_transformer.eclaim.files.hlab.E_2Pat import open_pat_csv, open_pat_dbf
from fhir_transformer.eclaim.files.hlab.E_7Ipd import open_ipd_csv, open_ipd_dbf
from fhir_transformer.eclaim.files.hlab.E_8Irf import open_irf_csv, open_irf_dbf
from fhir_transformer.eclaim.files.hlab.E_9Idx import open_idx_csv, open_idx_dbf
from fhir_transformer.eclaim.files.hlab.E_10Iop import open_iop_csv, open_iop_dbf
from fhir_transformer.eclaim.files.hlab.E_11Cht import open_cht_csv, open_cht_dbf
from fhir_transformer.eclaim.files.hlab.E_12Cha import open_cha_csv, open_cha_dbf
from fhir_transformer.eclaim.files.hlab.E_13Aer import open_aer_csv, open_aer_dbf
from fhir_transformer.eclaim.files.hlab.E_14Adp import open_adp_csv, open_adp_dbf
from fhir_transformer.eclaim.files.hlab.E_15Lvd import open_lvd_csv, open_lvd_dbf
from fhir_transformer.eclaim.files.hlab.E_16Dru import open_dru_csv, open_dru_dbf
from fhir_transformer.eclaim.files.hlab.E_17Labfu import open_labfu_csv, open_labfu_dbf



def open_csv_files(file_path: str,set_files_name: list, slash: str):
    eclaim_17_df = []
    eclaim_17_name = []
    for i in file_path:
        file_name = i.split(slash)[4][:3]
        file_name = file_name.lower()
        if file_name == 'ins':
            frame_csv = open_ins_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'pat':
            frame_csv = open_pat_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'ipd':
            frame_csv = open_ipd_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'irf':
            frame_csv = open_irf_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'lvd':
            frame_csv = open_lvd_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'idx':
            frame_csv = open_idx_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'iop':
            frame_csv = open_iop_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'dru':
            frame_csv = open_dru_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'cha':
            frame_csv = open_cha_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'cht':
            frame_csv = open_cht_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'aer':
            frame_csv = open_aer_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'adp':
            frame_csv = open_adp_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'labfu':
            frame_csv = open_labfu_csv(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
    for i in list(set(set_files_name) - set(eclaim_17_name)):
        print(f'{i} file is not found')
        print(f'Mocking {i} file .. Successfully')
        df = pd.DataFrame()
        eclaim_17_df.append(df)
        eclaim_17_name.append(i)           
    return eclaim_17_df,eclaim_17_name
def open_dbf_files(file_path: str,set_files_name: list, slash: str):
    eclaim_17_df = []
    eclaim_17_name = []
    for i in file_path:
        file_name = i.split(slash)[4][:3]
        file_name = file_name.lower()
        if file_name == 'ins':
            frame_csv = open_ins_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'pat':
            frame_csv = open_pat_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'ipd':
            frame_csv = open_ipd_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'irf':
            frame_csv = open_irf_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'lvd':
            frame_csv = open_lvd_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'idx':
            frame_csv = open_idx_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'iop':
            frame_csv = open_iop_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'dru':
            frame_csv = open_dru_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'cha':
            frame_csv = open_cha_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'cht':
            frame_csv = open_cht_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'aer':
            frame_csv = open_aer_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'adp':
            frame_csv = open_adp_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
        if file_name == 'labfu':
            frame_csv = open_labfu_dbf(i)
            eclaim_17_df.append(frame_csv)
            eclaim_17_name.append(file_name)
    for i in list(set(set_files_name) - set(eclaim_17_name)):
        print(f'{i} file is not found')
        print(f'Mocking {i} file .. Successfully')
        df = pd.DataFrame()
        eclaim_17_df.append(df)
        eclaim_17_name.append(i)                  
    return eclaim_17_df,eclaim_17_name
def save_json_files(file_path,json_data):
    # Writing to sample.json
    with open(file_path, "w") as outfile:
        outfile.write(json_data)
    print('Saving Json Files..')
def json_dump(json_data):
    return json.dumps(json_data, indent=2, ensure_ascii=False, cls=NpEncoder)
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)