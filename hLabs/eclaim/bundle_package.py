import uuid
import json
import pandas as pd
import numpy as np
import hashlib
from hashlib import sha256

marriage_status_dict = { 
    "1": "โสด",
    "2": "คู่",
    "3": "ม่าย",
    "4": "หย่า",
    "5": "แยก",
    "6": "สมณะ",
    "9": "ไม่ทราบ"
}
marriage_status_dict_en = { 
    "1": "S",
    "2": "M",
    "3": "W",
    "4": "D",
    "5": "L",
    "9": "UNK"
}
uuc_dict = {
    "1" : "ใช้สิทธิ",
    "2" : "ไม่ใช้สิทธิ ไม่ขอเบิก"
}
discht_dict = {
    "1": "With Approval",
    "2": "Against Advice",
    "3": "By Escape",
    "4": "By Transfer",
    "5": "Other (specify)",
    "8": "Death Autopsy",
    "9": "Dead Non autopsy"
}
dischs_dict = {
    "1": "Complete Recovery",
    "2": "Improved",
    "3": "Not Improved",
    "4": "Normal Delivery",
    "5": "Un-Delivery",
    "6": "Normal child discharged with mother",
    "7": "Normal child discharged separately",
    "8": "Dead stillbirth",
    "9": "Dead"
}
diag_role_dict = {
    "1": "PRINCIPLE DX (การวินิจฉัยโรคหลัก)",
    "2": "CO-MORBIDITY (การวินิจฉัยโรคร่วม)",
    "3": "COMPLICATION (การวินิจฉัยโรคแทรก)",
    "4": "OTHER (อื่น ๆ)",
    "5": "EXTERNAL CAUSE (สาเหตุภายนอก)",
    "6": "Additional Code (รหัสเสริม)",
    "7": "Morphology Code (รหัสเกี่ยวกับเนื้องอก)"
}
procedure_type_dict = {
    "1": "Principal procedure",
    "2": "Secondary procedure",
    "3": "Others"
}
cha_type_dict = {
    "1" : "เบิกได้",
    "11" : "ค่าห้อง/ค่าอาหาร",
    "21" : "อวัยวะเทียม/อุปกรณ์ในการบำบัดรักษา",
    "31" : "ยาและสารอาหารทางเส้นเลือดที่ใช้ใน รพ.",
    "41" : "ยาที่: นำไปใช้ต่อที่บ้าน",
    "51" : "เวชภัณฑ์ที่ไม่ใช่ยา",
    "61" : "บริการโลหิตและส่วนประกอบของโลหิต",
    "71" : "ตรวจวินิจฉัยทางเทคนิคการแพทย์และพยาธิวิทยา",
    "81" : "ตรวจวินิจฉัยและรักษาทางรังสีวิทยา",
    "91" : "ตรวจวินิจฉัยโดยวิธีพิเศษอื่น ๆ",
    "A1" : "อุปกรณ์ของใช้และเครื่องมือทางการแพทย์",
    "B1" : "ทำหัตถการ และบริการวิสัญญี",
    "C1" : "ค่าบริการทางการพยาบาล",
    "D1" : "บริการทางทันตกรรม",
    "E1" : "บริการทางกายภาพบำบัด และเวชกรรมฟื้นฟู",
    "F1" : "บริการฝังเข็ม/การบำบัดของผู้ประกอบโรคศิลปะอื่น ๆ",
    "G1" : "ค่าห้องผ่าตัดและห้องคลอด",
    "H1" : "ค่าธรรมเนียมบุคลากรทางการแพทย์",
    "I1" : "บริการอื่น ๆ และส่งเสริมป้องกันโรค",
    "J1" : "บริการอื่น ๆ ที่ยังไม่จัดหมวด",
    "2"  : "ส่วนเกิน",
    "12" : "ค่าห้อง/ค่าอาหาร",
    "22" : "อวัยวะเทียม/อุปกรณ์ในการบำบัดรักษา",
    "32" : "ยาและสารอาหารทางเส้นเลือดที่ใช้ในรพ.",
    "42" : "ยาที่นำไปใช้ต่อที่บ้าน",
    "52" : "เวชภัณฑ์ที่ไม่ใช่ยา",
    "62" : "บริการโลหิตและส่วนประกอบของโลหิต",
    "72" : "ตรวจวินิจฉัยทางเทคนิคการแพทย์และพยาธิวิทยา",
    "82" : "ตรวจวินิจฉัยและรักษาทางรังสีวิทยา",
    "92" : "ตรวจวินิจฉัยโดยวิธีพิเศษอื่น ๆ",
    "A2" : "อุปกรณ์ของใช้และเครื่องมือทางการแพทย์",
    "B2" : "ทำหัตถการ และบริการวิสัญญี",
    "C2" : "ค่าบริการทางการพยาบาล",
    "D2" : "บริการทางทันตกรรม",
    "E2" : "บริการทางกายภาพบำบัด และเวชกรรมฟื้นฟู",
    "F2" : "บริการฝังเข็ม/การบำบัดของผู้ประกอบโรคศิลปะอื่น ๆ",
    "G2" : "ค่าห้องผ่าตัดและห้องคลอด",
    "H2" : "ค่าธรรมเนียมบุคลากรทางการแพทย์",
    "I2" : "บริการอื่น ๆ และส่งเสริมป้องกันโรค",
    "J2" : "บริการอื่น ๆ ที่ยังไม่จัดหมวด",
    "K1" : "พรบ."
}
adp_type_dict = {
    "1" : "HC (OPD)",
    "2" : "Instrument (หมวด 2)",
    "3" : "ค่าบริการอื่นๆ ที่ยังไม่ได้จัดหมวด",
    "4" : "ค่าส่งเสริมป้องกัน/บริการเฉพาะ",
    "5" : "Project code",
    "6" : "การรักษามะเร็งตามโปรโตคอล",
    "7" : "การรักษาโรคมะเร็งด้วยรังสีวิทยา",
    "8" : "OP REFER และ รายการ Fee Schedule (สามารถใช้ชื่อ TYPE หรือ TYPEADP ได้)",
    "9" : "ตรวจวินิจฉัยด้วยวิธีพิเศษอื่นๆ (หมวด 9)",
    "10" : "ค่าห้อง/ค่าอาหาร (หมวด 1)",
    "11" : "เวชภัณฑ์ที่ไม่ใช่ยา (หมวด 5)",
    "12" : "ค่าบริการทันตกรรม (หมวด 13)",
    "13" : "ค่าบริการฝังเข็ม (หมวด 15)",
    "14" : "บริการโลหิตและส่วนประกอบของโลหิต (หมวด 6)",
    "15" : "ตรวจวินิจฉัยทางเทคนิคการแพทย์และพยาธิวิทยา (หมวด 7)",
    "16" : "ค่าตรวจวินิจฉัยและรักษาทางรังสีวิทยา (หมวด 8)",
    "17" : "ค่าบริการทางการพยาบาล (หมวด 12)",
    "18" : "อุปกรณ์และเครื่องมือทางการแพทย์ (หมวด 10)",
    "19" : "ทำหัตถการและวิสัญญี (หมวด 11)",
    "20" : "ค่าบริการทางกายภาพบำบัดและเวชกรรมฟื้นฟู (หมวด 14)"
}
drug_mark_dict = {
    "EA" : "เกิดอาการไม่พึงประสงค์จากยาหรือแพ้ยาที่สามารถใช้ได้ในบัญชียาหลักแห่งชาติ",
    "EB" : "ผลการรักษาไม่บรรลุเป้าหมายแม้ว่าได้ใช้ยาในบัญชียาหลักแห่งชาติครบตามมาตรฐานการรักษาแล้ว",
    "EC" : "ไม่มีกลุ่มยาในบัญชียาหลักแห่งชาติให้ใช้ แต่ผู้ป่วยมีความจำเป็นในการใช้ยานี้ ตามข้อบ่งชี้ที่ได้ขึ้นทะเบียนไว้กับสำนักงานคณะกรรมการอาหารและยา",
    "ED" : "ผู้ป่วยมีภาวะหรือโรคที่ห้ามใช้ยาในบัญชีอย่างสมบูรณ์ หรือมีข้อห้ามการใช้ยาในบัญชีร่วมกับยาอื่น ที่ผู้ป่วยจำเป็นต้องใช้อย่างหลักเลี่ยงไม่ได้",
    "EE" : "ยาในบัญชียาหลักแห่งชาติมีราคาแพงกว่า (ในเชิงความคุ้มค่า)",
    "EF" : "ผู้ป่วยแสดงความจำนงต้องการ (เบิกไม่ได้)",
    "PA" : "ยากลุ่มที่ต้องขออนุมัติก่อนการใช้ (PA) เช่น ยามะเร็ง 6 ชนิด ยารักษากลุ่มโรครูมาติกและโรคสะเก็ดเงิน 2 ชนิด"
}
drug_use_status_dict = {
    "1" : "ใช้ในโรงพยาบาล",
    "2" : "ใช้ที่บ้าน",
    "3" : "ยาเกิน 2 สัปดาห์ (กลับบ้าน)",
    "4" : "ยาโรคเรื้อรัง (กลับบ้าน)"
}
refer_type_dict = {
    "A" : "อุบัติเหตุ",
    "E" : "ฉุกเฉิน",
    "NONE" : "ไม่เป็น A / E",
    "I" : "OP Refer ในจังหวัด",
    "O" : "OP Refer ข้ามจังหวัด",
    "C" : "ย้ายหน่วยบริการเกิดสิทธิทันที",
    "Z" : "บริการเชิงรุก"
}
refer_in_reason_dict = {
    "1" : "เพื่อการวินิจฉัยและรักษา",
    "2" : "เพื่อการวินิจฉัย",
    "3" : "เพื่อการรักษาและพื้นฟูต่อเนื่อง",
    "4" : "เพื่อการดูแลต่อใกล้บ้าน",
    "5" : "ตามความต้องการผู้ป่วย"
}
refer_priority_code_dict = {
    "1" : "ต้องการรักษาเป็นการด่วน",
    "2" : "ต้องผ่าตัดด่วน",
    "3" : "โรคที่คณะกรรมการกำหนด"
}
cancer_type_dict = {
    "Bd" : "Bladder",
    "Br" : "Breast",
    "Ch" : "Cholangiocarcinoma",
    "Cr" : "Colon & Rectum",
    "Cx" : "Cervix",
    "Es" : "Esophagus",
    "Ln" : "Lung (Non small cell)",
    "Lu" : "Lung (Small cell)",
    "Na" : "Nasopharynx",
    "Ov" : "Ovary",
    "Ps" : "Prostate",
    "Gca" : "มะเร็งทั่วไป"
}
provider_type_coverage_dict = {
    "primary" : "สถานบริการหลัก",
    "secondary" : "สถานบริการรอง",
    "primary-care" : "สถานบริการปฐมภูมิ"

}
gender_dict = {
    "1" : "male",
    "2" : "female"
}
provider_type_dict = {
    "1": "Main Contractor",
    "2": "Sub Contractor",
    "3": "Supra Contractor",
    "4": "Excellent",
    "5": "Super tertiary"
}
        
def create_bundle_resource_eclaim(eclaim_17_df,eclaim_17_name,h_code,h_name,os):
    for i in range(len(eclaim_17_df)):
        if eclaim_17_name[i] == 'ins':
            frame_ins = eclaim_17_df[i]
            frame_ins.columns = frame_ins.columns.str.upper()
        if eclaim_17_name[i] == 'pat':
            frame_pat = eclaim_17_df[i]
            frame_pat.columns = frame_pat.columns.str.upper()
        if eclaim_17_name[i] == 'ipd':
            frame_ipd = eclaim_17_df[i]
            frame_ipd.columns = frame_ipd.columns.str.upper()
        if eclaim_17_name[i] == 'irf':
            frame_irf = eclaim_17_df[i]
            frame_irf.columns = frame_irf.columns.str.upper()
        if eclaim_17_name[i] == 'idx':
            frame_idx = eclaim_17_df[i]
            frame_idx.columns = frame_idx.columns.str.upper()
        if eclaim_17_name[i] == 'iop':
            frame_iop = eclaim_17_df[i]
            frame_iop.columns = frame_iop.columns.str.upper()
        if eclaim_17_name[i] == 'cha':
            frame_cha = eclaim_17_df[i]
            frame_cha.columns = frame_cha.columns.str.upper()
        if eclaim_17_name[i] == 'adp':
            frame_adp = eclaim_17_df[i]
            frame_adp.columns = frame_adp.columns.str.upper()
        if eclaim_17_name[i] == 'dru':
            frame_dru = eclaim_17_df[i]
            frame_dru.columns = frame_dru.columns.str.upper()
        if eclaim_17_name[i] == 'aer':
            frame_aer = eclaim_17_df[i]
            frame_aer.columns = frame_aer.columns.str.upper()
        if eclaim_17_name[i] == 'cht':
            frame_cht = eclaim_17_df[i]
            frame_cht.columns = frame_cht.columns.str.upper()
        if eclaim_17_name[i] == 'lvd':
            frame_lvd = eclaim_17_df[i]
            frame_lvd.columns = frame_lvd.columns.str.upper()
        if eclaim_17_name[i] == 'labfu':
            frame_labfu = eclaim_17_df[i]
            frame_labfu.columns = frame_labfu.columns.str.upper()
    # Filter Nan
    frame_pat = frame_pat[~frame_pat['PERSON_ID'].isna()]
    unique_pat_hn = frame_pat['HN'].unique()
    frame_ipd = frame_ipd[frame_ipd.HN.isin(unique_pat_hn)]
    unique_ipd_hn = frame_ipd['HN'].unique()
    # init array bundle & dict
    entry = []
    hcode_id = "hcode" + str(h_code)
    # create resource
    for hn in unique_ipd_hn:
        bundle_resource = []
        claim = []
        ins = filter_visit_hn(frame_ins,hn)
        pat = filter_visit_hn(frame_pat,hn)
        ipd = filter_visit_hn(frame_ipd,hn)
        irf = filter_visit_an(frame_irf,ipd['AN'].values)
        idx = filter_visit_an(frame_idx,ipd['AN'].values)
        iop = filter_visit_an(frame_iop,ipd['AN'].values)
        cha = filter_visit_an(frame_cha,ipd['AN'].values)
        adp = filter_visit_an(frame_adp,ipd['AN'].values)
        dru = filter_visit_an(frame_dru,ipd['AN'].values)
        aer = filter_visit_an(frame_aer,ipd['AN'].values)
        cht = filter_visit_an(frame_cht,ipd['AN'].values)
        lvd = filter_visit_an(frame_lvd,ipd['AN'].values)
        labfu = filter_visit_an(frame_labfu,ipd['AN'].values)
        # Create Organization Resource
        bundle_organization_structure = create_organization_resource(h_name,h_code)
        # Create Patient Resource (PAT)
        bundle_patient_structure,bundle_observation_occupation_structure = create_patient_resource(pat,h_code)
        # Create Encounter Resource (IPD)
        bundle_encounter_structure,bundle_account_structure,bundle_observation_bw_structure = create_encounter_resource(ipd,bundle_patient_structure,h_code)
        # Create Coverage Resource (INS)
        bundle_coverage_structure = create_coverage_resource(ins,h_code)
        # Update Encounter By (INS)
        bundle_encounter_structure = update_encounter_resource_by_ins(ins,bundle_encounter_structure)
        # Update Encounter By (LVD)
        bundle_encounter_structure = update_encounter_resource_by_lvd(lvd,bundle_encounter_structure)  
        # Create Service Request Resource (IRF)
        bundle_service_req_structure = create_service_request_resource(irf,bundle_patient_structure,bundle_encounter_structure,h_code)
        # Update Encounter By (IRF)
        bundle_encounter_structure = update_encounter_resource_by_irf(irf,bundle_service_req_structure,bundle_encounter_structure)
        # Create Condition Resource
        bundle_condition_structure_arr = create_condition_resource(idx,aer,bundle_patient_structure,bundle_encounter_structure)
        # Update Encounter By (IDX)
        bundle_encounter_structure = update_encounter_resource_by_idx(idx,bundle_encounter_structure,bundle_condition_structure_arr)
        # Create Condition Resource
        bundle_procedure_structure_arr = create_procedure_resource(iop,bundle_patient_structure,bundle_encounter_structure)
        # Create Med Resource
        bundle_med_disp_structure_arr = create_medication_dispense_resource(dru,bundle_patient_structure,bundle_encounter_structure)
        bundle_med_req_structure_arr = create_medication_request_resource(dru,bundle_patient_structure,bundle_encounter_structure)
        # Create Claim Resource
        claim_items = create_claim_item(cha,adp,dru,bundle_encounter_structure)
        bundle_claim_structure = create_claim_resource(cha,claim_items,bundle_patient_structure,bundle_encounter_structure,h_code)              
        # Update Claim Resource By (CHT)
        bundle_claim_structure = update_claim_resource_by_cht(cht,bundle_claim_structure,bundle_patient_structure,bundle_encounter_structure,h_code)
        # Update Claim & Service Request By (AER)
        bundle_claim_structure= update_claim_resource_by_aer(aer,bundle_claim_structure,bundle_patient_structure,bundle_encounter_structure,h_code)
        bundle_service_req_structure = update_service_request_resource_by_aer(aer,irf,bundle_service_req_structure)
        # Create Observation & Condition By (ADP)
        if adp.shape[0] != 0:
            for i in range(adp.shape[0]):
                # Create Condition Resource By (ADP)
                bundle_condition_adp_structure = create_condition_resource_by_adp(adp,bundle_patient_structure,bundle_encounter_structure,i)
                if len(bundle_condition_adp_structure) > 0:
                    bundle_resource.append(bundle_condition_adp_structure)
                # Create Observation BT Resource By (ADP)
                bundle_observation_bt_structure = create_observation_bt_resource(adp,bundle_patient_structure,bundle_encounter_structure,i,h_code)
                if len(bundle_observation_bt_structure) > 0:
                    bundle_resource.append(bundle_observation_bt_structure)
                # Create Observation Covid Resource By (ADP)   
                bundle_observation_covid_structure = create_observation_covid_resource(adp,bundle_patient_structure,bundle_encounter_structure,i,h_code)
                if len(bundle_observation_covid_structure) > 0:
                    bundle_resource.append(bundle_observation_covid_structure)
                # Create Observation Dcip Resource By (ADP) 
                bundle_observation_dcip_structure = create_observation_dcip_resource(adp,bundle_patient_structure,bundle_encounter_structure,i,h_code)
                if len(bundle_observation_dcip_structure) > 0:
                    bundle_resource.append(bundle_observation_dcip_structure)
                # Create Observation Ga Resource By (ADP)     
                bundle_observation_ga_structure = create_observation_ga_resource(adp,bundle_patient_structure,bundle_encounter_structure,i,h_code)
                if len(bundle_observation_ga_structure) > 0:
                    bundle_resource.append(bundle_observation_ga_structure)
                # Create Observation Gravida Resource By (ADP) 
                bundle_observation_gravida_structure = create_observation_gravida_resource(adp,bundle_patient_structure,bundle_encounter_structure,i,h_code)
                if len(bundle_observation_gravida_structure) > 0:
                    bundle_resource.append(bundle_observation_gravida_structure)
        # Create Observation Labfu
        bundle_observation_labfu_structure = create_observation_lab_resource(labfu,bundle_patient_structure,bundle_encounter_structure,h_code)
        if len(bundle_organization_structure) > 0:  
            bundle_resource.append(bundle_organization_structure)
        if len(bundle_encounter_structure) > 0:  
            bundle_resource.append(bundle_encounter_structure)
        if len(bundle_patient_structure) > 0:  
            bundle_resource.append(bundle_patient_structure)
        if len(bundle_observation_occupation_structure) > 0:  
            bundle_resource.append(bundle_observation_occupation_structure)
        if len(bundle_service_req_structure) > 0:  
            bundle_resource.append(bundle_service_req_structure)
        if len(bundle_coverage_structure) > 0:  
            bundle_resource.append(bundle_coverage_structure)
        if len(bundle_observation_bw_structure) > 0:  
            bundle_resource.append(bundle_observation_bw_structure)
        if len(bundle_account_structure) > 0:  
            bundle_resource.append(bundle_account_structure)
        if len(bundle_condition_structure_arr) > 0:
            bundle_resource.extend(bundle_condition_structure_arr)
        if len(bundle_procedure_structure_arr) > 0:
            bundle_resource.extend(bundle_procedure_structure_arr)
        if len(bundle_med_disp_structure_arr) > 0:
            bundle_resource.extend(bundle_med_disp_structure_arr)
        if len(bundle_med_req_structure_arr) > 0:
            bundle_resource.extend(bundle_med_req_structure_arr)
        if len(bundle_observation_labfu_structure) > 0:
            bundle_resource.append(bundle_observation_labfu_structure)
        if len(bundle_claim_structure) > 0:   
            bundle_resource.append(bundle_claim_structure)
        entry.extend(bundle_resource)
    bundle_json = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry" : entry
    }
    print('Create Bundle Json Successfully')
    return bundle_json
def filter_visit_hn(df,hn):
  if df.shape[0] != 0:     
    return df[df.HN == hn].reset_index(drop=True)
  else:
    return df
def filter_visit_an(df,an):
  if df.shape[0] != 0:     
    return df[df.AN.isin(an)].reset_index(drop=True)
  else:
    return df

def create_coverage_resource(ins,h_code):
    # init bundle resource
    bundle_coverage_structure = dict()
    if ins.shape[0] != 0:
      coverage_resource = {
          "resourceType" : "Coverage",
          "id": f"hcode-{h_code}-uc-cid-{ins.iloc[0]['CID']}",
          "status": "active",
          "payor":[
            {
              "type":"Organization",
              "display":"UCS"
            }
          ]
      }
      if ins.iloc[0]['HOSPMAIN'] is not None and len(str(ins.iloc[0]['HOSPMAIN'])) > 0:
          if "extension" not in coverage_resource:
              coverage_resource["extension"] = []
          coverage_resource['extension'].append(
              {
                "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-coverage-contracted-provider",
                "extension": [
                  {
                    "url": "type",
                    "valueCodeableConcept": {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-meta-provider-type-coverage",
                          "code": "primary",
                          "display": "สถานบริการหลัก"
                        }
                      ]
                    }
                  },
                  {
                    "url": "provider",
                    "valueReference": {
                      "identifier": {
                        "system": "https://terms.sil-th.org/id/th-moph-hcode",
                        "value": ins.iloc[0]['HOSPMAIN']
                      }
                    }
                  }
                ]
              }
          )
      if ins.iloc[0]['HOSPSUB'] is not None and len(str(ins.iloc[0]['HOSPSUB'])) > 0:
          if "extension" not in coverage_resource:
              coverage_resource["extension"] = []
          coverage_resource['extension'].append(
              {
                "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-coverage-contracted-provider",
                "extension": [
                  {
                    "url": "type",
                    "valueCodeableConcept": {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-meta-provider-type-coverage",
                          "code": "primary-care",
                          "display": "สถานบริการปฐมภูมิ"
                        }
                      ]
                    }
                  },
                  {
                    "url": "provider",
                    "valueReference": {
                      "identifier": {
                        "system": "https://terms.sil-th.org/id/th-moph-hcode",
                        "value": ins.iloc[0]['HOSPSUB']
                      }
                    }
                  }
                ]
              }
          )
      if ins.iloc[0]['INSCL'] is not None and len(str(ins.iloc[0]['INSCL']))> 0:
          coverage_resource["type"] = {
            "coding" : [{
                "system" : "https://terms.sil-th.org/ValueSet/vs-eclaim-coverage-use",
                "code" : ins.iloc[0]['INSCL'] 
              }]
          }
      if ins.iloc[0]['CID'] is not None and len(str(ins.iloc[0]['CID'])) > 0:
          coverage_resource["beneficiary"] = {
              "reference" : f"Patient/cid-{ins.iloc[0]['CID']}",
              "identifier" : {
                "system" : "https://terms.sil-th.org/id/th-cid",
                "value" : ins.iloc[0]['CID']
              }
          }
      if ins.iloc[0]['DATEIN'] is not None:
          if "period" not in coverage_resource:
              coverage_resource["period"] = dict()
          coverage_resource["period"]["start"] = str(ins.iloc[0]['DATEIN'])+"T17:00:00+07:00"
      if ins.iloc[0]['DATEEXP'] is not None:
          if "period" not in coverage_resource:
              coverage_resource["period"] = dict()
          coverage_resource["period"]["end"] = str(ins.iloc[0]['DATEEXP'])+"T17:00:00+07:00"
      if ins.iloc[0]['SUBTYPE'] is not None and len(str(ins.iloc[0]['SUBTYPE'])) > 0: 
          coverage_resource["class"] = [
            {
              "type":{
                  "coding":[
                    {
                        "system":"http://terminology.hl7.org/CodeSystem/coverage-class",
                        "code":"subplan"
                    }
                  ]
              },
              "value": ins.iloc[0]['SUBTYPE']
            }
          ]
      bundle_coverage_structure = {
          "fullUrl": f"https://example.com/Coverage/{coverage_resource['id']}",
          "resource": coverage_resource,
          "request": {
              "method": "PUT",
              "url": f"Coverage/{coverage_resource['id']}"
              }
        }
    return bundle_coverage_structure
def create_patient_resource(pat,h_code):
    # init bundle resource
    bundle_patient_structure = dict()
    bundle_observation_occupation_structure = dict()
    if pat.shape[0] != 0:
      patient_resource = {
        "resourceType" : "Patient",
        "id": "cid-" + str(pat.iloc[0]['PERSON_ID'])
      }
      if pat.iloc[0]['NATION'] is not None and len(str(pat.iloc[0]['NATION'])) > 0:
          patient_resource["extension"] = [
              {
                "url": "http://hl7.org/fhir/StructureDefinition/patient-nationality",
                "extension": [
                  {
                    "url": "code",
                    "valueCodeableConcept": {
                      "coding": [
                        {
                          "system": "https://sil-th.org/fhir/CodeSystem/thcc-nationality-race",
                          "code": pat.iloc[0]['NATION']
                        }
                      ]
                    }
                  }
                ]
              }
            ]
      if pat.iloc[0]['PERSON_ID'] is not None and len(pat.iloc[0]['PERSON_ID']) > 0:
          if "identifier" not in patient_resource:
              patient_resource["identifier"] = []
          patient_resource["identifier"].append({
                "type": {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                      "code": "cid"
                    }
                  ]
                },
                "system": "https://terms.sil-th.org/id/th-cid",
                "value": pat.iloc[0]['PERSON_ID']
              })
      if pat.iloc[0]['HN'] is not None and len(str(pat.iloc[0]['HN'])) > 0:
          if "identifier" not in patient_resource:
              patient_resource["identifier"] = []
          patient_resource["identifier"].append({
                "type": {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                      "code": "localHn"
                    }
                  ]
                },
                "system": "https://terms.sil-th.org/hcode/5/{}/HN".format(h_code),
                "value": pat.iloc[0]['HN']
              })
      if pat.iloc[0]['NAMEPAT'] is not None and len(str(pat.iloc[0]['NAMEPAT'])) > 0:
          if "name" not in patient_resource:
              patient_resource["name"] = [dict()]
          patient_resource["name"][0]['text'] = pat.iloc[0]['NAMEPAT']
      if pat.iloc[0]['LNAME'] is not None and len(str(pat.iloc[0]['LNAME'])) > 0:
          if "name" not in patient_resource:
              patient_resource["name"] = [dict()]
          patient_resource["name"][0]['family'] = pat.iloc[0]['LNAME']
      if pat.iloc[0]['FNAME'] is not None and len(str(pat.iloc[0]['FNAME'])) > 0:
          if "name" not in patient_resource:
              patient_resource["name"] = [dict()]
          patient_resource["name"][0]['given'] = []
          patient_resource["name"][0]['given'].append(pat.iloc[0]['FNAME'])
      if pat.iloc[0]['TITLE'] is not None and len(str(pat.iloc[0]['TITLE'])) > 0:
          if "name" not in patient_resource:
              patient_resource["name"] = [dict()]
          patient_resource["name"][0]['prefix'] = []
          patient_resource["name"][0]['prefix'].append(pat.iloc[0]['TITLE'])
      if pat.iloc[0]['SEX'] is not None and len(str(pat.iloc[0]['SEX'])) > 0:
          patient_resource["gender"] = gender_dict[str(pat.iloc[0]['SEX'])]
      if pat.iloc[0]['DOB'] is not None:
          patient_resource["birthDate"] = str(pat.iloc[0]['DOB'])
      if pat.iloc[0]['AMPHUR'] is not None and pat.iloc[0]['CHANGWAT'] is not None:
          patient_resource["address"] = [
              {
                "extension": [
                  {
                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-address-address-code",
                    "valueCodeableConcept": {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-dopa-location",
                          "code": pat.iloc[0]['CHANGWAT'] + pat.iloc[0]['AMPHUR']
                        }
                      ]
                    }
                  }
                ]
              }
            ]
      if pat.iloc[0]['MARRIAGE'] is not None and len(str(pat.iloc[0]['MARRIAGE'])) > 0:
          if str(pat.iloc[0]['MARRIAGE']) == '6':
              patient_resource["maritalStatus"] = {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-thcc-marital",
                      "code": pat.iloc[0]['MARRIAGE']
                    }
                  ]
                }
          else:
              patient_resource["maritalStatus"] = {
                  "coding": [
                    {
                      "system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
                      "code": marriage_status_dict_en[str(pat.iloc[0]['MARRIAGE'])]
                    },
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-thcc-marital",
                      "code": pat.iloc[0]['MARRIAGE']
                    }
                  ],
                  "text": marriage_status_dict[str(pat.iloc[0]['MARRIAGE'])]
                }
      if pat.iloc[0]['HCODE'] is not None and len(str(pat.iloc[0]['HCODE'])) > 0:
          patient_resource["managingOrganization"] = {
              "type": "Organization",
              "identifier": {
                "system": "https://terms.sil-th.org/id/th-moph-hcode",
                "value": pat.iloc[0]['HCODE']
              }
            }
      bundle_patient_structure = {
          "fullUrl": f"https://example.com/Patient/{patient_resource['id']}",
          "resource": patient_resource,
          "request": {
              "method": "PUT",
              "url": f"Patient/{patient_resource['id']}"
              }
      }
      if pat.iloc[0]['OCCUPA'] is not None and len(str(pat.iloc[0]['OCCUPA'])) > 0:
          observation_id = f"hcode-{h_code}-code-11341-5-cid-{pat.iloc[0]['PERSON_ID']}"
          observation_resource = {
                "resourceType": "Observation",
                "id": observation_id,
                "status": "final",
                "category": [
                  {
                    "coding": [
                      {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "social-history",
                        "display": "Social History"
                      }
                    ],
                    "text": "Social History"
                  }
                ],
                "code": {
                  "coding": [
                    {
                      "system": "http://loinc.org",
                      "code": "11341-5",
                      "display": "History of Occupation"
                    }
                  ]
                },
                "subject": {
                  "reference": f"Patient/{str(patient_resource['id'])}",
                  "identifier": {
                    "type": {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                          "code": "cid"
                        }
                      ]
                    },
                    "system": "https://terms.sil-th.org/id/th-cid",
                    "value": pat.iloc[0]['PERSON_ID']
                  }
                },
                "valueCodeableConcept": {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-thcc-occupation",
                      "code": pat.iloc[0]['OCCUPA']
                    }
                  ]
                }
          }
          bundle_observation_occupation_structure = {
              "fullUrl": f"https://example.com/Observation/{observation_resource['id']}",
              "resource": observation_resource,
              "request": {
                  "method": "PUT",
                  "url": f"Observation/{observation_resource['id']}"
                  }
          }
    return bundle_patient_structure, bundle_observation_occupation_structure
def create_organization_resource(h_name,h_code):
    bundle_organization_structure = dict()
    if len(str(h_code)) > 1:
      hcode_id = f"hcode-{h_code}"
      bundle_organization_structure = {
              "fullUrl": f"Organization/{hcode_id}",
              "resource": {
                  "resourceType": "Organization",
                  "id" : f"{hcode_id}",
                  "identifier": [
                      {
                          "system": "https://bps.moph.go.th/hcode/5",
                          "value": f"{h_code}"
                      }
                  ],
                  "name": f"{h_name}"
              },
              "request": {
                  "method": "PUT",
                  "url": f"Organization/{hcode_id}"
              }
          }
    return bundle_organization_structure
def create_encounter_resource(ipd,bundle_patient_structure,h_code):
    # init bundle resource
    bundle_encounter_structure = dict()
    bundle_account_structure = dict()
    bundle_observation_bw_structure = dict()
    patient_resource = dict()
    if ipd.shape[0] != 0:
      if 'resource' in bundle_patient_structure:
          patient_resource = bundle_patient_structure['resource']
      account_id = f"hcode-{h_code}-an-{ipd.iloc[0]['AN']}"
      account_resource = {
        "resourceType": "Account",
        "id": account_id,
        "status": "active"
      }
      if ipd.iloc[0]['UUC'] is not None and len(str(ipd.iloc[0]['UUC'])) > 0:
          account_resource["extension"] = [
              {
                "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-account-coverage-use",
                "valueCodeableConcept": {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-43plus-coverage-use",
                      "code": ipd.iloc[0]['UUC'],
                      "display": uuc_dict[str(ipd.iloc[0]['UUC'])]
                    }
                  ]
                }
              }
            ]
      if ipd.iloc[0]['AN'] is not None and len(str(ipd.iloc[0]['AN'])) > 0:
          account_resource["identifier"] = [
              {
                "type": {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                      "code": "localAn"
                    }
                  ]
                },
                "system": f"https://terms.sil-th.org/hcode/5/{h_code}/AN",
                "value": ipd.iloc[0]['AN'] 
              }
            ]
      if ipd.iloc[0]['DATEADM'] is not None and ipd.iloc[0]['DATEADM'] is not None:
          service_period_in = str(ipd.iloc[0]['DATEADM']) + 'T' + str(ipd.iloc[0]['TIMEADM'])[:2] +':' + str(ipd.iloc[0]['TIMEADM'])[2:] + ':00+07:00'
          if "servicePeriod" not in account_resource:
              account_resource["servicePeriod"] = dict()
          account_resource["servicePeriod"]["start"] = service_period_in
      if ipd.iloc[0]['DATEDSC'] is not None and ipd.iloc[0]['DATEDSC'] is not None:
          service_period_out = str(ipd.iloc[0]['DATEDSC']) + 'T' + str(ipd.iloc[0]['TIMEDSC'])[:2] +':' + str(ipd.iloc[0]['TIMEDSC'])[2:] + ':00+07:00'
          if "servicePeriod" not in account_resource:
              account_resource["servicePeriod"] = dict()
          account_resource["servicePeriod"]["end"] = service_period_out
      encounter_resource = {
          "resourceType" : "Encounter",
          "id": f"hcode-{h_code}-an-{ipd.iloc[0]['AN']}",
          "status" : "finished"
      }
      if ipd.iloc[0]['AN'] is not None and len(str(ipd.iloc[0]['AN'])) > 0:
          encounter_resource["identifier"] = [
              {
                "type": {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                      "code": "localAn"
                    }
                  ]
                },
                "system": "https://terms.sil-th.org/hcode/5/{}/AN".format(h_code),
                "value": ipd.iloc[0]['AN']
              }
          ]
      if ipd.iloc[0]['SVCTYPE'] is not None and len(str(ipd.iloc[0]['SVCTYPE'])) > 0:
          encounter_resource["class"] = {
              "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
              "code": ipd.iloc[0]['SVCTYPE'] 
          }
      else:
          encounter_resource["class"] = {
              "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
              "code": "IMP"
          }
      if ipd.iloc[0]['DATEDSC'] is not None:
          encounter_resource["period"] = dict()
          encounter_resource["period"]["end"] = str(ipd.iloc[0]['DATEDSC'])+"T17:00:00+07:00"
      if ipd.iloc[0]['DISCHS'] is not None and len(str(ipd.iloc[0]['DISCHS'])) > 0:
          if "hospitalization" not in encounter_resource:
              encounter_resource["hospitalization"] = dict()
              encounter_resource["hospitalization"]["extension"] = []
          encounter_resource["hospitalization"]["extension"].append({
              "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-encounter-ipd-discharge-status",
              "valueCodeableConcept": 
                {
                "coding": [
                  {
                    "system": "https://terms.sil-th.org/CodeSystem/cs-thcc-ipd-discharge-status",
                    "code": ipd.iloc[0]['DISCHS'],
                    "display": dischs_dict[str(ipd.iloc[0]['DISCHS'])]
                  }
                ]
              }
            })
      if ipd.iloc[0]['DISCHT'] is not None and len(str(ipd.iloc[0]['DISCHT'])) > 0 and str(ipd.iloc[0]['DISCHT']) in discht_dict:
          if "hospitalization" not in encounter_resource:
              encounter_resource["hospitalization"] = dict()
              encounter_resource["hospitalization"]["extension"] = []
          encounter_resource["hospitalization"]["extension"].append({
              "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-encounter-ipd-discharge-type",
              "valueCodeableConcept": {
                "coding": [
                  {
                    "system": "https://terms.sil-th.org/CodeSystem/cs-thcc-ipd-discharge-type",
                    "code": ipd.iloc[0]['DISCHT'],
                    "display": discht_dict[str(ipd.iloc[0]['DISCHT'])]
                  }
                ]
              }
            })
      if ipd.iloc[0]['DEPT'] is not None and len(str(ipd.iloc[0]['DEPT'])) > 0:
          if "location" not in encounter_resource:
              encounter_resource["location"] = []
          encounter_resource["location"].append({
                "location": {
                  "identifier": {
                    "type": {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                          "code": "localDep",
                          "display": "รหัสประจำแผนก ของหน่วยบริการ"
                        }
                      ]
                    },
                    "system": "https://terms.sil-th.org/hcode/5/{}/DepCode".format(h_code),
                    "value": ipd.iloc[0]['DEPT']
                  }
                }
              })
      if ipd.iloc[0]['WARDDSC'] is not None and len(str(ipd.iloc[0]['WARDDSC'])) > 0:
          if "location" not in encounter_resource:
              encounter_resource["location"] = []
          encounter_resource["location"].append({
                "extension": [
                  {
                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-encounter-ipd-journey",
                    "valueCodeableConcept": {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-meta-ipd-journey",
                          "code": "discharge",
                          "display": "รหัสแผนกที่จำหน่ายผู้ป่วย (discharge clinic)"
                        }
                      ]
                    }
                  }
                ],
                "location": {
                  "identifier": {
                    "type": {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                          "code": "localDep",
                          "display": "รหัสประจำแผนก ของหน่วยบริการ"
                        }
                      ]
                    },
                    "system": "https://terms.sil-th.org/hcode/5/{}/DepCode".format(h_code),
                    "value": ipd.iloc[0]['WARDDSC']
                  }
                }
              })
      observation_bw_id = f"hcode-{h_code}-code-29463-7-{patient_resource['id']}"
      observation_bw_resource = {
          "resourceType": "Observation",
          "id": observation_bw_id,
          "status": "final",
          "category": [
              {
                "coding": [
                  {
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "vital-signs",
                    "display": "Vital Signs"
                  }
                ],
                "text": "Vital Signs"
              }
          ],
          "code": {
              "coding": [
                {
                  "system": "http://loinc.org",
                  "code": "29463-7",
                  "display": "Body Weight"
                },
                {
                  "system": "http://snomed.info/sct",
                  "code": "27113001",
                  "display": "Body weight"
                }
              ],
              "text": "น้ำหนักแรกรับ"
          }
      }
      if ipd.iloc[0]['DATEADM'] is not None:
          observation_bw_resource["effectiveDateTime"] = str(ipd.iloc[0]['DATEADM'])+"T17:00:00+07:00"
      if ipd.iloc[0]['ADM_W'] is not None:
          observation_bw_resource["valueQuantity"] = {
                  "value": float(ipd.iloc[0]['ADM_W']),
                  "unit": "กิโลกรัม",
                  "system": "http://unitsofmeasure.org",
                  "code": "kg"
              }
      if len(patient_resource) > 0:
          account_resource["subject"] =[
              {
                "reference": f"Patient/{str(patient_resource['id'])}"
              }
            ]
          encounter_resource["subject"] = {
                "reference": f"Patient/{str(patient_resource['id'])}"
              }
          observation_bw_resource["subject"] = {
              "reference": f"Patient/{str(patient_resource['id'])}"
          }
      if len(account_resource) > 0:
          encounter_resource["account"] = [
              {
                "reference": f"Account/{account_resource['id']}"
              }
          ]
      if len(encounter_resource) > 0:   
          observation_bw_resource["encounter"] = {
              "reference": f"Encounter/{str(encounter_resource['id'])}"
          }
      bundle_account_structure = {
          "fullUrl": f"https://example.com/Account/{account_resource['id']}",
          "resource": account_resource,
          "request": {
              "method": "PUT",
              "url": f"Account/{account_resource['id']}"
              }
      }
      bundle_encounter_structure = {
          "fullUrl": f"https://example.com/Encounter/{encounter_resource['id']}",
          "resource": encounter_resource,
          "request": {
              "method": "PUT",
              "url": f"Encounter/{encounter_resource['id']}"
              }
      }
      bundle_observation_bw_structure = {
          "fullUrl": f"https://example.com/Observation/{observation_bw_resource['id']}",
          "resource": observation_bw_resource,
          "request": {
              "method": "PUT",
              "url": f"Observation/{observation_bw_resource['id']}"
              }
      }
    return bundle_encounter_structure,bundle_account_structure,bundle_observation_bw_structure
def create_service_request_resource(irf,patient_bundle,encounter_bundle,h_code):
    bundle_service_req_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if irf.shape[0] != 0:
      if irf[irf['REFERTYPE']=='2'].shape[0] > 0:
          service_id = encounter_resource['id']
          refer_out = irf[irf['REFERTYPE']=='2']
          service_req_resource = {
              "resourceType" : "ServiceRequest",
              "id": service_id,
              "status": "completed",
              "intent": "order",
              "code": {
                  "coding": [
                    {
                      "system": "http://snomed.info/sct",
                      "code": "3457005",
                      "display": "Patient referral"
                    }
                  ]
                }
          }
          if len(patient_resource) > 0:
              service_req_resource["subject"] = {
                    "reference": f"Patient/{str(patient_resource['id'])}"
                  }
          if len(encounter_resource) > 0:
              service_req_resource["encounter"] = {
                    "reference" : f"Encounter/{str(encounter_resource['id'])}"
                  }
          if refer_out.iloc[0]['REFER'] is not None and len(refer_out.iloc[0]['REFER']) > 0:
              service_req_resource['performer'] = [
                      {
                        "type": "Organization",
                        "identifier": {
                          "system": "https://terms.sil-th.org/id/th-moph-hcode",
                          "value": refer_out.iloc[0]['REFER'] 
                        }
                      }
                  ]
          bundle_service_req_structure = {
              "fullUrl": f"https://example.com/ServiceRequest/{service_req_resource['id']}",
              "resource": service_req_resource,
              "request": {
                  "method": "PUT",
                  "url": f"ServiceRequest/{service_req_resource['id']}"
                  }
          }
    return bundle_service_req_structure
def create_condition_resource(idx,aer,patient_bundle,encounter_bundle):
    bundle_condition_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    bundle_condition_structure_arr = []
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if idx.shape[0] != 0:
      for i in range(idx.shape[0]):
        condition_id = f"{encounter_resource['id']}-code-idx-{idx['DIAG'][i]}"
        condition_resource = {
            "resourceType" : "Condition",
            "id": condition_id,
            "clinicalStatus": {
                "coding": [
                  {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                    "code": "active"
                  }
                ]
            },
            "category": [
                {
                  "coding": [
                    {
                      "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                      "code": "encounter-diagnosis",
                      "display": "Encounter Diagnosis"
                    }
                  ]
                }
            ]
        }
        if idx['DIAG'][i] is not None and len(idx['DIAG'][i]) > 0:
            condition_resource['code'] = {
                "coding" : [{
                    "system" : "http://hl7.org/fhir/sid/icd-10",
                    "code" : idx['DIAG'][i] 
                  }]
              }
        if len(patient_resource) > 0:
            condition_resource['subject'] = {
                    "reference": f"Patient/{str(patient_resource['id'])}"
                }
        if len(encounter_resource) > 0:
            condition_resource['encounter'] = {
                    "reference": f"Encounter/{str(encounter_resource['id'])}"
                }
        if idx['DRDX'][i] is not None and len(idx['DRDX'][i]) > 0:
            condition_resource['asserter'] = {
                    "type": "Practitioner",
                    "identifier": {
                      "system": "https://terms.sil-th.org/id/th-doctor-id",
                      "value": idx['DRDX'][i] 
                    }
                }
        if aer.shape[0] != 0:
            if idx['DXTYPE'][i] == 1:
                condition_resource['onsetDateTime'] = str(aer.iloc[0]['AEDATE']) + 'T' + str(aer.iloc[0]['AETIME'][:2]) +':' + str(lvd.iloc[0]['AETIME'][2:]) + 'Z'
        bundle_condition_structure = {
            "fullUrl": f"https://example.com/Condition/{condition_resource['id']}",
            "resource": condition_resource,
            "request": {
                "method": "PUT",
                "url": f"Condition/{condition_resource['id']}"
                }
        }
        bundle_condition_structure_arr.append(bundle_condition_structure)
    return bundle_condition_structure_arr
def create_procedure_resource(iop,patient_bundle,encounter_bundle):
    bundle_procedure_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    bundle_procedure_structure_arr = []
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if iop.shape[0] != 0:
      for i in range(iop.shape[0]):
        procedure_id = f"{encounter_resource['id']}-code-{iop['OPER'][i]}"
        procedure_resource = {
            "resourceType" : "Procedure",
            "id": procedure_id,
            "status": "completed"
        }
        if iop['OPTYPE'][i] is not None and len(iop['OPTYPE'][i]) > 0:
            procedure_resource['extension'] = [
                    {
                      "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-procedure-procedure-type",
                      "valueCodeableConcept": {
                        "coding": [
                          {
                            "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-procedure-type",
                            "code": iop['OPTYPE'][i],
                            "display": procedure_type_dict[iop['OPTYPE'][i]]
                          }
                        ]
                      }
                    }
                ]
        if iop['OPER'][i] is not None and len(iop['OPER'][i]) > 0:
            procedure_resource['code'] = {
                    "coding": [
                      {
                        "system": "http://hl7.org/fhir/sid/icd-9-cm",
                        "code": iop['OPER'][i]
                      }
                    ]
                }
        if len(patient_resource) > 0:
            procedure_resource['subject'] = {
                    "reference": f"Patient/{str(patient_resource['id'])}"
                }
        if len(encounter_resource) > 0:
            procedure_resource['encounter'] = {
                    "reference": f"Encounter/{str(encounter_resource['id'])}"
                }
        if iop['DATEIN'][i] is not None:
            if "performedPeriod" not in procedure_resource:
                procedure_resource["performedPeriod"] = dict()
            procedure_resource["performedPeriod"]["start"] = str(iop['DATEIN'][i])+"T17:00:00+07:00" 
        if iop['DATEOUT'][i] is not None:
            if "performedPeriod" not in procedure_resource:
                procedure_resource["performedPeriod"] = dict()
            procedure_resource["performedPeriod"]["end"] = str(iop['DATEOUT'][i])+"T17:00:00+07:00" 
        if iop['DROPID'][i] is not None and len(iop['DROPID'][i]) > 0:
            procedure_resource["performer"] = [
                    {
                      "actor": {
                        "type": "Practitioner",
                        "identifier": {
                          "system": "https://terms.sil-th.org/id/th-doctor-id",
                          "value": iop['DROPID'][i]
                        }
                      }
                    }
                ]
        bundle_procedure_structure = {
            "fullUrl": f"https://example.com/Procedure/{procedure_resource['id']}",
            "resource": procedure_resource,
            "request": {
                "method": "PUT",
                "url": f"Procedure/{procedure_resource['id']}"
                }
        }
        bundle_procedure_structure_arr.append(bundle_procedure_structure)
    return bundle_procedure_structure_arr
def create_medication_dispense_resource(dru,patient_bundle,encounter_bundle):
    encounter_resource = dict()
    patient_resource = dict()
    bundle_med_disp_structure = dict()
    bundle_med_disp_structure_arr = []
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if dru.shape[0] != 0:
        for i in range(dru.shape[0]):
          med_dispense_id = f"{encounter_resource['id']}-code-{dru['DID'][i]}"
          med_dispense_resource = {
              "resourceType" : "MedicationDispense",
              "id" : med_dispense_id,
              "status": "completed"
          }
          if dru['DID'][i] is not None and len(dru['DID'][i]) > 0:
              if "medicationCodeableConcept" not in med_dispense_resource:
                  med_dispense_resource["medicationCodeableConcept"] = dict()
                  med_dispense_resource["medicationCodeableConcept"]["coding"] = []
              med_dispense_resource["medicationCodeableConcept"]["coding"].append({
                  "system": "https://terms.sil-th.org/CodeSystem/cs-th-local-drug-code",
                  "code": dru['DID'][i] 
                })
          if dru['DIDSTD'][i] is not None and len(dru['DIDSTD'][i]) > 0:
              if "medicationCodeableConcept" not in med_dispense_resource:
                  med_dispense_resource["medicationCodeableConcept"] = dict()
                  med_dispense_resource["medicationCodeableConcept"]["coding"] = []
              med_dispense_resource["medicationCodeableConcept"]["coding"].append({
                  "system": "https://terms.sil-th.org/CodeSystem/cs-th-24drug",
                  "code": dru['DIDSTD'][i] 
                })
          if dru['DIDNAME'][i] is not None and len(dru['DIDNAME'][i]) > 0:
              if "medicationCodeableConcept" not in med_dispense_resource:
                  med_dispense_resource["medicationCodeableConcept"] = dict()
              med_dispense_resource["medicationCodeableConcept"]["text"] = dru['DIDNAME'][i]
          if dru['PERSON_ID'][i] is not None and len(dru['PERSON_ID'][i]) > 0:
              med_dispense_resource['subject'] = {
                      "reference": f"Patient/{str(patient_resource['id'])}",
                      "identifier": {
                        "system": "https://terms.sil-th.org/id/th-cid",
                        "value": str(int(dru['PERSON_ID'][i])) 
                      }
                  }
          if len(encounter_resource) > 0:
              med_dispense_resource['context'] = {
                      "reference": f"Encounter/{str(encounter_resource['id'])}"
                  }
          if dru['PROVIDER'][i] is not None and len(dru['PROVIDER'][i]) > 0:
              med_dispense_resource["performer"] = [
                      {
                        "function": {
                          "coding": [
                            {
                              "system": "http://terminology.hl7.org/CodeSystem/medicationdispense-performer-function",
                              "code": "finalchecker"
                            }
                          ]
                        },
                        "actor": {
                          "type": "Practitioner",
                          "identifier": {
                            "system": "https://terms.sil-th.org/id/th-pharmacist-id",
                            "value": dru['PROVIDER'][i] 
                          }
                        }
                      }
                  ]
          if dru['AMOUNT'][i] is not None and len(dru['AMOUNT'][i]) > 0:
              if "quantity" not in med_dispense_resource:
                  med_dispense_resource["quantity"] = dict()
              med_dispense_resource["quantity"]["value"] = float(dru['AMOUNT'][i])
          if dru['UNIT'][i] is not None and len(dru['UNIT'][i]) > 0:
              if "quantity" not in med_dispense_resource:
                  med_dispense_resource["quantity"] = dict()
              med_dispense_resource["quantity"]["unit"] = dru['UNIT'][i] 
          if dru['DATE_SERV'][i] is not None:
              med_dispense_resource["whenHandedOver"] = str(dru['DATE_SERV'][i])+"T17:00:00+07:00" 
          if dru['SIGTEXT'][i] is not None and len(dru['SIGTEXT'][i]) > 0:
              med_dispense_resource["dosageInstruction"] =[
                      {
                        "extension": [
                          {
                            "url": "https://sil-th.org/fhir/StructureDefinition/dosage-sigcode",
                            "valueCodeableConcept": {
                              "coding": [
                                {
                                  "system": "https://sil-th.org/fhir/CodeSystem/sig-code",
                                  "code": "O1TA2P"
                                }
                              ]
                            }
                          }
                        ],
                        "text": dru['SIGTEXT'][i]
                      }
                  ]
          bundle_med_disp_structure = {
              "fullUrl": f"https://example.com/MedicationDispense/{med_dispense_resource['id']}",
              "resource": med_dispense_resource,
              "request": {
                  "method": "PUT",
                  "url": f"MedicationDispense/{med_dispense_resource['id']}"
                  }
          }
          bundle_med_disp_structure_arr.append(bundle_med_disp_structure)
    return bundle_med_disp_structure_arr
def create_medication_request_resource(dru,patient_bundle,encounter_bundle):
    encounter_resource = dict()
    patient_resource = dict()
    bundle_med_req_structure = dict()
    bundle_med_req_structure_arr = []
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if dru.shape[0] != 0:
        for i in range(dru.shape[0]):
          med_request_id = f"{encounter_resource['id']}-code-{dru['DID'][i]}"
          med_request_resource = {
            "resourceType": "MedicationRequest",
            "id": med_request_id,
            "status": "completed",
            "intent": "order"
          }
          if dru['DRUGREMARK'][i] is not None and len(dru['DRUGREMARK'][i]) > 0:
              if "extension" not in med_request_resource:
                  med_request_resource["extension"] = []
              med_request_resource["extension"].append({
                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-medicationrequest-ned-criteria",
                    "valueCodeableConcept": {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-medication-ned-criteria",
                          "code": dru['DRUGREMARK'][i],
                          "display": drug_mark_dict[dru['DRUGREMARK'][i]]
                        }
                      ]
                    }
                  })
          if dru['PA_NO'][i] is not None and len(dru['PA_NO'][i]) > 0:
              if "extension" not in med_request_resource:
                  med_request_resource["extension"] = []
              med_request_resource["extension"].append({
                    "url" : "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-medicationrequest-med-approved-no",
                    "valueString" : dru['PA_NO'][i] 
                  })
          if dru['USE_STATUS'][i] is not None and len(dru['USE_STATUS'][i]) > 0:
              med_request_resource["category"] = [
                  {
                    "coding": [
                      {
                        "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-medication-category",
                        "code": dru['USE_STATUS'][i],
                        "display": drug_use_status_dict[dru['USE_STATUS'][i]]
                      }
                    ]
                  }
                ]
          if dru['DID'][i] is not None and len(dru['DID'][i]) > 0:
              if "medicationCodeableConcept" not in med_request_resource:
                  med_request_resource["medicationCodeableConcept"] = dict()
                  med_request_resource["medicationCodeableConcept"]["coding"] = []
              med_request_resource["medicationCodeableConcept"]["coding"].append({
                  "system": "https://terms.sil-th.org/CodeSystem/cs-th-local-drug-code",
                  "code": dru['DID'][i] 
                })
          if dru['DIDSTD'][i] is not None and len(dru['DIDSTD'][i]) > 0:
              if "medicationCodeableConcept" not in med_request_resource:
                  med_request_resource["medicationCodeableConcept"] = dict()
                  med_request_resource["medicationCodeableConcept"]["coding"] = []
              med_request_resource["medicationCodeableConcept"]["coding"].append({
                  "system": "https://terms.sil-th.org/CodeSystem/cs-th-24drug",
                  "code": dru['DIDSTD'][i] 
                })
          if dru['DIDNAME'][i] is not None and len(dru['DIDNAME'][i]) > 0:
              if "medicationCodeableConcept" not in med_request_resource:
                  med_request_resource["medicationCodeableConcept"] = dict()
              med_request_resource["medicationCodeableConcept"]["text"] = dru['DIDNAME'][i]
          if dru['PERSON_ID'][i] is not None and len(dru['PERSON_ID'][i]) > 0:
              med_request_resource['subject'] = {
                      "reference": f"Patient/{str(patient_resource['id'])}",
                      "identifier": {
                        "system": "https://terms.sil-th.org/id/th-cid",
                        "value": dru['PERSON_ID'][i]
                      }
                  }
          if len(encounter_resource) > 0:
              med_request_resource['encounter'] = {
                      "reference": f"Encounter/{str(encounter_resource['id'])}"
                  }
          if dru['DATE_SERV'][i] is not None:
              med_request_resource['authoredOn'] = str(dru['DATE_SERV'][i])+"T17:00:00+07:00"
          if dru['SIGCODE'][i] is not None and len(dru['SIGCODE'][i]) > 0:
              if "dosageInstruction" not in med_request_resource:
                  med_request_resource["dosageInstruction"] = [dict()]
              med_request_resource["dosageInstruction"][0]["extension"] = [
                  {
                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-medicationrequest-med-dosage-code",
                    "valueString":  dru['SIGCODE'][i] 
                  }
                ]
          if dru['SIGTEXT'][i] is not None and len(dru['SIGTEXT'][i]) > 0:
              if "dosageInstruction" not in med_request_resource:
                  med_request_resource["dosageInstruction"] = [dict()]
              med_request_resource["dosageInstruction"][0]["text"] = dru['SIGTEXT'][i] 
          bundle_med_req_structure = {
              "fullUrl": f"https://example.com/MedicationRequest/{med_request_resource['id']}",
              "resource": med_request_resource,
              "request": {
                  "method": "PUT",
                  "url": f"MedicationRequest/{med_request_resource['id']}"
                  }
          }
          bundle_med_req_structure_arr.append(bundle_med_req_structure)
    return bundle_med_req_structure_arr
def create_claim_item(cha,adp,dru,encounter_bundle):
    encounter_resource = dict()
    claim_items = []
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if cha.shape[0] != 0:
        for i in range(cha.shape[0]):
          if cha['CHRGITEM'][i] is not None and len(cha['CHRGITEM'][i]) > 0:
              claim_item = dict()
              claim_item["sequence"] = i+1
              claim_item["productOrService"] = {
                    "coding" : [
                        {
                          "system" : "https://terms.sil-th.org/CodeSystem/cs-eclaim-charge-item",
                          "code" : cha['CHRGITEM'][i],
                          "display": cha_type_dict[cha['CHRGITEM'][i]]
                        }
                    ]
                  }
              if cha['DATE'][i] is not None:
                  claim_item["servicedDate"] = str(cha['DATE'][i]) 
              if len(encounter_resource) > 0:
                  claim_item["encounter"] = [
                          {
                            "reference": f"Encounter/{encounter_resource['id']}"
                          }
                      ]
              if cha['AMOUNT'][i] is not None:
                  claim_item["net"] = {
                        "value" : float(cha['AMOUNT'][i]),
                        "currency" : "THB"
                      }
              if adp.shape[0] != 0:    
                  if claim_item['productOrService']['coding'][0]['code'] == 'B1':
                      claim_item['detail'] = []
                      for j in range(adp.shape[0]):
                          item = dict()
                          item["sequence"]= j+1
                          if adp['TOTCOPAY'][j] is not None:
                              if "extension" not in item:
                                  item["extension"] = []
                              item["extension"].append({
                                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-claim-item-copay",
                                    "valueMoney": {
                                      "value": float(adp['TOTCOPAY'][j]),
                                      "currency": "THB"
                                    }
                                  })
                          if adp['DATEOPD'][j] is not None:
                              if "extension" not in item:
                                  item["extension"] = []
                              item["extension"].append({
                                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition-ex-claim-item-detail-datetime.html",
                                    "valueDateTime": str(adp['DATEOPD'][j])+"T17:00:00+07:00" 
                                  })
                          if adp['TYPE'][j] is not None and len(adp['TYPE'][j]) > 0:
                              if "productOrService" not in item:
                                  item["productOrService"] = dict()
                                  item["productOrService"]["coding"] = []
                              item["productOrService"]["coding"].append({
                                  "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-adp-type",
                                  "code": adp['TYPE'][j],
                                  "display": adp_type_dict[adp['TYPE'][j]]
                                })
                          if adp['CODE'][j] is not None and len(adp['CODE'][j]) > 0:
                              if "productOrService" not in item:
                                  item["productOrService"] = dict()
                                  item["productOrService"]["coding"] = []
                              item["productOrService"]["coding"].append({
                                  "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-service-fee",
                                  "code": adp['CODE'][j]
                                })
                          if adp['TMLTCODE'][j] is not None and len(adp['TMLTCODE'][j]) > 0:
                              if "productOrService" not in item:
                                  item["productOrService"] = dict()
                                  item["productOrService"]["coding"] = []
                              item["productOrService"]["coding"].append({
                                  "system": "https://terms.sil-th.org/CodeSystem/cs-th-tmlt",
                                  "code": adp['TMLTCODE'][j] 
                                })
                          if adp['QTY'][j] is not None:
                              item["quantity"] = {
                                      "value": float(adp['QTY'][j]) 
                                  }
                          if adp['RATE'][j] is not None:
                              item["unitPrice"] = {
                                      "value": float(adp['RATE'][j]),
                                      "currency": "THB"
                                  }
                          if adp['TOTAL'][j] is not None:
                              item["net"] = {
                                      "value": float(adp['TOTAL'][j]),
                                      "currency": "THB"
                                  }
                          item["udi"] = [
                                  {
                                    "display": adp['SERIALNO'][j] if adp['SERIALNO'][j] is not None else "SerialNo"
                                  }
                              ]
                          claim_item['detail'].append(item)
              if dru.shape[0] != 0:    
                  if claim_item['productOrService']['coding'][0]['code'] == '31':
                      claim_item['detail'] = []
                      for j in range(dru.shape[0]):
                          item = dict()
                          item["sequence"] = j+1
                          if dru['DRUGCOST'][j] is not None:
                              if "extension" not in item:
                                  item["extension"] = []
                              item["extension"].append({
                                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-claim-item-cost",
                                    "valueMoney": {
                                      "value": float(dru['DRUGCOST'][j]),
                                      "currency": "THB"
                                      }
                                  })
                          if dru['TOTCOPAY'][j] is not None:
                              if "extension" not in item:
                                  item["extension"] = []
                              item["extension"].append({
                                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-claim-item-copay",
                                    "valueMoney": {
                                      "value": float(dru['TOTCOPAY'][j]),
                                      "currency": "THB"
                                      }
                                  })
                          if dru['DATE_SERV'][j] is not None:
                              if "extension" not in item:
                                  item["extension"] = []
                              item["extension"].append({
                                    "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition-ex-claim-item-detail-datetime.html",
                                    "valueDateTime": str(dru['DATE_SERV'][j])+"T17:00:00+07:00"
                                  })
                          if dru['DID'][j] is not None and len(dru['DID'][j]) > 0:
                              if "productOrService" not in item:
                                  item["productOrService"] = dict()
                                  item["productOrService"]["coding"] = []
                              item["productOrService"]["coding"].append({
                                  "system": "https://terms.sil-th.org/CodeSystem/cs-th-local-drug-code",
                                  "code": dru['DID'][j] 
                                })
                          if dru['DIDSTD'][j] is not None and len(dru['DIDSTD'][j]) > 0:
                              if "productOrService" not in item:
                                  item["productOrService"] = dict()
                                  item["productOrService"]["coding"] = []
                              item["productOrService"]["coding"].append({
                                  "system": "https://terms.sil-th.org/CodeSystem/cs-th-24drug",
                                  "code": dru['DIDSTD'][j] 
                                })
                          if dru['DIDNAME'][j] is not None and len(dru['DIDNAME'][j]) > 0:
                              if "productOrService" not in item:
                                  item["productOrService"] = dict()
                              item["productOrService"]["text"] = dru['DIDNAME'][j] 
                          if dru['AMOUNT'][j] is not None and len(dru['AMOUNT'][j]) > 0:
                              item["quantity"] = {
                                      "value": float(dru['AMOUNT'][j])
                                  }
                          if dru['DRUGPRIC'][j] is not None:
                              item["unitPrice"] = {
                                      "value": float(dru['DRUGPRIC'][j]), 
                                      "currency": "THB"
                                  }
                          if dru['TOTAL'][j] is not None:
                              item["net"] = {
                                      "value": float(dru['TOTAL'][j]),
                                      "currency": "THB"
                                  }
                          claim_item['detail'].append(item)
          claim_items.append(claim_item)                  
    return claim_items
def create_claim_resource(cha,claim_items,patient_bundle,encounter_bundle,h_code):
    bundle_claim_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if len(claim_items) > 0:
      claim_id = f"{encounter_resource['id']}"
      # type status use provider priority patient created   insurance 
      claim_resource = {
        "resourceType": "Claim",
        "id": claim_id,
        "status": "active",
        "item" : claim_items
      }
      claim_resource['type'] = {
          "coding": [
            {
              "system": "http://terminology.hl7.org/CodeSystem/claim-type",
              "code": "institutional"
            }
          ]
        }
      claim_resource['use'] = "claim"
      claim_resource['provider'] = {
          "reference": f"Organization/hcode-{h_code}"
        }
      claim_resource['priority'] = {
          "coding": [
            {
              "system": "http://terminology.hl7.org/CodeSystem/processpriority",
              "code": "normal"
            }
          ]
        }
      if len(patient_resource) > 0:
        if cha.iloc[0]['PERSON_ID'] is not None and len(cha.iloc[0]['PERSON_ID']) > 0:
            claim_resource['patient'] = {
                "reference": f"Patient/{str(patient_resource['id'])}",
                "identifier": {
                  "system": "https://terms.sil-th.org/id/th-cid",
                  "value": cha.iloc[0]['PERSON_ID']
                }
              }
      if cha.iloc[0]['DATE'] is not None:
          claim_resource['created'] = str(cha.iloc[0]['DATE'])+"T17:00:00+07:00" 
      if cha.iloc[0]['PERSON_ID'] is not None and len(cha.iloc[0]['PERSON_ID']) > 0:
          claim_resource['insurance'] = [
              {
                "sequence": 1,
                "focal": True,
                "coverage": {
                  "reference": f"Coverage/hcode-{h_code}-uc-cid-{str(cha.iloc[0]['PERSON_ID'])}"
                }
              }
            ]
      bundle_claim_structure = {
          "fullUrl": f"https://example.com/Claim/{claim_resource['id']}",
          "resource": claim_resource,
          "request": {
              "method": "PUT",
              "url": f"Claim/{claim_resource['id']}"
              }
      }
    return bundle_claim_structure
def create_condition_resource_by_adp(adp,patient_bundle,encounter_bundle,i):
    bundle_condition_adp_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    condition_id = f"{encounter_resource['id']}-code-adp-{adp['CODE'][i]}"
    condition_adp_resource = {
      "resourceType": "Condition",
      "id": condition_id,
      "clinicalStatus": {
        "coding": [
          {
            "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
            "code": "active"
          }
        ]
      },
      "category": [
        {
          "coding": [
            {
              "system": "http://terminology.hl7.org/CodeSystem/condition-category",
              "code": "encounter-diagnosis",
              "display": "Encounter Diagnosis"
            }
          ]
        }
      ]
    }
    if adp['CAGCODE'][i] is not None and len(adp['CAGCODE'][i]) > 0:
        condition_adp_resource["code"] = {
            "coding": [
              {
                "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-cancer-type",
                "code": adp['CAGCODE'][i],
                "display": cancer_type_dict[adp['CAGCODE'][i]]
              }
            ]
          }
    if len(patient_resource) > 0:
        condition_adp_resource["subject"] = {
            "reference": f"Patient/{str(patient_resource['id'])}"
        }
    if len(encounter_resource) > 0:   
        condition_adp_resource["encounter"] = {
            "reference": f"Encounter/{str(encounter_resource['id'])}"
        }
    if adp['DATEOPD'][i] is not None:
        condition_adp_resource["recordedDate"] = str(adp['DATEOPD'][i])+"T17:00:00+07:00" 
    if len(condition_adp_resource) > 0:
      bundle_condition_adp_structure = {
          "fullUrl": f"https://example.com/Condition/{condition_adp_resource['id']}",
          "resource": condition_adp_resource,
          "request": {
              "method": "PUT",
              "url": f"Condition/{condition_adp_resource['id']}"
              }
      }
    return bundle_condition_adp_structure
def create_observation_bt_resource(adp,patient_bundle,encounter_bundle,i,h_code):
    bundle_observation_bt_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if adp['BI'][i] is not None and len(adp['BI'][i]) > 1:
        observation_barthel_id = f"hcode-{h_code}-code-96761-2-{patient_resource['id']}"
        observation_barthel_resource = {
          "resourceType": "Observation",
          "id": observation_barthel_id,
          "status": "final",
          "category": [
            {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                  "code": "survey",
                  "display": "Survey"
                }
              ],
              "text": "Survey"
            }
          ],
          "code": {
            "coding": [
              {
                "system": "http://loinc.org",
                "code": "96761-2",
                "display": "Total score Barthel Index"
              },
              {
                "system": "http://snomed.info/sct",
                "code": "273302005",
                "display": "Barthel index"
              }
            ],
            "text": "Barthel ADL Index"
          }
        }
        if len(patient_resource) > 0:
            observation_barthel_resource["subject"] = {
                "reference": f"Patient/{str(patient_resource['id'])}"
            }
        if len(encounter_resource) > 0:   
            observation_barthel_resource["encounter"] = {
                "reference": f"Encounter/{str(encounter_resource['id'])}"
            }
        if adp['DATEOPD'][i] is not None:
            observation_barthel_resource["effectiveDateTime"] = str(adp['DATEOPD'][i])+"T17:00:00+07:00" 
        if adp['BI'][i] is not None:
            observation_barthel_resource["valueQuantity"] = {
                "value": int(adp['BI'][i]) ,
                "unit": "{score}",
                "system": "http://unitsofmeasure.org",
                "code": "{score}"
              }
        bundle_observation_bt_structure = {
            "fullUrl": f"https://example.com/Observation/{observation_barthel_resource['id']}",
            "resource": observation_barthel_resource,
            "request": {
                "method": "PUT",
                "url": f"Observation/{observation_barthel_resource['id']}"
                }
        } 
    return bundle_observation_bt_structure
def create_observation_covid_resource(adp,patient_bundle,encounter_bundle,i,h_code):
    bundle_observation_covid_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if adp['STATUS1'][i] is not None and len(adp['STATUS1'][i]) > 1:
        observation_covid_id = f"hcode-{h_code}-code-871562009-{patient_resource['id']}"
        observation_covid_resource = {
          "resourceType": "Observation",
          "id": observation_covid_id,
          "status": "final",
          "category": [
            {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                  "code": "laboratory",
                  "display": "Laboratory"
                }
              ],
              "text": "Laboratory"
            }
          ],
          "code": {
            "coding": [
              {
                "system": "http://snomed.info/sct",
                "code": "871562009",
                "display": "Detection of COVID-19"
              }
            ],
            "text": "ผลการตรวจ LAB COVID"
          }
        }
        if len(patient_resource) > 0:
            observation_covid_resource["subject"] = {
                "reference": f"Patient/{str(patient_resource['id'])}"
            }
        if len(encounter_resource) > 0:   
            observation_covid_resource["encounter"] = {
                "reference": f"Encounter/{str(encounter_resource['id'])}"
            }
        if adp['DATEOPD'][i] is not None:
            observation_covid_resource["effectiveDateTime"] = str(adp['DATEOPD'][i])+"T17:00:00+07:00" 
        if adp['STATUS1'][i] is not None:
            observation_covid_resource["valueCodeableConcept"] = {
                "coding": [
                  {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                    "code": adp['STATUS1'][i], 
                    "display": "Negative"
                  }
                ]
              }
        bundle_observation_covid_structure = {
            "fullUrl": f"https://example.com/Observation/{observation_covid_resource['id']}",
            "resource": bundle_observation_bt_structure,
            "request": {
                "method": "PUT",
                "url": f"Observation/{observation_covid_resource['id']}"
                }
        }
    return bundle_observation_covid_structure
def create_observation_dcip_resource(adp,patient_bundle,encounter_bundle,i,h_code):
    bundle_observation_dcip_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if adp['DCIP'][i] is not None and len(adp['DCIP'][i]) > 1:
        observation_dcip_id = f"hcode-{h_code}-code-300058-{patient_resource['id']}"
        observation_dcip_resource = {
          "resourceType": "Observation",
          "id": observation_dcip_id,
          "status": "final",
          "category": [
            {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                  "code": "laboratory",
                  "display": "Laboratory"
                }
              ],
              "text": "Laboratory"
            }
          ],
          "code": {
            "coding": [
              {
                "system": "https://terms.sil-th.org/CodeSystem/cs-th-tmlt",
                "code": "300058",
                "display": "Hemoglobin E [+/-] in Blood by DCIP"
              }
            ],
            "text": "การคัดกรอง DCIP/E screen"
          }
        }
        if len(patient_resource) > 0:
            observation_dcip_resource["subject"] = {
                "reference": f"Patient/{str(patient_resource['id'])}"
            }
        if len(encounter_resource) > 0:   
            observation_dcip_resource["encounter"] = {
                "reference": f"Encounter/{str(encounter_resource['id'])}"
            }
        if adp['DATEOPD'][i] is not None:
            observation_dcip_resource["effectiveDateTime"] = str(adp['DATEOPD'][i])+"T17:00:00+07:00" 
        if adp['PROVIDER'][i] is not None:
            observation_dcip_resource["performer"] = [
                {
                  "type": "Practitioner",
                  "identifier":{
                    "system": "https://terms.sil-th.org/id/th-doctor-id",
                    "value": adp['PROVIDER'][i]
                  }
                }
              ]
        if adp['DCIP'][i] is not None:
            observation_dcip_resource["valueCodeableConcept"] = {
                "coding": [
                  {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                    "code": adp['DCIP'][i],
                    "display": "Negative"
                  }
                ]
              }
        bundle_observation_dcip_structure = {
            "fullUrl": f"https://example.com/Observation/{observation_dcip_resource['id']}",
            "resource": observation_dcip_resource,
            "request": {
                "method": "PUT",
                "url": f"Observation/{observation_dcip_resource['id']}"
                }
        }
    return bundle_observation_dcip_structure
def create_observation_ga_resource(adp,patient_bundle,encounter_bundle,i,h_code):
    bundle_observation_ga_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if adp['GA_WEEK'][i] is not None and len(adp['GA_WEEK'][i]) > 1:
        observation_ga_id = f"hcode-{h_code}-code-57714-8-{patient_resource['id']}"
        observation_gestational_resource = {
          "resourceType": "Observation",
          "id": observation_ga_id,
          "status": "final",
          "category": [
            {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                  "code": "exam",
                  "display": "Exam"
                }
              ],
              "text": "Exam"
            }
          ],
          "code": {
            "coding": [
              {
                "system": "http://loinc.org",
                "code": "57714-8",
                "display": "Obstetric estimation of gestational age"
              },
              {
                "system": "http://snomed.info/sct",
                "code": "57036006",
                "display": "Fetal gestational age"
              }
            ],
            "text": "อายุครรภ์ปัจจุบัน ณ วันที่ตรวจครั้งแรก (สัปดาห์)"
          }
        }
        if len(patient_resource) > 0:
            observation_gestational_resource["subject"] = {
                "reference": f"Patient/{str(patient_resource['id'])}"
            }
        if len(encounter_resource) > 0:   
            observation_gestational_resource["encounter"] = {
                "reference": f"Encounter/{str(encounter_resource['id'])}"
            }
        if adp['DATEOPD'][i] is not None:
            observation_gestational_resource["effectiveDateTime"] = str(adp['DATEOPD'][i])+"T17:00:00+07:00" 
        if adp['GA_WEEK'][i] is not None:
            observation_gestational_resource["valueQuantity"] = {
                "value": int(adp['GA_WEEK'][i]),
                "unit": "สัปดาห์",
                "system": "http://unitsofmeasure.org",
                "code": "wk"
              }
        bundle_observation_ga_structure = {
            "fullUrl": f"https://example.com/Observation/{observation_gestational_resource['id']}",
            "resource": observation_gestational_resource,
            "request": {
                "method": "PUT",
                "url": f"Observation/{observation_gestational_resource['id']}"
                }
        }
    return bundle_observation_ga_structure
def create_observation_gravida_resource(adp,patient_bundle,encounter_bundle,i,h_code):
    bundle_observation_gravida_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if adp['GRAVIDA'][i] is not None and len(adp['GRAVIDA'][i]) > 1:
        observation_gravida_id = f"hcode-{h_code}-code-11996-6-{patient_resource['id']}"
        observation_gravida_resource = {
          "resourceType": "Observation",
          "id": observation_gravida_id,
          "status": "final",
          "category": [
            {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                  "code": "exam",
                  "display": "Exam"
                }
              ],
              "text": "Exam"
            }
          ],
          "code": {
            "coding": [
              {
                "system": "http://loinc.org",
                "code": "11996-6",
                "display": "[#] Pregnancies"
              },
              {
                "system": "http://snomed.info/sct",
                "code": "161732006",
                "display": "Gravida"
              }
            ],
            "text": "ครรภ์ที่"
          }
        }
        if len(patient_resource) > 0:
            observation_gravida_resource["subject"] = {
                "reference": f"Patient/{str(patient_resource['id'])}"
            }
        if len(encounter_resource) > 0:   
            observation_gravida_resource["encounter"] = {
                "reference": f"Encounter/{str(encounter_resource['id'])}"
            }
        if adp['DATEOPD'][i] is not None:
            observation_gravida_resource["effectiveDateTime"] = str(adp['DATEOPD'][i])+"T17:00:00+07:00" 
        if adp['GRAVIDA'][i] is not None:
            observation_gravida_resource["valueInteger"] = int(adp['GRAVIDA'][i]) 
        bundle_observation_gravida_structure = {
            "fullUrl": f"https://example.com/Observation/{observation_gravida_resource['id']}",
            "resource": observation_gravida_resource,
            "request": {
                "method": "PUT",
                "url": f"Observation/{observation_gravida_resource['id']}"
                }
        }
    return bundle_observation_gravida_structure
def create_observation_lab_resource(labfu,patient_bundle,encounter_bundle,h_code):
    bundle_observation_labfu_structure = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if labfu.shape[0] != 0:
        labfu_id = f"hcode-{h_code}-code-01-{patient_resource['id']}"
        labfu_resource = {
          "resourceType": "Observation",
          "id": labfu_id,
          "status": "final",
          "category": [
            {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                  "code": "laboratory",
                  "display": "Laboratory"
                }
              ],
              "text": "Laboratory"
            }
          ],
          "code": {
            "coding": [
              {
                "system": "https://terms.sil-th.org/CodeSystem/cs-thcc-2digit-lab",
                "code": "01",
                "display": "ตรวจน้ำตาลในเลือด จากหลอดเลือดดำ หลังอดอาหาร"
              }
            ]
          }
        }
        if len(patient_resource) > 0:
            labfu_resource["subject"] = {
                "reference": f"Patient/{str(patient_resource['id'])}"
            }
        if len(encounter_resource) > 0:   
            labfu_resource["encounter"] = {
                "reference": f"Encounter/{str(encounter_resource['id'])}"
            }
        if labfu.iloc[0]['DATESERV'] is not None:
            labfu_resource["effectiveDateTime"] = str(labfu.iloc[0]['DATESERV'])+"T17:00:00+07:00" 
        if labfu.iloc[0]['HCODE'] is not None:
            labfu_resource["performer"] = [
                {
                  "type": "Organization",
                  "identifier": {
                    "system": "https://terms.sil-th.org/id/th-moph-hcode",
                    "value": labfu.iloc[0]['HCODE'] 
                  }
                }
            ]
        if labfu.iloc[0]['LABRESULT'] is not None:
            labfu_resource["valueQuantity"] = {
                  "value": labfu.iloc[0]['LABRESULT'] 
                }
        bundle_observation_labfu_structure = {
            "fullUrl": f"https://example.com/Observation/{labfu_resource['id']}",
            "resource": labfu_resource,
            "request": {
                "method": "PUT",
                "url": f"Observation/{labfu_resource['id']}"
                }
            }
    return bundle_observation_labfu_structure
def update_encounter_resource_by_ins(ins,encounter_bundle):
    encounter_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if len(encounter_resource) > 0:
      if ins.shape[0] != 0:
        if ins.iloc[0]['HTYPE'] is not None and len(ins.iloc[0]['HTYPE']) > 0:
            encounter_resource["serviceProvider"] = {
                    "extension": [
                      {
                        "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-encounter-provider-type",
                        "valueCodeableConcept": {
                          "coding": [
                            {
                              "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-provider-type",
                              "code": ins.iloc[0]['HTYPE'],
                              "display": provider_type_dict[ins.iloc[0]['HTYPE']]
                            }
                          ]
                        }
                      }
                    ],
                    "type": "Organization",
                    "identifier": {
                      "system": "https://terms.sil-th.org/id/th-moph-hcode",
                      "value": h_code
                    }
                }
    return encounter_bundle
def update_encounter_resource_by_lvd(lvd,encounter_bundle):
    encounter_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if len(encounter_resource) > 0:
      if lvd.shape[0] != 0:
        if lvd.iloc[0]['SEQLVD'] is not None and len(lvd.iloc[0]['SEQLVD']) > 0:
            if "extension" not in encounter_resource:
                encounter_resource['extension'] = [dict()]
                encounter_resource['extension'][0]["extension"] = []
                encounter_resource['extension'][0]['url'] = "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-encounter-leave-day"
            encounter_resource['extension'][0]["extension"].append({
                  "url": "id",
                  "valueString": lvd.iloc[0]['SEQLVD'] 
                })
        if lvd.iloc[0]['DATEOUT'] is not None and lvd.iloc[0]['TIMEOUT'] is not None:
            leave_dt = str(lvd.iloc[0]['DATEOUT']) + 'T' + str(lvd.iloc[0]['TIMEOUT'][:2]) +':' + str(lvd.iloc[0]['TIMEOUT'][2:]) + ':00+07:00'
            if "extension" not in encounter_resource:
                encounter_resource['extension'] = [dict()]
                encounter_resource['extension'][0]["extension"] = []
                encounter_resource['extension'][0]['url'] = "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-encounter-leave-day"
            encounter_resource['extension'][0]["extension"].append({
                  "url": "leaveDate",
                  "valueDateTime": leave_dt
                })
        if lvd.iloc[0]['DATEIN'] is not None and lvd.iloc[0]['TIMEIN'] is not None:
            cb_dt = str(lvd.iloc[0]['DATEIN']) + 'T' + str(lvd.iloc[0]['TIMEIN'][:2]) +':' + str(lvd.iloc[0]['TIMEIN'][2:]) + ':00+07:00'
            if "extension" not in encounter_resource:
                encounter_resource['extension'] = [dict()]
                encounter_resource['extension'][0]["extension"] = []
                encounter_resource['extension'][0]['url'] = "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-encounter-leave-day"
            encounter_resource['extension'][0]["extension"].append({
                  "url": "comeBack",
                  "valueDateTime": cb_dt
                })
        if lvd.iloc[0]['QTYDAY'] is not None and len(lvd.iloc[0]['QTYDAY']) > 0:
            if "extension" not in encounter_resource:
                encounter_resource['extension'] = [dict()]
                encounter_resource['extension'][0]["extension"] = []
                encounter_resource['extension'][0]['url'] = "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-encounter-leave-day"
            encounter_resource['extension'][0]["extension"].append({
                  "url": "duration",
                  "valueDuration": {
                    "value": lvd.iloc[0]['QTYDAY'],
                    "unit": "วัน",
                    "system": "http://unitsofmeasure.org",
                    "code": "d"
                  }
                })
    return encounter_bundle
def update_encounter_resource_by_irf(irf,service_req_bundle,encounter_bundle):
    encounter_resource = dict()
    service_req_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in service_req_bundle:
        service_req_resource = service_req_bundle['resource']
    if len(encounter_resource) > 0:
      if irf.shape[0] != 0:
        if irf[irf['REFERTYPE']=='1'].shape[0] > 0:
            refer_in = irf[irf['REFERTYPE']=='1']
            if refer_in.iloc[0]['REFER'] is not None:
                encounter_resource['hospitalization']['origin'] = {
                      "type": "Organization",
                      "identifier": {
                        "system": "https://terms.sil-th.org/id/th-moph-hcode",
                        "value": refer_in.iloc[0]['REFER']
                      }
                    }
        if len(service_req_resource) > 0:
          if irf[irf['REFERTYPE']=='2'].shape[0] > 0:
              encounter_resource['basedOn']  = [
                  {
                    "reference": f"ServiceRequest/{service_req_resource['id']}"
                  }
              ]
    return encounter_bundle
def update_encounter_resource_by_idx(idx,encounter_bundle,condition_bundle_arr):
    encounter_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if idx.shape[0] != 0:
      if len(encounter_resource) > 0:
        if len(condition_bundle_arr) > 0:
          encounter_resource['diagnosis'] = [dict() for i in range(idx.shape[0])]
          for i in range(idx.shape[0]):
            if idx['DXTYPE'][i] is not None and len(idx['DXTYPE'][i]) > 0:
                encounter_resource['diagnosis'][i]['use'] = dict()
                encounter_resource['diagnosis'][i]['condition'] = dict()
                encounter_resource['diagnosis'][i]['condition']['reference'] = f"Condition/{condition_bundle_arr[i]['resource']['id']}"
                encounter_resource['diagnosis'][i]['use']['coding'] = [dict()]
                encounter_resource['diagnosis'][i]['use']['coding'][0]['system'] = "https://terms.sil-th.org/CodeSystem/cs-43plus-encounter-diagnosis-role"
                encounter_resource['diagnosis'][i]['use']['coding'][0]['code'] = idx['DXTYPE'][i] 
                encounter_resource['diagnosis'][i]['use']['coding'][0]['display'] = diag_role_dict[idx['DXTYPE'][i]] 
    return encounter_bundle
def update_encounter_by_dru(dru,encounter_resource):
    if dru['CLINIC'][0] is not None and len(dru['CLINIC'][0]) > 0:
        encounter_resource['serviceType'] = {
            "coding": [
                      {
                        "system": "",
                        "code": dru['CLINIC'][0] 
                      }
                    ]
        }
    return encounter_resource
def update_claim_resource_by_cht(cht,claim_bundle,patient_bundle,encounter_bundle,h_code):
    claim_resource = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if 'resource' in claim_bundle:
        claim_resource = claim_bundle['resource']
    if cht.shape[0] != 0:
      if len(claim_resource) == 0:
          claim_id = f"{encounter_resource['id']}"
          claim_resource = {
            "resourceType": "Claim",
            "id": claim_id,
            "status": "active"
          }
          claim_resource['type'] = {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                  "code": "institutional"
                }
              ]
            }
          claim_resource['use'] = "claim"
          claim_resource['provider'] = {
              "reference": f"Organization/hcode-{h_code}"
            }
          claim_resource['priority'] = {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/processpriority",
                  "code": "normal"
                }
              ]
            }
          if cht.iloc[0]['PERSON_ID'] is not None and len(cht.iloc[0]['PERSON_ID']) > 0:
            claim_resource['patient'] = {
                "reference": f"Patient/{str(patient_resource['id'])}",
                "identifier": {
                  "system": "https://terms.sil-th.org/id/th-cid",
                  "value": cht.iloc[0]['PERSON_ID']
                }
              }
          if cht.iloc[0]['DATE'] is not None:
              claim_resource['created'] = str(cht.iloc[0]['DATE'])+"T17:00:00+07:00" 
          if cht.iloc[0]['PERSON_ID'] is not None and len(cht.iloc[0]['PERSON_ID']) > 0:
              claim_resource['insurance'] = [
                  {
                    "sequence": 1,
                    "focal": True,
                    "coverage": {
                      "reference": f"Coverage/hcode-{h_code}-uc-cid-{cht.iloc[0]['PERSON_ID']}"
                    }
                  }
                ]
          claim_bundle['resource'] = claim_resource
      if cht.iloc[0]['PAID'] is not None:
          claim_resource['extension'] = [
              {
                "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-claim-total-paid",
                "valueMoney": {
                  "value": float(cht.iloc[0]['PAID']),
                  "currency": "THB"
                }
              }
            ]
      if cht.iloc[0]['INVOICE_NO'] is not None and len(cht.iloc[0]['INVOICE_NO']) > 0:
          if "identifier" not in claim_resource:
              claim_resource['identifier'] = []
          claim_resource['identifier'].append({
                "type": {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                      "code": "localInvNo",
                      "display": "เลขที่อ้างอิงใบแจ้งหนี้ของหน่วยบริการ"
                    }
                  ]
                },
                "system": "https://terms.sil-th.org/hcode/5/{}/Inv".format(h_code),
                "value": cht.iloc[0]['INVOICE_NO'] 
              })   
      if cht.iloc[0]['INVOICE_LT'] is not None and len(cht.iloc[0]['INVOICE_LT']) > 0:
          if "identifier" not in claim_resource:
              claim_resource['identifier'] = []
          claim_resource['identifier'].append({
                "type": {
                  "coding": [
                    {
                      "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                      "code": "localInvLt",
                      "display": "เลขที่อ้างอิงชุดข้อมูลใบแจ้งหนี้ของหน่วยบริการ"
                    }
                  ]
                },
                "system": "https://terms.sil-th.org/hcode/5/{}/InvLt".format(h_code),
                "value": cht.iloc[0]['INVOICE_LT'] 
              })
      if cht.iloc[0]['OPD_MEMO'] is not None and len(cht.iloc[0]['OPD_MEMO']) > 0:
          claim_resource['supportingInfo'] = [
              {
                "sequence": 1,
                "category": {
                  "coding": [
                    {
                      "system": "http://terminology.hl7.org/CodeSystem/claiminformationcategory",
                      "code": "info"
                    }
                  ]
                },
                "valueString": cht.iloc[0]['OPD_MEMO']
              }
            ]
      if cht.iloc[0]['TOTAL'] is not None:
          claim_resource['total'] = {
              "value": float(cht.iloc[0]['TOTAL']),
              "currency": "THB"
            }
    return claim_bundle
def update_claim_resource_by_aer(aer,claim_bundle,patient_bundle,encounter_bundle,h_code):
    claim_resource = dict()
    encounter_resource = dict()
    patient_resource = dict()
    if 'resource' in encounter_bundle:
        encounter_resource = encounter_bundle['resource']
    if 'resource' in patient_bundle:
        patient_resource = patient_bundle['resource']
    if 'resource' in claim_bundle:
        claim_resource = claim_bundle['resource']
    if aer.shape[0] != 0:
      if len(claim_resource) == 0:
          claim_id = f"{encounter_resource['id']}"
          claim_resource = {
            "resourceType": "Claim",
            "id": claim_id,
            "status": "active"
          }
          claim_resource['type'] = {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                  "code": "institutional"
                }
              ]
            }
          claim_resource['use'] = "claim"
          claim_resource['provider'] = {
              "reference": f"Organization/hcode-{h_code}"
            }
          claim_resource['priority'] = {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/processpriority",
                  "code": "normal"
                }
              ]
            }
          if aer.iloc[0]['CID'] is not None and len(aer.iloc[0]['CID']) > 0:
            claim_resource['patient'] = {
                "reference": f"Patient/{str(patient_resource['id'])}",
                "identifier": {
                  "system": "https://terms.sil-th.org/id/th-cid",
                  "value": aer.iloc[0]['CID']
                }
              }
          if aer.iloc[0]['DATEEXP'] is not None:
              claim_resource['created'] = str(aer.iloc[0]['DATEEXP'])+"T17:00:00+07:00" 
          if aer.iloc[0]['CID'] is not None and len(aer.iloc[0]['CID']) > 0:
              claim_resource['insurance'] = [
                  {
                    "sequence": 1,
                    "focal": True,
                    "coverage": {
                      "reference": f"Coverage/hcode-{h_code}-uc-cid-{aer.iloc[0]['CID']}"
                    }
                  }
                ]
          claim_bundle['resource'] = claim_resource
      if aer.iloc[0]['REFER_NO'] is not None:
          claim_resource['referral'] = {
              "identifier": {
                "system": "https://terms.sil-th.org/id/th-refer-code",
                "value": aer.iloc[0]['REFER_NO'] 
              }
            }
      if 'supportingInfo' in claim_resource:
          claim_resource:['supportingInfo'].append({
            "sequence": len(claim_resource['supportingInfo']) + 1,
            "category": {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/claiminformationcategory",
                  "code": "info",
                  "display": "Information"
                }
              ]
            },
            "valueReference": {
              "reference": f"Encounter/{encounter_resource['id']}"
            }
          })
      else:
          claim_resource['supportingInfo'] = [{
            "sequence": 1,
            "category": {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/claiminformationcategory",
                  "code": "info",
                  "display": "Information"
                }
              ]
            },
            "valueReference": {
              "reference": f"Encounter/{encounter_resource['id']}"
            }
          }]
      if 'insurance' in claim_resource:
          if aer.iloc[0]['AETYPE'] is not None:
              claim_resource['insurance'][0]['extension'] = [{
                  "valueCodeableConcept": {
                      "coding": [
                          {
                          "system": "https://fhir-ig.sil-th.org/extensions/StructureDefinition-ex-claim-insurance-aetype.html",
                          "code": aer.iloc[0]['AETYPE'] 
                          }
                      ]
                  }
              }]
          if aer.iloc[0]['AUTHAE'] is not None:
              claim_resource['insurance'][0]['preAuthRef'] = [
                  aer.iloc[0]['AUTHAE'] 
              ]
          if aer.iloc[0]['AEDATE'] is not None:
              if "accident" not in claim_resource:
                  claim_resource['accident'] = dict()
              claim_resource['accident']["date"] = aer.iloc[0]['AEDATE']
          if aer.iloc[0]['AETIME'] is not None:
              if "accident" not in claim_resource:
                  claim_resource['accident'] = dict()
              claim_resource['accident']["_date"] = {
                    "extension": [
                      {
                        "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-claim-accident-datetime",
                        "valueDateTime": aer.iloc[0]['AETIME'] 
                      }
                    ]
                  }
      else:
          if aer.iloc[0]['AETYPE'] is not None:
              claim_resource['insurance'] = [{
                  "extension" : [{
                  "valueCodeableConcept": {
                      "coding": [
                          {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-accident-coverage",
                          "code": aer.iloc[0]['AETYPE'] 
                          }
                      ]
                  }
              }]
              }]
          if aer.iloc[0]['AUTHAE'] is not None:
              claim_resource['insurance'][0]['preAuthRef'] = [
                  aer.iloc[0]['AUTHAE'] 
              ]
          if aer.iloc[0]['AEDATE'] is not None:
              if "accident" not in claim_resource:
                  claim_resource['accident'] = dict()
              claim_resource['accident']["date"] = aer.iloc[0]['AEDATE']
          if aer.iloc[0]['AETIME'] is not None:
              if "accident" not in claim_resource:
                  claim_resource['accident'] = dict()
              claim_resource['accident']["_date"] = {
                    "extension": [
                      {
                        "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-claim-accident-datetime",
                        "valueDateTime": aer.iloc[0]['AETIME'] 
                      }
                    ]
                  }
    return claim_bundle
def update_service_request_resource_by_aer(aer,irf,service_req_bundle):
    service_req_resource = dict()
    if 'resource' in service_req_bundle:
        service_req_resource = service_req_bundle['resource']
    if aer.shape[0] != 0:
      if len(service_req_resource) > 0:
        if irf[irf['REFERTYPE']=='2'].shape[0] > 0:
            if aer.iloc[0]['UCAE'] is not None: 
                service_req_resource['extension'] = [
                    {
                      "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-servicerequest-refer-patient-category",
                      "valueCodeableConcept": {
                        "coding": [
                          {
                            "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-refer-type-eclaim",
                            "code": aer.iloc[0]['UCAE'],
                            "display": refer_type_dict[aer.iloc[0]['UCAE']] 
                          }
                        ]
                      }
                    }
                  ]
            if aer.iloc[0]['REFER_NO'] is not None:
                service_req_resource['identifier'] = [
                    {
                      "type": {
                        "coding": [
                          {
                            "system": "https://terms.sil-th.org/CodeSystem/cs-th-identifier-type",
                            "code": "localReferDoc"
                          }
                        ]
                      },
                      "system": "https://terms.sil-th.org/hcode/5/13814/ReferDoc",
                      "value": aer.iloc[0]['REFER_NO'] 
                    }
                  ]
            if aer.iloc[0]['OREFTYPE'] is not None:
                service_req_resource['category'] = [
                    {
                      "coding": [
                        {
                          "system": "https://terms.sil-th.org/CodeSystem/cs-thcc-refer-in-reason",
                          "code": aer.iloc[0]['OREFTYPE'],
                          "display": refer_in_reason_dict[aer.iloc[0]['OREFTYPE']] 
                        }
                      ]
                    }
                  ]
            if aer.iloc[0]['EMTYPE'] is not None:
                service_req_resource['_priority'] = {
                    "extension": [
                      {
                        "url": "https://fhir-ig.sil-th.org/extensions/StructureDefinition/ex-servicerequest-refer-priority-reason",
                        "valueCodeableConcept": {
                          "coding": [
                            {
                              "system": "https://terms.sil-th.org/CodeSystem/cs-eclaim-refer-priority-code",
                              "code": aer.iloc[0]['EMTYPE'],
                              "display": refer_priority_code_dict[aer.iloc[0]['EMTYPE']]
                            }
                          ]
                        }
                      }
                    ]
                  }
    return service_req_bundle
