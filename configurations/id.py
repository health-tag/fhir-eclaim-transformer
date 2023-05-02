from fhir.resources.R4B.fhirtypes import Id


def organization_id(hospital_code)->Id:
    return Id(f"hcode-{hospital_code}")

def patient_id(citizen_id)->Id:
    return Id(f"cid-{citizen_id}")

def coverage_id(hospital_code, citizen_id)->Id:
    return Id(f"hcode-{hospital_code}-cid-{citizen_id}")

def account_id(hospital_code, sequence)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}")

def encounter_id(hospital_code, sequence)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}")

def service_request_id(hospital_code, sequence)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}")

def condition_id(hospital_code, sequence, icd10)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}-code-odx-{icd10}")

def procedure_id(hospital_code, sequence, icd9)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}-code-{icd9}")

def claim_id(hospital_code, sequence)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}")

def claim_drug_id(hospital_code, sequence, drug_local_code)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}-drug-{drug_local_code}")

def medication_request_id(hospital_code, sequence, drug_local_code)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}-drug-{drug_local_code}")

def medication_dispense_id(hospital_code, sequence, drug_local_code)->Id:
    return Id(f"hcode-{hospital_code}-vn-{sequence}-drug-{drug_local_code}")