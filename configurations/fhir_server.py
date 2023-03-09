import os
from dotenv import load_dotenv

load_dotenv(".env")

base_fhir_url = os.getenv('FHIR_SERVER_URL')

max_patient_per_cycle = 5000
