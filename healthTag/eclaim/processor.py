from functools import partial
from multiprocessing import Manager, Pool

from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path

import pandas as pd
from fhir.resources import FHIRAbstractModel

from configurations.fhir import use_pydactic_validation
from configurations.id import patient_id, coverage_id, account_id, encounter_id, service_request_id, condition_id, \
    procedure_id, claim_id, organization_id, medication_request_id, medication_dispense_id, claim_drug_id

FHIRAbstractModel.Config.validate_assignment = use_pydactic_validation
from fhir.resources.resource import Resource
from fhir.resources.bundle import Bundle
from fhir.resources.fhirtypes import Id, Code, String, Uri, Boolean, Decimal
from fhir.resources.claim import Claim, ClaimInsurance, ClaimItem
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.condition import Condition
from fhir.resources.coverage import Coverage, CoverageClass
from fhir.resources.humanname import HumanName
from fhir.resources.extension import Extension
from fhir.resources.medicationdispense import MedicationDispense
from fhir.resources.medicationrequest import MedicationRequest
from fhir.resources.money import Money
from fhir.resources.procedure import Procedure, ProcedurePerformer
from fhir.resources.quantity import Quantity
from fhir.resources.servicerequest import ServiceRequest

from fhir.resources.organization import Organization
from fhir.resources.identifier import Identifier
from fhir.resources.patient import Patient
from fhir.resources.reference import Reference
from fhir.resources.coding import Coding
from fhir.resources.period import Period
from fhir.resources.account import Account
from fhir.resources.encounter import Encounter, EncounterLocation
from tqdm import tqdm

from healthTag.eclaim.files.E_1InsCsv import open_ins_file, InsCsvRow
from healthTag.eclaim.files.E_2PatCsv import open_pat_file, PatCsvRow
from healthTag.eclaim.files.E_3OpdCsv import open_opd_file, OpdCsvRow
from healthTag.eclaim.files.E_4OrfCsv import open_orf_file, OrfCsvRow
from healthTag.eclaim.files.E_5OdxCsv import open_odx_file, OdxCsvRow
from healthTag.eclaim.files.E_6OopCsv import open_oop_file, OopCsvRow
from healthTag.eclaim.files.E_11ChtCsv import open_cht_file, ChtCsvRow
from healthTag.eclaim.files.E_12ChaCsv import open_cha_file, ChaCsvRow
from healthTag.eclaim.files.E_16DruCsv import open_dru_file, DruCsvRow
from typing import Optional
from collections.abc import Iterable
from utilities.fhir_communicator import create_bundle


def save_bundle_to_file(bundle: Bundle, output_folder: PathLike, file_name: str):
    file_path = Path.joinpath(output_folder, file_name)
    print(f'Saving result to {file_path}')
    with open(file_path, "w", encoding='utf8') as f:
        f.write(bundle.json(ensure_ascii=False, indent=2))
    print(f'Successfully save result to {file_path}')


@dataclass
class JoinedOpd:
    row_1ins: Optional[InsCsvRow] = None  # 1 Sequence
    row_3opd: Optional[OpdCsvRow] = None  # 1 Sequence
    row_4orf: Optional[OrfCsvRow] = None  # 1 Sequence
    row_5odx: list[OdxCsvRow] = field(default_factory=list)  # Many Sequence
    row_6oop: list[OopCsvRow] = field(default_factory=list)  # Many Sequence
    row_11cht: Optional[ChtCsvRow] = None  # 1 Sequence
    row_12cha: list[ChaCsvRow] = field(default_factory=list)  # Many Sequence
    row_16dru: list[DruCsvRow] = field(default_factory=list)  # Many Sequence


def process_patient(organizations_dict: dict[str, Organization], patients_dict: dict[str, Patient], row: PatCsvRow):
    patient: Patient = Patient.construct()
    patient.identifier = [
        Identifier(system=Uri("https://www.dopa.go.th"), value=f"{row.citizen_id}"),
        Identifier(system=Uri("https://terms.sil-th.org/id/th-cid"), value=f"{row.hospital_number}"),
        Identifier(system=Uri("https://sil-th.org/fhir/Id/hn"), value=f"{row.hospital_number}"),
    ]
    patient.name = [HumanName(prefix=[String(row.title)], given=[String(row.name)], family=String(row.surname),
                              text=String(f"{row.title} {row.name} {row.surname}"))]
    patient.gender = Code("male" if row.gender_number == 1 else "female")
    matched_org = organizations_dict[row.hospital_code]
    patient.managingOrganization = Reference(reference=matched_org.relative_path())
    patient.id = patient_id(row.citizen_id)

    patients_dict[row.hospital_number] = patient


def process_matched_seq(organizations_dict: dict[str, Organization], patients_dict: dict[str, Patient],
                        coverages: list[Coverage], accounts: list[Account], encounters: list[Encounter],
                        service_requests: list[ServiceRequest], conditions: list[Condition],
                        procedures: list[Procedure], claims: list[Claim],
                        medication_dispenses: list[MedicationDispense],
                        medication_requests: list[MedicationRequest],
                        matched):
    sequence: str
    matched: JoinedOpd
    sequence, matched = matched
    if matched.row_1ins is None:
        # This is not an OPD entry. Ignore it because hLabs code will handle it
        return
    patient = patients_dict[matched.row_1ins.hospital_number]
    matched_org: Optional[Organization] = None
    main_hospital_code = None
    if not pd.isna(matched.row_1ins.main_hospital_code):
        matched_org = organizations_dict[matched.row_1ins.main_hospital_code]
        main_hospital_code= matched.row_1ins.main_hospital_code
    citizenId = patient.identifier[0].value  # type: ignore
    # Coverage
    if matched.row_1ins is not None and matched.row_11cht is not None:
        coverage: Coverage = Coverage.construct()
        coverage.status = Code("active")
        coverage.type = CodeableConcept(coding=[
            Coding(system=Uri("https://terms.sil-th.org/ValueSet/vs-eclaim-coverage-use"),
                   code=Code(matched.row_1ins.insurance_type))])
        coverage.beneficiary = Reference(reference=patient.relative_path())
        if isinstance(matched.row_1ins.insurance_expired_date, str):
            coverage.period = Period(end=matched.row_1ins.insurance_expired_date)
        if matched_org is not None:
            coverage.payor = [Reference(reference=matched_org.relative_path())]
        coverage.class_fhir = [CoverageClass(type=CodeableConcept(coding=[
            Coding(system=Uri("http://terminology.hl7.org/CodeSystem/coverage-class"), code=Code("subplan"))]),
            value=String(matched.row_1ins.subtype)),
            CoverageClass(type=CodeableConcept(coding=[
                Coding(system=Uri("http://terminology.hl7.org/CodeSystem/coverage-class"),
                       code=Code("subplan"))]), value=String(matched.row_11cht.patient_type))]
        coverage.extension = [Extension(
            url=Uri("https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-coverage-contracted-provider"),
            extension=[
                Extension(url=Uri("type"), valueCodeableConcept=CodeableConcept(coding=[
                    Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-meta-provider-type-coverage"),
                           code=Code("primary"), display=String("สถานบริการหลัก"))])),
                Extension(url=Uri("provider"),
                          valueIdentifier=Identifier(system=Uri("https://terms.sil-th.org/id/th-moph-hcode"),
                                                     value=String(matched.row_1ins.main_hospital_code)))
            ])]
        coverage.id = coverage_id(main_hospital_code,citizenId)
        coverages.append(coverage)

    if matched.row_3opd is not None:
        # Account
        account = Account.construct()
        account.status = Code("active")
        account.identifier = [Identifier(type=CodeableConcept(coding=[
            Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-th-identifier-type"),
                   code=Code("localVn"))]),
            system=Uri(
                f"https://terms.sil-th.org/hcode/5/{matched.row_1ins.main_hospital_code}/VN"),
            value=String(matched.row_3opd.sequence))]
        account.subject = [Reference(reference=patient.relative_path())]
        account.servicePeriod = Period(start=matched.row_3opd.dateopd + matched.row_3opd.timeopd)
        account.extension = [
            Extension(url=Uri("https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-account-coverage-use"),
                      valueCodeableConcept=CodeableConcept(coding=[
                          Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-43plus-coverage-use"),
                                 code=Code(matched.row_3opd.uuc))]))]
        account.id = account_id(main_hospital_code,matched.row_3opd.sequence)
        accounts.append(account)

        # Encounter
        encounter = Encounter.construct()
        encounter.status = Code("finished")
        encounter.identifier = [Identifier(type=CodeableConcept(coding=[
            Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-th-identifier-type"),
                   code=Code("localVn"))]),
            system=Uri(
                f"https://terms.sil-th.org/hcode/5/{matched.row_1ins.main_hospital_code}/VN"),
            value=String(matched.row_3opd.sequence))]
        encounter.class_fhir = Coding(system=Uri("http://terminology.hl7.org/CodeSystem/v3-ActCode"),
                                      code=Code("AMB"), display=String("ambulatory"))
        encounter.subject = Reference(reference=patient.relative_path())
        encounter.period = Period(start=matched.row_3opd.dateopd + matched.row_3opd.timeopd)
        if matched_org is not None:
            if isinstance(matched.row_1ins.htype, str):
                encounter.serviceProvider = Reference(reference=matched_org.relative_path(),
                                                      extension=[Extension(url=Uri(
                                                          "https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-encounter-provider-type"),
                                                          valueCodeableConcept=CodeableConcept(
                                                              coding=[Coding(system=Uri(
                                                                  "https://terms.sil-th.org/CodeSystem/cs-eclaim-provider-type"),
                                                                  code=Code(
                                                                      matched.row_1ins.htype),
                                                                  display=String(
                                                                      "Main Contractor"))]))])
            else:
                encounter.serviceProvider = Reference(reference=matched_org.relative_path())
        encounter.account = [Reference(reference=account.relative_path())]
        # encounter.diagnosis = [EncounterDiagnosis(condition=Reference(display=String(matched.row_3opd.icd10)))]
        # encounter.hospitalization = EncounterHospitalization(admitSource=CodeableConcept(coding=[Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-eclaim-admit-source"), code=Code(matched.row_3opd.admit_source))]))
        encounter.location = [EncounterLocation(location=Reference(identifier=Identifier(
            system=Uri(f"https://terms.sil-th.org/hcode/5/{matched.row_1ins.main_hospital_code}/DepCode"),
            value=String(matched.row_3opd.clinic))))]
        encounter.extension = [
            Extension(
                url=Uri("https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-encounter-service-type-th"),
                valueCodeableConcept=CodeableConcept(coding=[
                    Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-eclaim-service-type-th"),
                           code=Code(matched.row_3opd.optype), display=String("OP บัตรตัวเอง"))]))]
        encounter.id = encounter_id(main_hospital_code,matched.row_3opd.sequence)
        encounters.append(encounter)

        # ServiceRequest
        if matched.row_4orf is not None:
            service_request = ServiceRequest.construct(status=Code("completed"), intent=Code("order"))
            service_request.code = CodeableConcept(coding=[
                Coding(system=Uri("http://snomed.info/sct"), code=Code("3457005"),
                       display=String("Patient referral"))])
            service_request.subject = Reference(reference=patient.relative_path())
            service_request.encounter = Reference(reference=encounter.relative_path())
            if matched_org is not None:
                service_request.performer = [Reference(reference=matched_org.relative_path())]
            service_request.id = service_request_id(main_hospital_code, matched.row_4orf.sequence)
            service_request.code = CodeableConcept(coding=[
                Coding(system=Uri("http://snomed.info/sct"), code=Code("3457005"),
                       display=String("Patient referral"))])
            service_requests.append(service_request)
        # Condition
        for matched_row_5odx in matched.row_5odx:
            condition = Condition.construct()
            condition.clinicalStatus = CodeableConcept(coding=[
                Coding(system=Uri("http://terminology.hl7.org/CodeSystem/condition-clinical"),
                       code=Code("active"))])
            condition.category = [CodeableConcept(coding=[
                Coding(system=Uri("http://terminology.hl7.org/CodeSystem/condition-category"),
                       code=Code("encounter-diagnosis"), display=String("Encounter Diagnosis"))])]
            condition.code = CodeableConcept(coding=[
                Coding(system=Uri("http://hl7.org/fhir/sid/icd-10"),
                       code=Code(matched_row_5odx.diagnosis_icd10))])
            condition.subject = Reference(reference=patient.relative_path())
            condition.encounter = Reference(reference=encounter.relative_path())
            condition.id = condition_id(main_hospital_code, matched_row_5odx.sequence, matched_row_5odx.diagnosis_icd10)
            conditions.append(condition)
        # Procedure
        for matched_row_6oop in matched.row_6oop:
            procedure = Procedure.construct()
            procedure.status = Code("completed")
            procedure.identifier = [Identifier(system=Uri(f"https://sil-th.org/fhir/Id/service-id"),
                                               value=String(matched.row_3opd.sequence))]
            procedure.code = CodeableConcept(coding=[
                Coding(system=Uri("http://hl7.org/fhir/sid/icd-9-cm"), code=Code(matched_row_6oop.operation),
                       display=String("Patient referral"))])
            procedure.category = CodeableConcept(coding=[
                Coding(system=Uri("http://terminology.hl7.org/CodeSystem/procedure-category"),
                       code=Code("exam"),
                       display=String("Exam"))])
            procedure.subject = Reference(reference=patient.relative_path())
            procedure.encounter = Reference(reference=encounter.relative_path())
            procedure.performedDateTime = matched_row_6oop.dateopd
            procedure.performer = [ProcedurePerformer(actor=Reference(type=Uri("Practitioner"),
                                                                      identifier=Identifier(system=Uri(
                                                                          f"https://terms.sil-th.org/id/th-doctor-id"),
                                                                          value=String(
                                                                              matched_row_6oop.dropid))))]
            procedure.id = procedure_id(main_hospital_code,matched_row_6oop.sequence, matched_row_6oop.operation )
            procedures.append(procedure)
        # Claim (11,12)
        if matched.row_11cht is not None:
            claim = Claim.construct(status=Code("active"), use=Code("claim"),
                                    created=String(matched.row_11cht.date))
            claim.extension = [
                Extension(url=Uri("https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-claim-total-cost"),
                          valueMoney=Money(value=Decimal(100), currency=Code("THB"))),
                Extension(url=Uri("https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-claim-total-copay"),
                          valueMoney=Money(value=Decimal(100), currency=Code("THB"))),
                Extension(url=Uri("https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-claim-total-paid"),
                          valueMoney=Money(value=Decimal(matched.row_11cht.paid), currency=Code("THB"))),
            ]
            # claim.identifier = [
            #    Identifier(type=CodeableConcept(coding=[Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-th-identifier-type"),code=Code("localVn"))]),system=Uri(f"https://terms.sil-th.org/hcode/5/{row.hospital_code}/Inv"), value=String(matched.row_11cht.invno)),
            #    Identifier(type=CodeableConcept(coding=[Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-th-identifier-type"),code=Code("localVn"))]),system=Uri(f"https://terms.sil-th.org/hcode/5/{row.hospital_code}/Inv"), value=String(matched.row_11cht.invoice_lt)),
            # ]
            claim.type = CodeableConcept(coding=[
                Coding(system=Uri("http://terminology.hl7.org/CodeSystem/claim-type"),
                       code=Code("institutional"))])
            claim.patient = Reference(reference=patient.relative_path())
            if matched_org is not None:
                claim.provider = Reference(reference=matched_org.relative_path())
            claim.priority = CodeableConcept(coding=[
                Coding(system=Uri("http://terminology.hl7.org/CodeSystem/processpriority"),
                       code=Code("normal"))])
            # claim.supportingInfo = [ClaimSupportingInfo(sequence=1, )]
            claim.insurance = [ClaimInsurance(sequence=1, focal=Boolean(True),
                                              coverage=Reference(reference=coverage.relative_path()))]
            claim.total = Money(value=Decimal(matched.row_11cht.total), currency=Code("THB"))
            if (len(matched.row_12cha) > 1):
                claim.item = []
                for idx, matched_row_12cha in enumerate(matched.row_12cha):
                    claimItem = ClaimItem.construct(sequence=idx)
                    claimItem.productOrService = CodeableConcept(coding=[
                        Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-eclaim-charge-item"),
                               code=Code(matched_row_12cha.chrgitem),
                               display=String("ค่าบริการทางการพยาบาล (เบิกได้)"))])
                    claimItem.servicedDate = matched_row_12cha.date
                    claimItem.encounter = [Reference(reference=encounter.relative_path())]
                    claimItem.net = Money(value=Decimal(matched_row_12cha.amount), currency=Code("THB"))
                    claim.item.append(claimItem)
            claim.id = claim_id(main_hospital_code, matched.row_11cht.sequence)
            claims.append(claim)

        for matched_row_16dru in matched.row_16dru:
            # Claim (16)
            drug_claim = Claim.construct(created=String(matched_row_16dru.service_date), use=Code("claim"),
                                         status=Code("active"))
            drug_claim.type = CodeableConcept(coding=[
                Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-th-identifier-type"),
                       code=Code("localInvNo"), display=String("เลขที่อ้างอิงใบแจ้งหนี้ของหน่วยบริการ"))])
            drug_claim.patient = Reference(reference=patient.relative_path())
            if matched_org is not None:
                drug_claim.provider = Reference(reference=matched_org.relative_path())
            drug_claim.priority = CodeableConcept(coding=[
                Coding(system=Uri("http://terminology.hl7.org/CodeSystem/processpriority"),
                       code=Code("normal"))])
            if coverage is not None:
                drug_claim.insurance = [ClaimInsurance(sequence=1, focal=Boolean(True),
                                                       coverage=Reference(reference=coverage.relative_path()),
                                                       preAuthRef=[String(matched_row_16dru.pa_no)])]
            drug_claim.total = Money(value=Decimal(matched_row_16dru.total), currency=Code("THB"))
            drug_claim.item = [ClaimItem(sequence=1, productOrService=CodeableConcept(coding=[
                Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-th-local-drug-code"),
                       code=Code(matched_row_16dru.drug_id)),
                Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-th-24drug"),
                       code=Code(matched_row_16dru.drug_id24))], text=String(matched_row_16dru.drug_name)),
                                         quantity=Quantity(value=Decimal(matched_row_16dru.amount),
                                                           unit=String(matched_row_16dru.unit)),
                                         servicedDate=String(matched_row_16dru.service_date),
                                         encounter=[Reference(reference=encounter.relative_path())],
                                         unitPrice=Money(value=Decimal(matched_row_16dru.drug_price),
                                                         currency=Code("THB")),
                                         net=Money(value=Decimal(matched_row_16dru.total),
                                                   currency=Code("THB")))]
            drug_claim.id = claim_drug_id(main_hospital_code,matched_row_16dru.sequence)
            claims.append(drug_claim)

            # MedicationDispense
            medication_dispense = MedicationDispense.construct(status=Code("completed"),
                                                               medicationCodeableConcept=CodeableConcept(
                                                                   text=String(matched_row_16dru.drug_name),
                                                                   coding=[Coding(system=Uri(
                                                                       "https://terms.sil-th.org/CodeSystem/cs-th-local-drug-code"),
                                                                       code=Code(
                                                                           matched_row_16dru.drug_id)),
                                                                       Coding(system=Uri(
                                                                           "https://terms.sil-th.org/CodeSystem/cs-th-24drug"),
                                                                           code=Code(
                                                                               matched_row_16dru.drug_id24))]))
            medication_dispense.subject = Reference(reference=patient.relative_path())
            medication_dispense.context = Reference(reference=encounter.relative_path())
            # medication_dispense.performer = [MedicationDispensePerformerType(function=CodeableConcept(coding=[Coding(system=Uri("http://terminology.hl7.org/CodeSystem/medicationdispense-performer-function"), code=Code("finalchecker"))], actor=Reference(type=String("Practitioner"), identifier=Identifier(system=String("https://terms.sil-th.org/id/th-pharmacist-id"),value=String(matched.row_16dru.provider)))))]
            medication_dispense.quantity = Quantity(value=Decimal(matched_row_16dru.amount),
                                                    unit=String(matched_row_16dru.unit))
            medication_dispense.whenHandedOver = String(matched_row_16dru.service_date)
            medication_dispense.id = medication_dispense_id(main_hospital_code, matched_row_16dru.sequence, matched_row_16dru.drug_id24)
            medication_dispenses.append(medication_dispense)

            # MedicationRequest
            medication_request = MedicationRequest.construct(status=Code("completed"), intent=Code("order"),
                                                             medicationCodeableConcept=CodeableConcept(
                                                                 text=String(matched_row_16dru.drug_name),
                                                                 coding=[
                                                                     Coding(system=Uri(
                                                                         "https://terms.sil-th.org/CodeSystem/cs-th-local-drug-code"),
                                                                         code=Code(matched_row_16dru.drug_id)),
                                                                     Coding(system=Uri(
                                                                         "https://terms.sil-th.org/CodeSystem/cs-th-24drug"),
                                                                         code=Code(
                                                                             matched_row_16dru.drug_id24))]))
            medication_request.extension = [Extension(url=Uri(
                "https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-medicationrequest-med-approved-no"),
                valueString=String(matched_row_16dru.pa_no))]
            if isinstance(matched_row_16dru.drug_remark, str) and not matched_row_16dru.drug_remark:
                medication_request.extension.append(Extension(url=Uri(
                    "https://fhir-ig.sil-th.org/mophpc/StructureDefinition/ex-medicationrequest-ned-criteria"),
                    valueCodeableConcept=CodeableConcept(coding=[Coding(
                        system=Uri(
                            "https://terms.sil-th.org/CodeSystem/cs-eclaim-medication-ned-criteria"),
                        code=Code(matched_row_16dru.drug_remark),
                        display=String(
                            "เกิดอาการไม่พึงประสงค์จากยาหรือแพ้ยาที่สามารถใช้ได้ในบัญชียาหลักแห่งชาติ"))])))
            medication_request.category = [CodeableConcept(coding=[
                Coding(system=Uri("https://terms.sil-th.org/CodeSystem/cs-eclaim-medication-category"),
                       code=Code(matched_row_16dru.use_status), display=String("ใช้ในโรงพยาบาล"))])]
            medication_request.subject = Reference(reference=patient.relative_path())
            medication_request.encounter = Reference(reference=encounter.relative_path())
            medication_request.authoredOn = String(matched_row_16dru.service_date)
            medication_request.id = medication_request_id(main_hospital_code, matched_row_16dru.sequence, matched_row_16dru.drug_id24)
            medication_requests.append(medication_request)


def process_all(_1ins_path: PathLike, _2pat_path: PathLike, _3opd_path: PathLike,
                _4orf_path: PathLike, _5odx_path: PathLike, _6oop_path: PathLike,
                _11cht_path: PathLike, _12cha_path: PathLike, _16dru_path: PathLike,
                output_folder: PathLike):
    chunk_size = 10
    _1ins_rows = open_ins_file(_1ins_path)
    _2pat_rows = open_pat_file(_2pat_path)
    _3opd_rows = open_opd_file(_3opd_path)
    _4orf_rows = open_orf_file(_4orf_path)
    _5odx_rows = open_odx_file(_5odx_path)
    _6oop_rows = open_oop_file(_6oop_path)

    _11cht_rows = open_cht_file(_11cht_path)
    _12cha_rows = open_cha_file(_12cha_path)
    _16dru_rows = open_dru_file(_16dru_path)

    joined_opd_files: dict[str, JoinedOpd] = dict()

    for row in _1ins_rows:
        if row.sequence not in joined_opd_files:
            o = JoinedOpd()
            o.row_1ins = row
            joined_opd_files[row.sequence] = o
        else:
            joined_opd_files[row.sequence].row_1ins = row

    for row in _3opd_rows:
        if row.sequence not in joined_opd_files:
            o = JoinedOpd()
            o.row_3opd = row
            joined_opd_files[row.sequence] = o
        else:
            joined_opd_files[row.sequence].row_3opd = row

    for row in _4orf_rows:
        if row.sequence not in joined_opd_files:
            o = JoinedOpd()
            o.row_4orf = row
            joined_opd_files[row.sequence] = o
        else:
            joined_opd_files[row.sequence].row_4orf = row

    for row in _5odx_rows:
        if row.sequence not in joined_opd_files:
            o = JoinedOpd()
            o.row_5odx.append(row)
            joined_opd_files[row.sequence] = o
        else:
            joined_opd_files[row.sequence].row_5odx.append(row)

    for row in _6oop_rows:
        if row.sequence not in joined_opd_files:
            o = JoinedOpd()
            o.row_6oop.append(row)
            joined_opd_files[row.sequence] = o
        else:
            joined_opd_files[row.sequence].row_6oop.append(row)

    for row in _11cht_rows:
        if row.sequence not in joined_opd_files:
            o = JoinedOpd()
            o.row_11cht = row
            joined_opd_files[row.sequence] = o
        else:
            joined_opd_files[row.sequence].row_11cht = row

    for row in _12cha_rows:
        if row.sequence not in joined_opd_files:
            o = JoinedOpd()
            o.row_12cha.append(row)
            joined_opd_files[row.sequence] = o
        else:
            joined_opd_files[row.sequence].row_12cha.append(row)

    for row in _16dru_rows:
        if row.sequence not in joined_opd_files:
            o = JoinedOpd()
            o.row_16dru.append(row)
            joined_opd_files[row.sequence] = o
        else:
            joined_opd_files[row.sequence].row_16dru.append(row)

    # https://www.nhso.go.th/page/hospital
    df_nhso = pd.read_excel("reference/nhso_2002-11-25_hospitals_information.xlsx")
    df_nhso["hcode"] = df_nhso["hcode"].str.strip()
    df_nhso.set_index("hcode", inplace=True)
    df_nhso.drop_duplicates(inplace=True)
    hcode_hname_dict = df_nhso.to_dict("index")
    # Organization
    unique_hosp_code = set([item.main_hospital_code for item in _1ins_rows if isinstance(item.main_hospital_code, str)])
    organizations = list[Organization]()
    for hospital_code in unique_hosp_code:
        org: Organization = Organization.construct()
        org.identifier = [Identifier(system="https://www.nhso.go.th", value=hospital_code)]
        try:
            org.name = String(hcode_hname_dict[hospital_code.strip()]["hname"])
        except KeyError:
            pass
        org.id = organization_id(hospital_code)
        organizations.append(org)
    organizations_dict = dict(zip(unique_hosp_code, organizations))

    # Patient
    with Manager() as manager:
        patients_dict: dict[str, Patient] = manager.dict()
        coverages: list[Coverage] = manager.list()
        accounts: list[Account] = manager.list()
        encounters: list[Encounter] = manager.list()
        service_requests: list[ServiceRequest] = manager.list()
        conditions: list[Condition] = manager.list()
        procedures: list[Procedure] = manager.list()
        claims: list[Claim] = manager.list()
        medication_dispenses: list[MedicationDispense] = manager.list()
        medication_requests: list[MedicationRequest] = manager.list()

        with Pool() as pool:
            for i in tqdm(pool.imap(partial(process_patient, organizations_dict, patients_dict), _2pat_rows, chunk_size), total=len(_2pat_rows), desc="Creating Patient Resource"):
                pass
            for i in tqdm(pool.imap(partial(process_matched_seq, organizations_dict, patients_dict, coverages,accounts,
                             encounters, service_requests, conditions, procedures, claims, medication_dispenses,
                             medication_requests),
                     joined_opd_files.items(), chunk_size), total=len(joined_opd_files),
                          desc="Creating Other Resources"):
                pass
            pool.close()

        # Save to file
        resource_dict = dict[str, Iterable[Resource]]()  # FileName (not used when export as a single file), Resource
        # The order of resource creation is important because of the parent reference need to be existed before being able to be referenced.
        resource_dict["1_Organization"] = organizations
        resource_dict["2_Patient"] = patients_dict.values()
        resource_dict["3_Coverage"] = coverages
        resource_dict["4_Account"] = accounts
        resource_dict["5_Encounter"] = encounters
        resource_dict["6_ServiceRequest"] = service_requests
        resource_dict["7_Condition"] = conditions
        resource_dict["8_Procedure"] = procedures
        resource_dict["9_Claim"] = claims
        resource_dict["10_MedicationDispense"] = medication_dispenses
        resource_dict["11_MedicationRequest"] = medication_requests

        save_bundle_to_file(create_bundle([resource for resources in resource_dict.values() for resource in resources]),
                            output_folder, "bundle_1.json")
