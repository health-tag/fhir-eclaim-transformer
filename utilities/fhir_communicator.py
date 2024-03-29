import traceback
import warnings
from collections.abc import Iterable
from datetime import datetime
from math import ceil
import re

import requests
from fhir.resources.R4B.fhirresourcemodel import FHIRResourceModel
from fhir.resources.R4B.fhirtypes import Code, BundleEntryType
from fhir.resources.R4B.bundle import Bundle

from configurations.fhir import max_patient_per_cycle

from configurations.fhir import base_fhir_url
from healthTag.models.result import BundleResult, EntryResult

actual_header = {
    "Content-Type": "application/json"
}


def create_bundle(fhir_resources: Iterable[FHIRResourceModel]):
    bundle = Bundle.construct(type=Code("batch"), entry=[BundleEntryType({
        "fullUrl": p.relative_path(),
        "resource": p,
        "request": {
            "method": "PUT",
            "url": p.relative_path()
        }
    }) for p in fhir_resources])
    return bundle


def send_singletype_bundle(fhir_resources: Iterable[FHIRResourceModel], processed_results: list[BundleResult] = None):
    resource_name = next(iter(fhir_resources)).resource_type
    print(f"⌚ Convert {resource_name} to FHIR Bundle entries at {datetime.now()} .")

    bundle = create_bundle(fhir_resources)

    print(f"Sending {len(fhir_resources)} entries of {resource_name} to FHIR server at {datetime.now()} .")
    if processed_results is not None:
        processed_results.append(post_bundle_to_fhir_server(bundle))


def bundle_cycler(fhir_resources: Iterable[FHIRResourceModel], processed_results: list[BundleResult]):
    next_item = None
    try:
        next_item = next(iter(fhir_resources))
    except StopIteration:
        pass
    if next_item is None:
        return
    resource_name = next_item.resource_type
    cycle = 0
    cycle_entries = list()
    print(f"⌚ Convert {resource_name} to FHIR Bundle entries at {datetime.now()}")
    entries = [e for e in fhir_resources]
    items_count = len(entries)
    print(f"🔗 There are {items_count} entries which will send in {ceil(items_count / max_patient_per_cycle)} cycle.")
    for i in range(0, items_count):
        cycle_entries.append(entries[i])
        if ((i > 0) and (i % max_patient_per_cycle == 0)) or (i + 1 == items_count):
            print(
                f"📡 Sending {resource_name} cycle {cycle + 1} of {ceil(items_count / max_patient_per_cycle)} at {datetime.now()}.")
            bundle = create_bundle(cycle_entries)
            processed_results.append(post_bundle_to_fhir_server(bundle))
            cycle = cycle + 1
            cycle_entries.clear()
    return


def post_bundle_to_fhir_server(bundle: Bundle) -> BundleResult:
    payload = bundle.json()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            res = requests.post(base_fhir_url, data=payload, headers=actual_header, verify=False)
            fhir_response = res.json()
            if fhir_response["resourceType"] == "OperationOutcome":
                bundle_result = BundleResult(591,
                                             [EntryResult(entry["resource"].resource_type, entry["fullUrl"],
                                                          "591 FHIR Server Error") for entry in
                                              bundle.entry])
                bundle_result.fhirErrorResponse = fhir_response
                return bundle_result
            results = [EntryResult(entry["resource"].resource_type, entry["fullUrl"]) for entry in bundle.entry]
            for i, entry in enumerate(fhir_response["entry"]):
                results[i].status = entry["response"]["status"]
                results[i].location = entry["response"]["location"] if "location" in entry["response"] else None
                if int(re.sub("[^0-9]", "", results[i].status)) >= 400:
                    results[i].fhirErrorResponse = entry["response"]

            return BundleResult(res.status_code, results)
        except Exception:
            print("-- SEND --")
            print(payload)
            print("-- EXCEPTION --")
            traceback.print_exc()
            print("-- END EXCEPTION --")
            return BundleResult(592,
                                [EntryResult(entry["resource"].resource_type, entry["fullUrl"],
                                             "592 Python Exception Occurs")
                                 for entry in
                                 bundle.entry])
