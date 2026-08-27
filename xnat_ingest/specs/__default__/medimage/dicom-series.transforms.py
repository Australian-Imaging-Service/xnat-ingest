import os

TRANSFORMS = {
    "anon_birth_date": lambda ds: str(ds.get("PatientBirthDate", ""))[:4] + "0101",
    "anon_patient_name": lambda ds: str(ds.get("PatientID", "")),
    "anon_patient_id": lambda ds: f"{ds.get('PatientID', '')}-{ds.get('AcquisitionTime', '')}",
    "date_jitter": lambda _ds: int(os.environ.get("DEID_DATE_JITTER", "10")),
    "patient_comments": lambda ds: (
        f"Project={ds.get('ReferringPhysicianName', '')};"
        f"Subject={ds.get('PatientID', '')};"
        f"Session={ds.get('PatientID', '')}-{ds.get('AcquisitionTime', '')}"
    ),
}
