import typing as ty

# Numeric UIDs avoid coupling the classifier to pydicom's keyword coverage.
# This mirrors XNAT 1.9.3's series-scans.properties.
SOP_CLASS_UIDS_BY_SCAN_TYPE = {
    "petScanData": {
        "1.2.840.10008.5.1.4.1.1.128",
        "1.2.840.10008.5.1.4.1.1.130",
    },
    "mrScanData": {
        "1.2.840.10008.5.1.4.1.1.4",
        "1.2.840.10008.5.1.4.1.1.4.1",
        "1.2.840.10008.5.1.4.1.1.4.3",
    },
    "ctScanData": {
        "1.2.840.10008.5.1.4.1.1.2",
        "1.2.840.10008.5.1.4.1.1.2.1",
    },
    "xaScanData": {
        "1.2.840.10008.5.1.4.1.1.12.1",
        "1.2.840.10008.5.1.4.1.1.13.1.1",
    },
    "usScanData": {
        "1.2.840.10008.5.1.4.1.1.6.1",
        "1.2.840.10008.5.1.4.1.1.3.1",
    },
    "rtImageScanData": {
        "1.2.840.10008.5.1.4.1.1.481.1",
        "1.2.840.10008.5.1.4.1.1.481.2",
        "1.2.840.10008.5.1.4.1.1.481.3",
        "1.2.840.10008.5.1.4.1.1.481.4",
        "1.2.840.10008.5.1.4.1.1.481.5",
        "1.2.840.10008.5.1.4.1.1.481.6",
        "1.2.840.10008.5.1.4.1.1.481.7",
        "1.2.840.10008.5.1.4.1.1.481.8",
        "1.2.840.10008.5.1.4.1.1.481.9",
    },
    "crScanData": {"1.2.840.10008.5.1.4.1.1.1"},
    "optScanData": {"1.2.840.10008.5.1.4.1.1.77.1.5.4"},
    "dx3DCraniofacialScanData": {"1.2.840.10008.5.1.4.1.1.13.1.2"},
    "ecgScanData": {
        "1.2.840.10008.5.1.4.1.1.9.1.1",
        "1.2.840.10008.5.1.4.1.1.9.1.3",
    },
    "epsScanData": {"1.2.840.10008.5.1.4.1.1.9.3.1"},
    "esvScanData": {"1.2.840.10008.5.1.4.1.1.77.1.1.1"},
    "gmvScanData": {"1.2.840.10008.5.1.4.1.1.77.1.2.1"},
    "hdScanData": {"1.2.840.10008.5.1.4.1.1.9.2.1"},
    "ioScanData": {
        "1.2.840.10008.5.1.4.1.1.1.3",
        "1.2.840.10008.5.1.4.1.1.1.3.1",
    },
    "mgScanData": {
        "1.2.840.10008.5.1.4.1.1.1.2",
        "1.2.840.10008.5.1.4.1.1.1.2.1",
    },
    "dxScanData": {
        "1.2.840.10008.5.1.4.1.1.1.1",
        "1.2.840.10008.5.1.4.1.1.1.1.1",
    },
    "nmScanData": {"1.2.840.10008.5.1.4.1.1.20"},
    "opScanData": {
        "1.2.840.10008.5.1.4.1.1.77.1.5.1",
        "1.2.840.10008.5.1.4.1.1.77.1.5.2",
    },
    "rfScanData": {"1.2.840.10008.5.1.4.1.1.12.2"},
    "xcvScanData": {"1.2.840.10008.5.1.4.1.1.77.1.4.1"},
    "scScanData": {"1.2.840.10008.5.1.4.1.1.7"},
    "segScanData": {"1.2.840.10008.5.1.4.1.1.66.4"},
    "srScanData": {
        "1.2.840.10008.5.1.4.1.1.88.11",
        "1.2.840.10008.5.1.4.1.1.88.22",
        "1.2.840.10008.5.1.4.1.1.88.33",
        "1.2.840.10008.5.1.4.1.1.88.50",
        "1.2.840.10008.5.1.4.1.1.88.65",
        "1.2.840.10008.5.1.4.1.1.88.67",
        "1.2.840.10008.5.1.4.1.1.88.69",
        "1.2.840.10008.5.1.4.1.1.88.70",
    },
    "otherDicomScanData": {"1.3.12.2.1107.5.9.1"},
    "smScanData": {"1.2.840.10008.5.1.4.1.1.77.1.6"},
    "gmScanData": {"1.2.840.10008.5.1.4.1.1.77.1.2"},
    "dmsScanData": {"1.2.840.10008.5.1.4.1.1.77.1.7"},
    "cfmScanData": {"1.2.840.10008.5.1.4.1.1.77.1.8"},
    "paScanData": {"1.2.840.10008.5.1.4.1.1.6.3"},
    "annScanData": {"1.2.840.10008.5.1.4.1.1.91.1"},
    "emgScanData": {"1.2.840.10008.5.1.4.1.1.9.7.2"},
    "eogScanData": {"1.2.840.10008.5.1.4.1.1.9.7.3"},
    "respScanData": {
        "1.2.840.10008.5.1.4.1.1.9.6.1",
        "1.2.840.10008.5.1.4.1.1.9.6.2",
    },
    "posScanData": {"1.2.840.10008.5.1.4.1.1.9.8.1"},
    "objScanData": {"1.2.840.10008.5.1.4.1.1.104.4"},
    "mtlScanData": {"1.2.840.10008.5.1.4.1.1.104.5"},
    "eegScanData": {"1.2.840.10008.5.1.4.1.1.9.7.1"},
}

# XNAT uses this order when a series contains more than one SOP class, types
# omitted by XNAT's precedence list fall back to otherDicomScanData.
XNAT_SCAN_TYPE_PRECEDENCE = (
    "petScanData",
    "mrScanData",
    "ctScanData",
    "xaScanData",
    "usScanData",
    "rtImageScanData",
    "crScanData",
    "optScanData",
    "mgScanData",
    "dxScanData",
    "nmScanData",
    "srScanData",
    "segScanData",
    "scScanData",
    "smScanData",
    "gmScanData",
    "rfScanData",
    "esScanData",
    "rgScanData",
    "dmsScanData",
    "cfmScanData",
    "paScanData",
    "ecgScanData",
    "annScanData",
    "emgScanData",
    "eogScanData",
    "respScanData",
    "posScanData",
    "objScanData",
    "mtlScanData",
    "eegScanData",
    "otherDicomScanData",
)

SCAN_TYPE_BY_SOP_CLASS_UID = {
    uid: scan_type
    for scan_type, sop_class_uids in SOP_CLASS_UIDS_BY_SCAN_TYPE.items()
    for uid in sop_class_uids
}


def xnat_scan_type_from_sop_class(
    sop_class_uids: str | ty.Iterable[str] | None,
) -> str:
    """Match XNAT's SOP-class-based scan type selection."""
    if not sop_class_uids:
        return "otherDicomScanData"
    if isinstance(sop_class_uids, str):
        sop_class_uids = [sop_class_uids]
    scan_types = {SCAN_TYPE_BY_SOP_CLASS_UID.get(str(uid)) for uid in sop_class_uids}
    return next(
        (
            scan_type
            for scan_type in XNAT_SCAN_TYPE_PRECEDENCE
            if scan_type in scan_types
        ),
        "otherDicomScanData",
    )
