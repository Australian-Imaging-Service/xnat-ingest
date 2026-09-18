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


# XNAT labels a scan's DICOM catalog resource "DICOM" when the SOP class is on this
# list and "secondary" otherwise (CatalogBuilder.java, via SOPModel.isPrimaryImagingSOP
# and dicom-xnat-sop/src/main/resources/primary-sops.txt). It is the *only* input to
# that decision -- in particular ImageType is not consulted, so a DERIVED/SECONDARY
# image whose SOP class is on this list still goes to "DICOM".
#
# Taken verbatim from primary-sops.txt (XNAT 1.9.x), with the keywords resolved to the
# numeric UIDs the files actually carry, as elsewhere in this module.
PRIMARY_SOP_CLASS_UIDS = frozenset(
    (
        "1.2.840.10008.5.1.4.1.1.9.1.3",  # AmbulatoryECGWaveformStorage
        "1.2.840.10008.5.1.4.1.1.88.11",  # BasicTextSRStorage
        "1.2.840.10008.5.1.4.1.1.9.8.1",  # BodyPositionWaveformStorage
        "1.2.840.10008.5.1.4.1.1.13.1.3",  # BreastTomosynthesisImageStorage
        "1.2.840.10008.5.1.4.1.1.2",  # CTImageStorage
        "1.2.840.10008.5.1.4.1.1.9.3.1",  # CardiacElectrophysiologyWaveformStorage
        "1.2.840.10008.5.1.4.1.1.1",  # ComputedRadiographyImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.8",  # ConfocalMicroscopyImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.7",  # DermoscopyImageStorage
        "1.2.840.10008.5.1.4.1.1.1.3",  # DigitalIntraOralXRayImageStorageForPresentation
        "1.2.840.10008.5.1.4.1.1.1.3.1",  # DigitalIntraOralXRayImageStorageForProcessing
        "1.2.840.10008.5.1.4.1.1.1.2",  # DigitalMammographyXRayImageStorageForPresentation
        "1.2.840.10008.5.1.4.1.1.1.2.1",  # DigitalMammographyXRayImageStorageForProcessing
        "1.2.840.10008.5.1.4.1.1.1.1",  # DigitalXRayImageStorageForPresentation
        "1.2.840.10008.5.1.4.1.1.1.1.1",  # DigitalXRayImageStorageForProcessing
        "1.2.840.10008.5.1.4.1.1.9.7.2",  # ElectromyogramWaveformStorage
        "1.2.840.10008.5.1.4.1.1.9.7.3",  # ElectroocularWaveformStorage
        "1.2.840.10008.5.1.4.1.1.104.4",  # EncapsulatedObjStorage
        "1.2.840.10008.5.1.4.1.1.2.1",  # EnhancedCTImageStorage
        "1.2.840.10008.5.1.4.1.1.4.1",  # EnhancedMRImageStorage
        "1.2.840.10008.5.1.4.1.1.130",  # EnhancedPETImageStorage
        "1.2.840.10008.5.1.4.1.1.88.22",  # EnhancedSRStorage
        "1.2.840.10008.5.1.4.1.1.9.2.1",  # HemodynamicWaveformStorage
        "1.2.840.10008.5.1.4.1.1.4",  # MRImageStorage
        "1.2.840.10008.5.1.4.1.1.104.5",  # MaterialLibraryStorage
        "1.2.840.10008.5.1.4.1.1.91.1",  # MicroscopyBulkSimpleAnnotationStorage
        "1.2.840.10008.5.1.4.1.1.20",  # NuclearMedicineImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.5.2",  # OphthalmicPhotography16BitImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.5.1",  # OphthalmicPhotography8BitImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.5.4",  # OphthalmicTomographyImageStorage
        "1.2.840.10008.5.1.4.1.1.6.3",  # PhotoacousticImageStorage
        "1.2.840.10008.5.1.4.1.1.128",  # PositronEmissionTomographyImageStorage
        "1.2.840.10008.5.1.4.1.1.481.2",  # RTDoseStorage
        "1.2.840.10008.5.1.4.1.1.481.1",  # RTImageStorage
        "1.2.840.10008.5.1.4.1.1.9.6.1",  # RespiratoryWaveformStorage
        "1.2.840.10008.5.1.4.1.1.9.1.1",  # TwelveLeadECGWaveformStorage
        "1.2.840.10008.5.1.4.1.1.6.1",  # UltrasoundImageStorage
        "1.2.840.10008.5.1.4.1.1.3.1",  # UltrasoundMultiFrameImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.2",  # VLMicroscopicImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.6",  # VLWholeSlideMicroscopyImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.1.1",  # VideoEndoscopicImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.2.1",  # VideoMicroscopicImageStorage
        "1.2.840.10008.5.1.4.1.1.77.1.4.1",  # VideoPhotographicImageStorage
        "1.2.840.10008.5.1.4.1.1.13.1.1",  # XRay3DAngiographicImageStorage
        "1.2.840.10008.5.1.4.1.1.13.1.2",  # XRay3DCraniofacialImageStorage
        "1.2.840.10008.5.1.4.1.1.12.1",  # XRayAngiographicImageStorage
        "1.2.840.10008.5.1.4.1.1.12.2",  # XRayRadiofluoroscopicImageStorage
    )
)

#: The resource a scan's DICOM files are catalogued under
DICOM_RESOURCE_LABEL = "DICOM"
SECONDARY_RESOURCE_LABEL = "secondary"


def xnat_resource_label_from_sop_class(
    sop_class_uids: str | ty.Iterable[str] | None,
    separate_secondary: bool = True,
) -> str:
    """The resource label XNAT will catalogue the given DICOM files under.

    Matching XNAT means deciding this from the SOP class alone. Pre-creating a resource
    under a different name leaves the catalog XNAT generates (e.g. by pullDataFromHeaders)
    pointing at a directory the files aren't in, which breaks downloads.

    Parameters
    ----------
    sop_class_uids : str or Iterable[str], optional
        the SOP class UID(s) of the files being uploaded
    separate_secondary : bool
        whether the site separates secondary DICOM on archive, i.e. XNAT's
        'separateSecondaryDicomOnArchive' preference (added in 1.9.1, default true).
        When it is off, everything is catalogued under "DICOM" regardless of SOP class

    Returns
    -------
    str
        either "DICOM" or "secondary"
    """
    if not separate_secondary:
        return DICOM_RESOURCE_LABEL
    if not sop_class_uids:
        return SECONDARY_RESOURCE_LABEL
    if isinstance(sop_class_uids, str):
        sop_class_uids = [sop_class_uids]
    uids = [str(uid) for uid in sop_class_uids]
    if not uids:
        return SECONDARY_RESOURCE_LABEL
    # XNAT catalogues per file, so a scan holding any primary SOP class gets a "DICOM"
    # catalog; only a scan that is entirely non-primary is catalogued as "secondary"
    if any(uid in PRIMARY_SOP_CLASS_UIDS for uid in uids):
        return DICOM_RESOURCE_LABEL
    return SECONDARY_RESOURCE_LABEL
