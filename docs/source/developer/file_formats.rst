Adding support for new file types
=================================

*XNAT Ingest* doesn't have built-in knowledge of DICOM, NIfTI, or any other specific
format baked into its core logic. Instead, every file on disk is represented as a
typed ``FileSet`` from the `FileFormats
<https://arcanaframework.github.io/fileformats/>`_ package (e.g. ``DicomSeries``,
``NiftiGz``), and format-specific behaviour is provided by that type rather than by
``xnat_ingest`` itself. This is what lets the same pipeline code handle a wildly
different data — clinical DICOMs, derived NIfTIs, proprietary raw PET data — without
a format-specific branch for each one.

Grouping files into resources
----------------------------------

``group``'s ``--datatype`` option (see :doc:`/cli`) is a FileFormats MIME-like
identifier (or a ``|``-separated union of several) that says which types of file to
look for in the input paths at all. Within a matched session, ``--scan``/
``--resource`` (:class:`~xnat_ingest.helpers.arg_types.IDSpec`) then decide which
scan and resource each file belongs to, based on values read out of the file's own
metadata (e.g. DICOM ``SeriesNumber``, ``ImageType``) — see
:class:`~xnat_ingest.model.resource.ImagingResource` and
:class:`~xnat_ingest.model.scan.ImagingScan`.

Reading metadata
---------------------

Metadata is read via FileFormats' ``read_metadata`` "extra" — a method declared with
``@extra`` on ``FileSet`` itself (so it applies to every format), with the actual
implementation registered separately, per format, via ``@extra_implementation``. This
indirection is what lets ``xnat_ingest`` call ``fileset.metadata`` (or
``fileset.read_metadata()``) generically, regardless of what the underlying format
actually is, and is why adding support for a new file type is a matter of writing an
``extra_implementation`` for it in a `FileFormats extras package
<https://arcanaframework.github.io/fileformats/developer/extras.html>`_ (e.g.
``fileformats-medimage-extras``), rather than modifying ``xnat_ingest`` itself.

Deidentifying via extra implementations
--------------------------------------------

The ``deidentify`` command works the same way: ``MedicalImagingData.deidentify`` is
declared as an ``@extra`` (in ``fileformats-medimage``), and the concrete
implementation for DICOM lives in that package's own
``extra_implementation``-decorated function, keyed by type via
``functools.singledispatch``. ``ImagingSession.deidentify`` (see
:class:`~xnat_ingest.model.session.ImagingSession`) only decides *which* spec applies
to a given resource — the actual deidentification logic is entirely delegated to
whatever's registered for that resource's type. Note that as of writing, the
registered DICOM implementation ignores the ``spec`` argument's contents and always
strips a fixed, built-in set of tags — see :doc:`/how_to/deidentify` for the current
state of per-project spec customisation.

Only formats flagged ``contains_phi = True`` (the ``MedicalImagingData`` default) are
run through ``deidentify`` at all — formats known not to carry patient information
(e.g. derived NIfTIs) set ``contains_phi = False`` and are just copied through
unchanged.
