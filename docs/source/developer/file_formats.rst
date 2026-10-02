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

``group``'s ``--datatype`` option (see :doc:`/cli`) is a `FileFormats MIME-like
identifier <https://arcanaframework.github.io/fileformats/mime.html>`_ (or a ``|``-separated union of several) that says which types of file to
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
implementation for each format is registered with ``@extra_implementation`` in an
extras package (e.g. ``dicom_deidentify`` in ``fileformats-medimage-extras``).
``ImagingSession.deidentify`` (see :class:`~xnat_ingest.model.session.ImagingSession`)
only decides *which* recipe file applies to a given resource — the actual
deidentification logic is entirely delegated to whatever's registered for that
resource's type.

Typing the ``recipe`` argument
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*XNAT Ingest* never parses recipe files itself. It reads the annotation of the
``recipe`` argument of the registered implementation to work out which format(s) the
recipe file should be loaded as, loads it, and passes the loaded object in (see
:func:`~xnat_ingest.model.session.recipe_formats_for`). That annotation is therefore
part of the implementation's runtime behaviour, not just a hint for type checkers,
and must be one of:

* ``Loaded[<recipe format>]`` — the recipe file is loaded with
  ``<recipe format>(path).load()``, e.g. ``Loaded[DeidRecipe]``
* a union of ``Loaded[...]`` hints, in order of preference — the first format the
  recipe file matches is used. For DICOM this is
  ``Loaded[DeidRecipeX] | Loaded[DeidRecipe]``, so a recipe with its
  ``.transforms.py``/``.salt`` side-cars present is loaded together with them, and a
  bare recipe is loaded on its own
* ``None`` — the implementation doesn't take a recipe. A recipe file found in the
  spec directory for such a format is treated as an error rather than silently
  ignored

``| None`` can be appended to the first two (with a default of ``None``) to match the
base signature. For example:

.. code-block:: python

    @extra_implementation(MedicalImagingData.deidentify)
    def dicom_deidentify(
        dicom: DicomImage,
        out_dir: os.PathLike[str],
        recipe: Loaded[DeidRecipeX] | Loaded[DeidRecipe] | None = None,
        **kwargs: ty.Any,
    ) -> DicomImage:
        ...

If the argument is left unannotated, or annotated with a plain type (e.g.
``recipe: DeidRecipe`` or ``recipe: ty.Any``), *XNAT Ingest* can't tell what to load
the file as, so ``deidentify`` fails with a ``TypeError`` for every session with that
format rather than guessing. Things to watch out for:

* Annotate with the *file format* class (a ``FileSet`` subclass) wrapped in
  ``Loaded[...]``, not the in-memory type it loads into. ``Loaded[X]`` resolves to
  ``Annotated[X.loaded_type, LoadedMarker(X)]`` at runtime, which is what carries the
  format through to *XNAT Ingest*. Static type checkers just see ``Any``, so mypy
  won't catch a wrong annotation.
* String annotations (e.g. from ``from __future__ import annotations``) are fine, but
  only the ``recipe`` annotation is evaluated, in the implementation module's
  globals. The recipe format classes therefore have to be imported at module level,
  not only under ``if TYPE_CHECKING:``.
* The order of a union matters: put the most specific format (e.g. the one requiring
  side-cars) first. If a broader format comes first, it matches the recipe file
  before the specific one is tried, so the side-cars are never loaded.
* Where the implementation relies on something only the loaded type provides, check
  it at the top of the function with ``check_loaded(<recipe format>, recipe)``
  (as ``dicom_deidentify`` does), so a recipe passed in by other callers fails with a
  clear error.

Only formats flagged ``contains_phi = True`` (the ``MedicalImagingData`` default) are
run through ``deidentify`` at all — formats known not to carry patient information
(e.g. derived NIfTIs) set ``contains_phi = False`` and are just copied through
unchanged.
