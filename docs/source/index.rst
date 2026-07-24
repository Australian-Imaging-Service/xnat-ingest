.. _home:

XNAT Ingest
===========
.. image:: https://github.com/australian-imaging-service/xnat-ingest/actions/workflows/ci-cd.yml/badge.svg
   :target: https://github.com/australian-imaging-service/xnat-ingest/actions/workflows/ci-cd.yml
.. image:: https://codecov.io/gh/australian-imaging-service/xnat-ingest/branch/main/graph/badge.svg?token=UIS0OGPST7
   :target: https://codecov.io/gh/australian-imaging-service/xnat-ingest
.. image:: https://img.shields.io/pypi/v/xnat-ingest.svg
   :target: https://pypi.python.org/pypi/xnat-ingest/
   :alt: Latest Version
.. image:: https://img.shields.io/github/stars/australian-imaging-service/xnat-ingest?label=GitHub
   :alt: GitHub stars
   :target: https://github.com/australian-imaging-service/xnat-ingest


*XNAT Ingest* is a toolkit for capturing data from instruments and uploading them to an XNAT
instance.

Data files coming straight off a scanner or other instrument aren't organised the way XNAT
expects, and — particularly on clinical scanners — often still carry patient-identifying
information that needs stripping before they leave clinical control. *XNAT Ingest*
contains tools for each step of this process:

* grouping DICOM and non-DICOM files into sessions, scans and resources
* determining which XNAT project each belongs to and assigning appropriate subject and session labels
* optionally de-identifies files using flexible, file-format-specific methods
* uploads the sorted data into the specified XNAT instance.

Each of these is a separate step that can be chained together and left
running continuously as a service, watching for new files as they arrive.

See :doc:`quick_start` for a hands-on walkthrough using synthetic sample data or go through the
different sections in more detail using the links below

* :ref:`Basic ingest workflow` — group, assign and upload files to XNAT
* :ref:`Associate files without relevant metadata` — link in files by filename pattern instead
* :ref:`Deidentification` — strip patient-identifying data first
* :ref:`Deployment tips` — run the pipeline continuously via Docker/Kubernetes
* :doc:`cli` — full command-line reference


Installation
------------

The recommended way to run *XNAT Ingest*, particularly for a long-running node, is the
published Docker image, which bundles the CLI as its entrypoint along with all of its
external dependencies (e.g. dcm2niix, MRtrix3):

.. code-block:: console

    $ docker run ghcr.io/australian-imaging-service/xnat-ingest --help

See :ref:`Deployment tips` for how this fits into Docker Compose
or Kubernetes.

Alternatively, *XNAT Ingest* can be installed for Python >=3.11 using *pip*:

.. code-block:: console

    $ python3 -m pip install xnat-ingest


License
-------

This work is licensed under the
`Apache License, Version 2.0 <http://www.apache.org/licenses/LICENSE-2.0>`_


.. toctree::
    :maxdepth: 2
    :hidden:

    quick_start

.. toctree::
    :maxdepth: 2
    :caption: How-to
    :hidden:

    how_to/pipeline
    how_to/deidentify
    how_to/associate
    how_to/deploying

.. toctree::
    :maxdepth: 2
    :caption: Developer
    :hidden:

    developer/contributing
    developer/file_formats
    developer/data_model

.. toctree::
    :maxdepth: 2
    :caption: Reference
    :hidden:

    cli
    api
