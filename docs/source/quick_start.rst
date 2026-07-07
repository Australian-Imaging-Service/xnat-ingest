Quick start
===========

This walks through getting a single DICOM series from a directory on disk into an
XNAT project, using a synthetic sample scan so you don't need real patient data or an
existing XNAT server to follow along. It uses the same three commands you'd use in
production — see :doc:`/how_to/pipeline` for the full pipeline (including
de-identification) and :doc:`cli` for every option.


1. Install
----------

.. code-block:: console

    $ python3 -m pip install xnat-ingest


2. Get an XNAT to upload to
----------------------------

If you already have an XNAT instance and a project to upload to, skip ahead to step 3.

Otherwise, `xnat4tests <https://github.com/Australian-Imaging-Service/xnat4tests>`_ can
launch a disposable, fully working XNAT instance in a Docker container, which is handy
for trying things out without touching a real server:

.. code-block:: console

    $ python3 -m pip install xnat4tests
    $ xnat4tests start

This starts an XNAT instance at ``http://localhost:8080`` with the default credentials
``admin``/``admin``. Either way, log in to your instance's web interface and create a
project (e.g. called ``MYPROJECT``) to upload to — *XNAT Ingest* stages and de-identifies
data, but doesn't create new projects on the server itself.


3. Get some sample data
--------------------------

`medimages4tests <https://github.com/Australian-Imaging-Service/medimages4tests>`_
generates synthetic DICOM series for exactly this kind of test-drive:

.. code-block:: console

    $ python3 -m pip install medimages4tests
    $ python3 -c "
    from medimages4tests.dummy.dicom.mri.t1w.siemens.skyra.syngo_d13c import get_image
    print(get_image())
    "

This prints the path to a directory of DICOM files for a synthetic T1-weighted MRI
scan (downloaded and cached the first time it's run). Substitute your own directory of
DICOM files here instead if you have some handy.


4. Group, assign and upload
------------------------------

Group the files into scans/resources:

.. code-block:: console

    $ xnat-ingest group /path/to/dicom/series /tmp/xnat-ingest-quickstart/grouped

Assign the session to your project. The sample data from step 3 doesn't have
``StudyComments``/``AccessionNumber`` populated (the fields ``assign`` uses by default
for project/session), so we fix the project explicitly and use ``StudyInstanceUID`` for
the session instead — real scanner data may populate these differently, or the same
way, depending on your site (see :ref:`2. Assign project/subject/session IDs`):

.. code-block:: console

    $ xnat-ingest assign /tmp/xnat-ingest-quickstart/grouped /tmp/xnat-ingest-quickstart/assigned \
        --constant-project-id MYPROJECT --session StudyInstanceUID

Then upload to XNAT

.. code-block:: console

    $ xnat-ingest upload /tmp/xnat-ingest-quickstart/assigned http://localhost:8080 \
        --user admin --password admin


5. Check the result
----------------------

Log in to the XNAT web interface and open ``MYPROJECT`` — you should see a new
subject and session containing the uploaded scan. From here, see :doc:`/how_to/deidentify`
for adding de-identification to the pipeline, and :doc:`cli`/:doc:`api` for the full set
of options.
