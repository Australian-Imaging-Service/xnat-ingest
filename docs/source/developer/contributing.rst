Contribution guide
==================

Contributions of any size are welcome — fixing a typo, improving an example, reporting
a bug, reviewing a pull request, or adding a whole new pipeline stage are all useful.
You don't need to be an expert in XNAT, DICOM, or Python packaging to contribute
something worthwhile; a lot of the most useful contributions to scientific software
come from people hitting a rough edge as a user and fixing it, or writing down the
question they had to ask so the next person doesn't have to.

If you're new to contributing to open-source or scientific software generally, guides
like `The Turing Way's guide to collaboration
<https://the-turing-way.netlify.app/collaboration/collaboration>`_ and the
`Brainhack <https://brainhack.org/>`_ community's approach to open, welcoming
collaborative projects cover the general principles well, and they apply here too:
start small, ask questions early rather than guessing, open an issue before a large
pull request so the approach can be discussed up front, and treat review comments as
a normal, collaborative part of getting a change over the line rather than a judgement
on the contribution.


Setting up a development environment
-----------------------------------------

Clone the repository and install it in editable mode with the ``test``, ``dev`` and
``docs`` extras:

.. code-block:: console

    $ git clone https://github.com/Australian-Imaging-Service/xnat-ingest.git
    $ cd xnat-ingest
    $ python3 -m pip install -e .[test,dev,docs]
    $ pre-commit install

Some tests spin up real (disposable) service containers via `xnat4tests
<https://github.com/australian-imaging-service/xnat4tests>`_ and an Orthanc instance,
so a working Docker installation is required to run the full test suite.


Running the tests
----------------------

.. code-block:: console

    $ pytest .

CI runs the same suite (see the ``ci-cd.yml`` workflow) across supported Python
versions, with an Orthanc container available as a service for the tests that need
one.


Code style
--------------

Formatting and linting are enforced via ``pre-commit`` — `black
<https://black.readthedocs.io/>`_ (88-column lines), `isort
<https://pycqa.github.io/isort/>`_, `flake8 <https://flake8.pycqa.org/>`_ and
``codespell`` all run automatically against the files you've changed whenever you
``git commit``, once ``pre-commit install`` (above) has been run once in your clone.
CI itself only runs the test suite, not a separate lint step, so this is the only
real enforcement of style in this project — please make sure it's installed and
passing before opening a pull request.

To check your whole working tree rather than just what's staged (useful after
installing ``pre-commit`` for the first time, or before a big PR):

.. code-block:: console

    $ pre-commit run --all-files

The project is also configured for ``mypy --strict`` (see ``pyproject.toml``), though
it isn't currently run as a separate CI step or pre-commit hook. Docstrings follow
`NumPy style <https://numpydoc.readthedocs.io/en/latest/format.html>`_, which is what
``sphinx.ext.napoleon``/``numpydoc`` render into the :doc:`API reference </api>`.


Building the docs
----------------------

.. code-block:: console

    $ cd docs
    $ make html

The built site is written to ``docs/build/html``.


Adding yourself as a contributor
-------------------------------------

If your change is more than a trivial fix, add yourself to the ``authors`` list in
``pyproject.toml`` as part of the same pull request:

.. code-block:: toml

    authors = [
        { name = "Thomas G. Close", email = "thomas.close@sydney.edu.au" },
        { name = "Your Name", email = "your.email@example.com" },
    ]

This is what ends up in the package metadata and on PyPI, so please add yourself
rather than waiting to be asked or added by someone else.


Getting help
----------------

If something in these docs doesn't work, or you're not sure where to start, open an
issue on `GitHub <https://github.com/Australian-Imaging-Service/xnat-ingest/issues>`_
— asking a question is a legitimate contribution in its own right, since it usually
points at a gap in the documentation that's worth fixing for the next person too.
