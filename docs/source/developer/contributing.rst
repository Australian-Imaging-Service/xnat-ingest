Contributing
============

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

Formatting and linting are enforced locally via ``pre-commit`` (installed above) —
`black <https://black.readthedocs.io/>`_ (88-column lines), `isort
<https://pycqa.github.io/isort/>`_, `flake8 <https://flake8.pycqa.org/>`_ and
``codespell`` all run automatically on commit once the hook is installed. The project
is also configured for ``mypy --strict`` (see ``pyproject.toml``), though it isn't
currently run as a separate CI step. Docstrings follow `NumPy style
<https://numpydoc.readthedocs.io/en/latest/format.html>`_, which is what
``sphinx.ext.napoleon``/``numpydoc`` render into the :doc:`API reference </api>`.

Building the docs
----------------------

.. code-block:: console

    $ cd docs
    $ make html

The built site is written to ``docs/build/html``.
