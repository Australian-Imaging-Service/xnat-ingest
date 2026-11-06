# Repository plugins

Each subpackage of `accessionflow.repositories` adds support for one
repository, e.g. `accessionflow/repositories/xnat`.

`accessionflow` and `accessionflow/repositories` are namespace packages, so
neither may contain an `__init__.py`. That lets plugins be installed from
separate distributions, including editable installs, and still be found here.
