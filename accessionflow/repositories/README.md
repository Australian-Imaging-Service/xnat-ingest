# Repository plugins

Each subpackage of `accessionflow.repositories` adds support for one
repository, e.g. `accessionflow/repositories/xnat`.

`accessionflow` and `accessionflow/repositories` are namespace packages, so
neither may contain an `__init__.py`. That lets plugins be installed from
separate distributions, including editable installs, and still be found here.

A plugin package

- defines `REPOSITORY`, an `accessionflow.model.repository.Repository` that
  names the repository's label levels (e.g. XNAT's project, subject and
  session), their default metadata fields, and what the repository calls an
  acquisition (e.g. XNAT's scan)
- may have a `cli` subpackage, imported when the CLI first looks up a
  repository's subcommand, that registers the plugin's subcommands, e.g.
  `label xnat` by calling `accessionflow.cli.make_label_command(REPOSITORY)`,
  and `upload xnat` with `@accessionflow.cli.upload_group.command(name="xnat")`
