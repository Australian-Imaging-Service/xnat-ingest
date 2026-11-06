# Store plugins

Each subpackage of `accessionflow.stores` adds support for one
store, e.g. `accessionflow/stores/xnat`.

`accessionflow` and `accessionflow/stores` are namespace packages, so
neither may contain an `__init__.py`. That lets plugins be installed from
separate distributions, including editable installs, and still be found here.

A plugin package

- defines `STORE`, an `accessionflow.model.store_config.Store` that
  names the store's label levels (e.g. XNAT's project, subject and
  session), their default metadata fields, and what the store calls an
  acquisition (e.g. XNAT's scan)
- may have a `cli` subpackage, imported when the CLI first looks up a
  store's subcommand, that registers the plugin's subcommands, e.g.
  `label xnat` by calling `accessionflow.cli.make_label_command(STORE)`,
  and `store xnat` with `@accessionflow.cli.store_group.command(name="xnat")`
