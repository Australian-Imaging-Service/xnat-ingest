# Store plugins

Each subpackage of `accessionflow.stores` adds support for one kind of
data store, e.g. `accessionflow/stores/xnat` for XNAT and
`accessionflow/stores/tree` for a tree of label folders on a local or remote
file system.

`accessionflow` and `accessionflow/stores` are namespace packages, so
neither may contain an `__init__.py`. That lets plugins be installed from
separate distributions, including editable installs, and still be found here.

A plugin package

- defines `STORE`, an `accessionflow.model.store.Store` that
  names the store's label levels (e.g. XNAT's project, subject and
  session) and their default metadata fields, or leaves them to be named
  when labelling (`labels=None`, as the tree store does), and what the store
  calls an acquisition (e.g. XNAT's scan)
- may have a `cli` subpackage, imported when the CLI first looks up a
  store's subcommand, that registers the plugin's subcommands, e.g.
  `label xnat` by calling `accessionflow.cli.make_label_command(STORE)`,
  and `store xnat` with `@accessionflow.cli.store_group.command(name="xnat")`
