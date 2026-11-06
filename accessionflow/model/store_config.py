"""Configuration that a repository plugin provides to the repository-neutral stages.

Each repository plugin is a subpackage of the ``accessionflow.repositories``
namespace package, e.g. ``accessionflow.repositories.xnat``, which defines a
``REPOSITORY`` attribute holding a :class:`Repository`. Its optional ``cli``
subpackage registers the plugin's subcommands, e.g. ``label xnat``, when it is
imported (see :func:`load_repository_plugins`).
"""

import importlib
import pkgutil
import types
import typing as ty
import warnings

import attrs

PLUGINS_PACKAGE = "accessionflow.repositories"

# The Accession model holds exactly these three IDs, in this order, until it holds a
# general mapping of labels
MODEL_LABEL_LEVELS = 3


@attrs.frozen
class LabelLevel:
    """A level of a repository's hierarchy that a session is labelled with, or the
    repository's name for an acquisition within the session.

    Parameters
    ----------
    name : str
        the repository's name for the level, which names its command-line option
        (e.g. 'project' gives '--project') and environment variable
        (e.g. 'ACFLOW_PROJECT')
    default : str, optional
        the metadata field, or a format string over several fields, that the label is
        extracted from by default
    description : str
        what the label is in the repository's terms, for the help text, e.g.
        'XNAT project ID'
    """

    name: str
    default: str | None = None
    description: str = ""

    @property
    def option_name(self) -> str:
        return "--" + self.name.replace("_", "-")

    def envvar(self, prefix: str) -> str:
        return prefix + self.name.upper().replace("-", "_")


def _check_labels(
    repository: "Repository", attribute: ty.Any, labels: tuple[LabelLevel, ...]
) -> None:
    if len(labels) != MODEL_LABEL_LEVELS:
        raise ValueError(
            f"Repository '{repository.name}' has {len(labels)} label levels, but "
            f"sessions can only be labelled with exactly {MODEL_LABEL_LEVELS} for now"
        )
    names = [level.name for level in labels]
    if repository.acquisition is not None:
        names.append(repository.acquisition.name)
    if len(set(names)) != len(names):
        raise ValueError(
            f"Repository '{repository.name}' has repeated label names: {names}"
        )


@attrs.frozen
class Repository:
    """How sessions are labelled for, and named within, a repository.

    Parameters
    ----------
    name : str
        the plugin name, used as the subcommand name, e.g. 'xnat' for 'label xnat'
    title : str
        the name of the repository for help text, e.g. 'XNAT'
    labels : tuple[LabelLevel, ...]
        the levels of the repository's hierarchy, from the top. A labelled session is
        saved in a folder named by its labels joined by '.', in this order, and
        'deidentify' selects recipes by the first label
    acquisition : LabelLevel, optional
        the repository's name for an acquisition, and the metadata field to describe
        each one by, e.g. XNAT's 'scan' described by 'SeriesDescription'. None if the
        repository does not describe acquisitions
    multiple_resources : bool
        whether an acquisition can hold more than one resource (e.g. the DICOM files
        and a NIfTI conversion of them), as XNAT scans can. If False, each acquisition
        must hold exactly one, which 'label' checks
    envvar_prefix : str
        the prefix of the environment variables that the options can be set by
    """

    name: str
    title: str
    labels: tuple[LabelLevel, ...] = attrs.field(validator=_check_labels)
    acquisition: LabelLevel | None = None
    multiple_resources: bool = True
    envvar_prefix: str = "ACFLOW_"


def repository_plugins() -> list[str]:
    """The names of the repository plugins that are installed"""
    package = importlib.import_module(PLUGINS_PACKAGE)
    return sorted(
        info.name for info in pkgutil.iter_modules(package.__path__) if info.ispkg
    )


def get_repository(name: str) -> Repository:
    """The configuration of the named repository plugin"""
    try:
        module = importlib.import_module(f"{PLUGINS_PACKAGE}.{name}")
    except ModuleNotFoundError as e:
        if e.name != f"{PLUGINS_PACKAGE}.{name}":
            raise
        raise ValueError(
            f"No repository plugin named '{name}' is installed (installed: "
            f"{', '.join(repository_plugins()) or 'none'})"
        ) from None
    return ty.cast(Repository, module.REPOSITORY)


_loaded_plugins: dict[str, types.ModuleType | None] | None = None


def load_repository_plugins() -> dict[str, types.ModuleType | None]:
    """Import the 'cli' subpackage of each repository plugin once, so that the
    plugins can register their subcommands.

    A plugin that fails to import is skipped with a warning, so that one broken
    plugin does not stop the commands of the others from running.

    Returns
    -------
    dict[str, types.ModuleType | None]
        the imported 'cli' subpackages by plugin name, None for plugins without one
    """
    global _loaded_plugins
    if _loaded_plugins is None:
        _loaded_plugins = {}
        for name in repository_plugins():
            module_name = f"{PLUGINS_PACKAGE}.{name}.cli"
            try:
                _loaded_plugins[name] = importlib.import_module(module_name)
            except ModuleNotFoundError as e:
                if e.name != module_name:
                    warnings.warn(
                        f"Could not load the '{name}' repository plugin: {e}",
                        stacklevel=2,
                    )
                else:
                    _loaded_plugins[name] = None
            except Exception as e:  # noqa: BLE001 - see docstring
                warnings.warn(
                    f"Could not load the '{name}' repository plugin: {e}",
                    stacklevel=2,
                )
    return _loaded_plugins
