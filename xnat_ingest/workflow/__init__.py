"""``xnat-ingest workflow`` - run a multi-stage group/assign/deidentify/upload
pipeline from a single YAML spec instead of chaining CLI invocations or
hand-maintained per-stage env-var blocks. See ``spec.py`` for the spec format and
``docs/source/how_to/workflow.rst`` for the full write-up.

Only ``load_spec``/``WorkflowSpecError`` are imported eagerly here - ``run_workflow``/
``serve_workflow`` pull in Prefect lazily (see ``runner.py``), so importing this
package (and so ``xnat-ingest workflow check``) never requires Prefect to be
installed.
"""

from .errors import WorkflowSpecError
from .spec import StageSpec, WorkflowSpec, XnatConnectionSpec, load_spec

__all__ = [
    "WorkflowSpecError",
    "WorkflowSpec",
    "StageSpec",
    "XnatConnectionSpec",
    "load_spec",
]
