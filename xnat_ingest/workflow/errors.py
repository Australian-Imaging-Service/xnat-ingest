class WorkflowSpecError(ValueError):
    """A workflow YAML spec is malformed. The message is prefixed with a path
    pointing at where in the spec the problem is, e.g.
    "stages ('acemid-assign').args: ..." """
