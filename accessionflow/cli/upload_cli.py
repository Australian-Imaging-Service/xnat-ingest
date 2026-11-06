from .base import repository_group

upload_group = repository_group(
    name="upload",
    help="Uploads the labelled sessions in a staging directory to a repository",
)

check_upload_group = repository_group(
    name="check-upload",
    help=(
        "Checks a staging directory against what has been uploaded to a repository, "
        "and logs all files that haven't been"
    ),
)
