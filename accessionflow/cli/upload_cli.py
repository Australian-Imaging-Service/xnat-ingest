from .base import store_group

upload_group = store_group(
    name="upload",
    help="Uploads the labelled sessions in a staging directory to a store",
)

check_upload_group = store_group(
    name="check-upload",
    help=(
        "Checks a staging directory against what has been uploaded to a store, "
        "and logs all files that haven't been"
    ),
)
