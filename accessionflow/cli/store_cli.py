from .base import data_store_group, deprecated_alias

store_group = data_store_group(
    name="store",
    help="Deposits labelled accessions from a staging directory in a data store",
)

check_store_group = data_store_group(
    name="check-store",
    help=(
        "Checks what has been deposited in a data store against the staging directory "
        "it was deposited from, and logs all files that haven't been"
    ),
)

# The stages' names before they were renamed, kept so that existing deployments
# keep working while they move over
deprecated_alias("upload", store_group)
deprecated_alias("check-upload", check_store_group)
