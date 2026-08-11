#!/usr/bin/env python3

"""
Upload dermoscopy images and metadata to XNAT.

Requirements:
    pip install pandas requests

Usage:
    python dermoscopy_data_upload.py input.csv
"""

from pathlib import Path
import argparse
# import logging
import re
import zipfile

import pandas as pd
import requests

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

XNAT_URL = "https://your-xnat-url"
USERNAME = "your-xnat-username"
PASSWORD = "your-xnat-password"
PROJECT_ID = "your-project-id"

OUTPUT_DIR = Path("per_patient_csv_files")
ZIP_NAME = "dermoscopy_images.zip"

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(levelname)s - %(message)s"
# )

# ---------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------


def clean_hyperlink(value):
    """
    Convert an Excel HYPERLINK formula to a normal path.

    Example:
        =HYPERLINK("folder/image.jpg")
        -> folder/image.jpg
    """

    if pd.isna(value):
        return None

    value = str(value).strip()

    m = re.search(r'HYPERLINK\("([^"]+)"', value, re.IGNORECASE)

    if m:
        return m.group(1)

    return value


def create_patient_csvs(df):
    OUTPUT_DIR.mkdir(exist_ok=True)

    remove_columns = [
        "LastName",
        "FirstName",
        "DOB",
        "PatientNotes"
    ]

    keep = df.drop(remove_columns, axis="columns")

    for mrn, group in df.groupby("PatientMRN"):

        outfile = OUTPUT_DIR / f"patient_{mrn}.csv"

        group[keep].to_csv(outfile, index=False)

        # logging.info("Created %s", outfile)


def find_matching_images(image_names):
    """
    Search recursively for image files.
    """

    matches = []

    exts = {".jpg", ".jpeg", ".png"}

    for file in Path(".").rglob("*"):

        if file.suffix.lower() not in exts:
            continue

        if file.name in image_names:
            matches.append(file)

    return matches


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("csv_file")

    args = parser.parse_args()

    csv_file = Path(args.csv_file)

    if not csv_file.exists():
        raise FileNotFoundError(csv_file)

    # logging.info("Reading CSV...")

    df = pd.read_csv(csv_file)

    patient_mrns = sorted(df.PatientMRN.dropna().unique())

    # logging.info("Patients: %s", patient_mrns)

    # create_patient_csvs(df)

    df["ImagePath"] = df["ImagePath"].apply(clean_hyperlink)

    image_names = (
        df["ImagePath"]
        .dropna()
        .apply(lambda x: Path(x).name)
        .unique()
    )

    # logging.info("Searching for images...")

    images = find_matching_images(set(image_names))

    if not images:

        # logging.error("No images found.")

        return

    # logging.info("Upload complete.")


if __name__ == "__main__":
    main()