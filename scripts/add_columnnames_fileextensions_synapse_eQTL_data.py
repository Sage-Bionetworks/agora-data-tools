#!/usr/bin/env python3
"""Add a header row, and optionally a file extension, to files in a Synapse folder.

Every file in INPUT_FOLDER whose name matches FILE_PATTERN is downloaded,
given a header row of COLUMNS, and uploaded to OUTPUT_FOLDER. Synapse cannot
edit file contents in place, so the download is required. Local copies are
deleted after each upload.

Edit the constants below, then run from the repo root.
"""

import gzip
import re
import shutil
import tempfile
from pathlib import Path

import synapseclient
from synapseclient.models import File

INPUT_FOLDER = "syn78321269"
OUTPUT_FOLDER = "syn78768387"
FILE_PATTERN = r"eQTL_result_PsychAD_PFC_Astro+_chr[0-9]+_with_pvalue\.gz"
# Replaces the last suffix of the filename. Set to None to keep the name.
OUTPUT_FILE_EXTENSION = ".tsv.gz"
# Set to None to detect the delimiter from the first row.
DELIMITER = "\t"
COLUMNS = [
    "chr",
    "start",
    "end",
    "variant_id",
    "non_assessed_allele",
    "assessed_allele",
    "target_gene",
    "beta",
    "se",
    "z",
    "p_value",
]


def open_file(path: Path, mode: str):
    """Open path in binary mode, through gzip when the name ends in .gz."""
    return gzip.open(path, mode) if path.suffix == ".gz" else open(path, mode)


def add_header(source: Path, dest: Path) -> None:
    """Copy source to dest with the COLUMNS header row in front.

    The delimiter is DELIMITER, or the first candidate that splits the first
    row into len(COLUMNS) fields.
    """
    with open_file(source, "rb") as src:
        first = src.readline()
        candidates = [DELIMITER] if DELIMITER else ["\t", ",", ";", "|"]
        delimiter = next(
            (d for d in candidates if len(first.decode().split(d)) == len(COLUMNS)),
            None,
        )
        if delimiter is None:
            raise ValueError(
                f"{source.name}: first row does not have {len(COLUMNS)} columns"
            )
        with open_file(dest, "wb") as out:
            out.write((delimiter.join(COLUMNS) + "\n").encode())
            out.write(first)
            shutil.copyfileobj(src, out)


def main() -> None:
    """Add the header to every matching file and upload it to OUTPUT_FOLDER."""
    syn = synapseclient.login()
    for child in syn.getChildren(INPUT_FOLDER, includeTypes=["file"]):
        name = child["name"]
        if not re.fullmatch(FILE_PATTERN, name):
            continue
        new_name = name
        if OUTPUT_FILE_EXTENSION:
            new_name = Path(name).stem + OUTPUT_FILE_EXTENSION
        with tempfile.TemporaryDirectory() as tmp:
            downloaded = File(id=child["id"], path=tmp).get(synapse_client=syn)
            dest = Path(tmp) / "out" / new_name
            dest.parent.mkdir()
            add_header(Path(downloaded.path), dest)
            File(path=str(dest), parent_id=OUTPUT_FOLDER).store(synapse_client=syn)
        print(f"{name} -> {new_name}")


if __name__ == "__main__":
    main()
