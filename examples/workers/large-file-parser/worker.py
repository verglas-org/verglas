import gzip
import json
import os
import shutil
import sys
import tempfile
import urllib.request

import polars as pl


def parse_rows():
    source = os.environ["INPUT_URL"]
    with tempfile.TemporaryDirectory() as directory:
        compressed = os.path.join(directory, "input.csv.gz")
        expanded = os.path.join(directory, "input.csv")
        urllib.request.urlretrieve(source, compressed)
        with gzip.open(compressed, "rb") as input_file:
            with open(expanded, "wb") as output_file:
                shutil.copyfileobj(input_file, output_file, length=1024 * 1024)
        return pl.scan_csv(expanded).select(pl.len()).collect().item()


def main():
    try:
        result = {"rows": parse_rows(), "error": None}
        status = 0
    except Exception as error:
        result = {"rows": 0, "error": str(error)}
        status = 1
    with open(os.environ["RESULT_PATH"], "w", encoding="utf-8") as output:
        json.dump(result, output)
    return status


if __name__ == "__main__":
    sys.exit(main())
