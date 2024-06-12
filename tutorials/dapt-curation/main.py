# Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import shutil
from typing import Any, Optional, Tuple

from downloaders import (
    download_github_sources,
    download_pdf_sources,
    download_wikipedia_sources,
)
from utils import (
    FilterFilesBasedOnLines_code,
    FilterFilesBasedOnLines_txt,
    clean_and_unify,
    dedupe,
    filter_code,
    filter_code_dataset,
    filter_dataset,
    redact,
)

import nemo_curator as nc
from nemo_curator import ScoreFilter, Sequential
from nemo_curator.datasets import DocumentDataset
from nemo_curator.filters import RepeatingTopNGramsFilter, WordCountFilter
from nemo_curator.modifiers.pii_modifier import PiiModifier
from nemo_curator.modifiers.unicode_reformatter import UnicodeReformatter
from nemo_curator.modules import ExactDuplicates
from nemo_curator.modules.modify import Modify
from nemo_curator.utils.distributed_utils import get_client
from nemo_curator.utils.file_utils import (
    get_all_files_paths_under,
    separate_by_metadata,
)
from nemo_curator.utils.script_utils import add_distributed_args, parse_client_args

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR_PATH, "data")

#
# TODO
# Add instructions for apt install tessaract-ocr
#


def download_sources(
    wikipedia_limit: Optional[int] = None,
    github_limit: Optional[int] = None,
    pdf_limit: Optional[int] = None,
) -> Tuple[str, str]:
    """
    Downloads all the dataset sources and converts them to the JSONL format.

    Returns:
        tuple: the list of text files and the list of code files.
    """

    wikipedia_dir = download_wikipedia_sources(
        "sources/wikipedia_urls.jsonl", limit=wikipedia_limit
    )
    github_dir = download_github_sources(
        "sources/github_repos.jsonl", limit=github_limit
    )
    pdf_dir = download_pdf_sources("sources/arxiv_urls.jsonl", limit=pdf_limit)

    wiki_files = get_all_files_paths_under(wikipedia_dir)
    code_files = get_all_files_paths_under(github_dir)
    pdf_files = get_all_files_paths_under(pdf_dir)

    text_files = wiki_files + pdf_files

    return text_files, code_files


def run_curation_pipeline(args: Any, text_files: str, code_files: str) -> None:
    """
    Run the curation pipeline on the TinyStories dataset.

    Args:
        args (Any): Command-line arguments.
        jsonl_dir (str): Directory path where the JSONL files are stored.
    """
    print("Running the curation pipeline...")
    # Initialize the Dask cluster.
    client = get_client(**parse_client_args(args))

    # Define data curation steps for text and pdf files
    curation_steps_text = Sequential(
        [
            dedupe,
            FilterFilesBasedOnLines_txt,
            filter_dataset,
            clean_and_unify,
        ]
    )

    # Define data curation steps for code files
    curation_steps_code = Sequential(
        [dedupe, FilterFilesBasedOnLines_code, filter_code, clean_and_unify, redact]
    )

    orig_dataset_text = DocumentDataset.read_json(text_files, add_filename=True)
    orig_dataset_code = DocumentDataset.read_json(code_files, add_filename=True)

    dataset_text = curation_steps_text(orig_dataset_text)
    dataset_text = dataset_text.persist()

    print(f"Original dataset length for text files: {len(orig_dataset_text.df)}")
    print(f"After dataprep: {len(dataset_text.df)}")

    dataset_code = curation_steps_code(orig_dataset_code)
    dataset_code = dataset_code.persist()

    print(f"Original dataset length for code files: {len(orig_dataset_code.df)}")
    print(f"After dataprep: {len(dataset_code.df)}")

    # Overwrite existing files in the curated directory.
    out_path = os.path.join(DATA_DIR, "curated")

    if os.path.isdir(out_path):
        shutil.rmtree(out_path)

    os.makedirs(out_path)
    dataset_text.to_json(out_path, write_to_filename=True)
    dataset_code.to_json(out_path, write_to_filename=True)
    client.close()


def main():
    parser = argparse.ArgumentParser()
    parser = add_distributed_args(parser)
    args = parser.parse_args()
    # Limit the total number of workers to ensure we don't run out of memory.
    args.n_workers = min(args.n_workers, 4)

    # Download all the sources and get the list of text and code files.
    text_files, code_files = download_sources(10, 10, 10)
    run_curation_pipeline(args, text_files, code_files)


if __name__ == "__main__":
    main()
