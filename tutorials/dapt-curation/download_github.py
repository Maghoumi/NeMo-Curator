import json
import os
from typing import Optional

import cchardet as chardet
import pandas as pd  # FIXME

SUPPORTED_EXTENSIONS_TO_CATEGORY = {
    ".vx": "Viva",
    ".vxh": "Viva",
    ".v": "VerilogVHDL",
    ".vh": "VerilogVHDL",
    ".vhdl": "VerilogVHDL",
    ".va": "VerilogAnalog",
    ".c": "CPP",
    ".cpp": "CPP",
    ".h": "CPP",
    ".hpp": "CPP",
    ".py": "Python",
    ".config": "Config",
    ".mk": "Makefile",
    "makefile": "Makefile",
    "makeppfile": "Makefile",
    ".pm": "Perl",
    ".pl": "Perl",
    ".tcl": "Tcl",
    ".spec": "Spec",
    ".yaml": "Yaml",
    ".yml": "Yaml",
    ".sp": "Spice",
    ".cir": "Spice",
    ".cmd": "Spice",
    ".spf": "Spice",
    ".spice": "Spice",
    ".txt": "text",
    ".json": "text",
    ".xml": "text",
    ".html": "text",
    ".pdf": "text",
    ".md": "text",
    "": "text",  # No extension
}


def convert_file_to_json(input_fp: str) -> dict:
    """
    Convert a file to JSON format along with some metadata.

    Args:
        input_fp (str): The path to the input file.

    Returns:
        dict: The JSON representation of the file.
    """

    # Extract the file name and extension in lower case.
    filename = os.path.basename(input_fp)
    filename_no_ext, ext = os.path.splitext(filename)
    filename_no_ext = filename_no_ext.lower()
    ext = ext.lower()

    # If neither the file extension nor the filename is supported, return None
    if ext not in SUPPORTED_EXTENSIONS_TO_CATEGORY:
        if filename_no_ext not in SUPPORTED_EXTENSIONS_TO_CATEGORY:
            return None

        # The filename is there, but the extension is not. The category is determined by the filename.
        category = SUPPORTED_EXTENSIONS_TO_CATEGORY[filename_no_ext]
    else:
        category = SUPPORTED_EXTENSIONS_TO_CATEGORY[ext]

    # Open the file and read its content. Determine the encoding using cchardet. Skip over binary files.
    with open(input_fp, "rb") as file:
        content = file.read()
        # Determine the encoding of the file
        encoding = chardet.detect(content)["encoding"]

        if not encoding:
            return None

        try:
            content = content.decode(encoding)
        except UnicodeDecodeError:
            # If the file cannot be decoded, return None
            return None

    # Extract the metadata
    line_count = content.count("\n") + 1
    size_in_bytes = os.path.getsize(input_fp)

    return {
        # Use the file path as the unique ID
        "id": input_fp,
        "text": content,
        "file_extension": ext,
        "category": category,
        "line_count": line_count,
        "size_in_bytes": size_in_bytes,
        "path": input_fp,
    }


def convert_repo_to_jsonl(repo_dir: str, jsonl_dir: str):
    """
    Convert a repository to JSONL format and write it to the specified output directory.

    Args:
        repo_dir (str): The path to the repository directory.
        jsonl_dir (str): The path to the output directory.
    """

    repo_name = os.path.basename(repo_dir)
    output_fp = os.path.join(jsonl_dir, f"{repo_name}.jsonl")

    # Skip this repository if the JSONL file already exists
    if os.path.exists(output_fp):
        print(f"Repository '{repo_name}' already converted to JSONL. Skipping...")
        return
    else:
        print(f"Converting repository '{repo_name}' to JSONL format...")

    jsonl_docs = []

    for root, _, files in os.walk(repo_dir):
        # Skip over hidden directories, which include .git directories
        if any(part.startswith(".") for part in root.split(os.sep)):
            continue

        for file in files:
            # Skip the hidden files.
            if file.startswith("."):
                continue

            input_fp = os.path.join(root, file)
            jsonl = convert_file_to_json(input_fp)

            # Was the file converted to JSONL? If so, add it to the list.
            if jsonl:
                jsonl_docs.append(jsonl)

    # Write the JSONL documents to file
    with open(output_fp, "w") as output_file:
        for jsonl_doc in jsonl_docs:
            line = json.dumps(jsonl_doc)
            output_file.write(line + "\n")


def download_github_sources(
    source_links_file: Optional[str] = None,
    limit: Optional[int] = None,
) -> str:
    """
    Downloads GitHub repositories from the provided source links file and stores them in the specified directory.

    Args:
        source_links_file (str, optional): Path to the file containing the source links of the GitHub repositories.
            If not provided, a default file named 'github_repos.jsonl' in the same directory as this script will be used.
        limit (int, optional): Maximum number of repositories to download.

    Returns:
        str: The path to the directory where the repositories are stored.

    Raises:
        FileNotFoundError: If the source links file is not found.
    """

    if source_links_file is None:
        source_links_file = os.path.join(
            os.path.dirname(__file__), "sources", "github_repos.jsonl"
        )

    if not os.path.exists(source_links_file):
        raise FileNotFoundError(f"File '{source_links_file}' not found.")

    # The path under which the GitHub repositories data will be stored.
    github_root_dir = os.path.join(os.path.dirname(__file__), "data", "raw", "github")
    # The path under which the repositories will be cloned.
    clone_root_dir = os.path.join(github_root_dir, "repos")
    # The path under which the repositories will be stored in JSONL format.
    jsonl_dir = os.path.join(github_root_dir, "jsonl")
    os.makedirs(clone_root_dir, exist_ok=True)
    os.makedirs(jsonl_dir, exist_ok=True)

    urls = pd.read_json(path_or_buf=source_links_file, lines=True)
    urls = urls[0].tolist()

    if limit:
        urls = urls[:limit]

    # Given each URL, clone the repository
    for url in urls:
        repo_name = os.path.basename(url)
        clone_dir = os.path.join(clone_root_dir, repo_name)

        if os.path.exists(clone_dir):
            print(f"Repository '{repo_name}' already exists. Skipping...")
            continue

        exit_code = os.system(f"git clone https://github.com/{url} {clone_dir}")

        if exit_code != 0:
            print(f"Failed to clone repository '{repo_name}' from '{url}'.")
            continue

    # Walk through the cloned repositories using os.walk() and convert them to JSONL format
    for repo in os.listdir(clone_root_dir):
        repo_dir = os.path.join(clone_root_dir, repo)
        convert_repo_to_jsonl(repo_dir, jsonl_dir)

    return jsonl_dir
