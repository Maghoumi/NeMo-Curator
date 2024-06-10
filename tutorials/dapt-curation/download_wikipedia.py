import os
from typing import Optional

import pandas as pd  # FIXME
from docbuilder import DAPTtxtDownloader, DAPTtxtExtractor, DAPTtxtIterator

from nemo_curator.download.doc_builder import download_and_extract


def download_wikipedia_sources(
    source_links_file: Optional[str] = None,
    output_dir: Optional[str] = None,
    limit: Optional[int] = None,
) -> str:
    """
    Downloads Wikipedia sources based on the provided source links file.

    Args:
        source_links_file (str, optional): Path to the file containing the source links. If not provided,
            a default file path will be used.
        output_dir (str, optional): Directory where the downloaded sources will be saved. If not provided,
            a default directory path will be used.
        limit (int, optional): Maximum number of sources to download. If provided, only the first `limit`
            sources will be downloaded.

    Returns:
        str: The path to the output directory where the downloaded sources are saved.
    """

    if source_links_file is None:
        source_links_file = os.path.join(
            os.path.dirname(__file__), "sources", "wikipedia_urls.jsonl"
        )

    if not os.path.exists(source_links_file):
        raise FileNotFoundError(f"File '{source_links_file}' not found.")

    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), "data", "raw", "wikipedia")
        os.makedirs(output_dir, exist_ok=True)

    urls = pd.read_json(path_or_buf=source_links_file, lines=True)
    urls = urls[0].tolist()

    if limit:
        urls = urls[:limit]

    output_format = {
        "text": str,
        "title": str,
        "id": str,
        "url": str,
        "language": str,
        "source_id": str,
        "filename": str,
    }

    downloader = DAPTtxtDownloader(output_dir)
    iterator = DAPTtxtIterator()
    extractor = DAPTtxtExtractor()

    dataset = download_and_extract(
        urls=urls,
        output_paths=[os.path.join(output_dir, os.path.basename(url)) for url in urls],
        downloader=downloader,
        iterator=iterator,
        extractor=extractor,
        output_format=output_format,
    )
    # Force the computation of the dataset
    dataset.persist()
    return output_dir
