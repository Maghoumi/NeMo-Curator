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

from nemo_curator.download import batch_download, CommonCrawlWARCDownloader
from nemo_curator.utils.download_utils import get_common_crawl_urls
from nemo_curator.utils.distributed_utils import get_client
from nemo_curator.utils.script_utils import add_distributed_args
from nemo_curator.utils.file_utils import expand_outdir_and_mkdir


def main(args):
    # Params
    start_snapshot = "2021-04"
    end_snapshot = "2021-10"
    output_directory = "/path/to/output"

    # Only download 10 shards as an example
    url_limit = 10

    # Set up Dask client
    client = get_client(args, args.device)

    # Download the raw compressed WARC files
    # Unlike the download_common_crawl function, this does not extract the files
    output_directory = expand_outdir_and_mkdir(output_directory)
    downloader = CommonCrawlWARCDownloader(output_directory)
    urls = get_common_crawl_urls(start_snapshot, end_snapshot)
    urls = urls[:url_limit]
    output_files = batch_download(urls, downloader)

    print(f"Finished downloading {len(output_files)} files at:")
    for file in output_files:
        print(file)

def attach_args(parser=argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)):
    return add_distributed_args(parser)

if __name__ == "__main__":
    main(attach_args().parse_args())