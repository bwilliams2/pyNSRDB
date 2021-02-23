from typing import Union, Dict
import logging
from os import PathLike
from pathlib import Path
import requests
import hashlib
import json
import io
import time
import pandas as pd

from .data import _create_df, _process_download_url


def _process_response(
    response: requests.Response,
    base_url: str,
    timeout: int = 60,
    output_dir: PathLike = None,
):
    """[summary]

    Args:
        response (requests.Response): [description]
        base_url (str): [description]

    Returns:
        [type]: [description]
    """

    if response.status_code == 200:
        if "json" in base_url:
            logging.info(
                "NSRDB request successfully submitted. File generation in "
                "progress."
            )
            response_data = json.loads(response.text)
            if response_data["outputs"].get("downloadUrl", False):
                start = time.perf_counter()
                download_url = response_data["outputs"]["downloadUrl"]
                elapsed_time = 0
                while elapsed_time < timeout:
                    try:
                        response_data = _process_download_url(download_url)
                    except Exception as e:  # noqa: F841 Add specific handling
                        time.sleep(5)
                        elapsed_time = time.perf_counter() - start
        else:
            # for csv requests
            file_io = io.StringIO(response.text)
            response_data = _create_df(file_io)
        if output_dir is not None:
            return _save_to_output_dir(response_data, output_dir)
        else:
            return response_data
    else:
        logging.warning("NSRDB request returned an error.")
        try:
            # For invalid parameters
            return json.loads(response.content)
        except:  # noqa: E722
            # For invalid API keys a text response is returned
            return response.text


def _save_to_output_dir(
    data: Union[Dict[str, str], pd.DataFrame],
    output_dir: PathLike,
):
    if not isinstance(output_dir, Path):
        output_dir = Path(output_dir)
    if isinstance(data, pd.DataFrame):
        # construct csv in the same structure as received
        mode = "w"
        if hasattr(data, "attr") and isinstance(data.attr, dict):
            filenames = list(data.attrs.keys())
            if len(filenames) == 1:
                filename = filenames[0]
                header = pd.DataFrame([data.attr[filename]])
            else:
                # combine filenames up to 3
                filename = "_".join(filenames[:3]) + ".csv"
                header = pd.DataFrame(list(data.attr.values()))
            mode = "a"
            # Write header
            header.to_csv(filename, index=False)
        if not output_dir.is_dir():
            # assume filename
            filename = output_dir
        data.to_csv(filename, mode=mode)
    elif isinstance(data, dict):
        # likely response to Request
        if "inputs" in data and "downloadUrl" in data["inputs"]:
            filename = (
                data["inputs"]["downloadUrl"].split("/")[-1].strip(".zip")
            )
        else:
            filename = hashlib.md5(
                json.dumps(data, sort_keys=True).encode("utf-8")
            ).hexdigest()
        filename = f"NSRDB_request_{filename}.json"
        if output_dir.is_dir():
            with output_dir.joinpath(filename).open("w") as f:
                json.dump(data, f)
        else:
            # Assume filename is provided
            with output_dir.open("w") as f:
                json.dump(data, f)
    else:
        raise ValueError("data is incorrect format.")
