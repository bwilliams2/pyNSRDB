import logging
import requests
import json
import io
import time
import zipfile

import pandas as pd

from .data import create_df, process_zip_file


def process_response(response: requests.Response, base_url: str, timeout: int = 30):
    """[summary]

    Args:
        response (requests.Response): [description]
        base_url (str): [description]

    Returns:
        [type]: [description]
    """


    if response.status_code == 200:
        if "json" in base_url:
            logging.info("NSRDB request successfully submitted. File generation in progress.")
            response_data = json.loads(response.text)
            if response_data["outputs"].get("downloadUrl", False):
                download_url = response_data["outputs"]["downloadUrl"]
                elapsed_time = 0
                while elapsed_time < timeout:
                    try:
                        data = requests.get(download_url)
                        filename = download_url.split("/")[0]
                        zipped = io.BytesIO(data.content)
                        with zipfile.ZipFile(zipped) as zf:
                            df = process_zip_file(zf)
                        # TODO: Merge attrs from each csv
                        return df
                    except Exception as e:
                        if elapsed_time > timeout:
                            logging.info("File download is still being prepared.")
                            return response_data
                        else:
                            time.sleep(5)
                            elapsed_time += 5
            else:
                return response_data
        else:
            file_io = io.StringIO(response.text)
            return create_df(file_io)
    else:
        logging.warning("NSRDB request returned an error.")
        try:
            # For invalid parameters
            return json.loads(response.content)
        except:
            # For invalid API keys a text response is returned
            return response.text