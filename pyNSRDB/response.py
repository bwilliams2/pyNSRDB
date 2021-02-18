import logging
import requests
import json
import io
import time

from .data import _create_df, _process_download_url


def _process_response(
    response: requests.Response, base_url: str, timeout: int = 60
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
                        return _process_download_url(download_url)
                    except Exception as e:  # noqa: F841 Add specific handling
                        if elapsed_time > timeout:
                            logging.info(
                                "File download is still being prepared."
                            )
                            return response_data
                        else:
                            time.sleep(5)
                            elapsed_time += time.perf_counter() - start
            return response_data
        else:
            file_io = io.StringIO(response.text)
            return _create_df(file_io)
    else:
        logging.warning("NSRDB request returned an error.")
        try:
            # For invalid parameters
            return json.loads(response.content)
        except:  # noqa: E722
            # For invalid API keys a text response is returned
            return response.text
