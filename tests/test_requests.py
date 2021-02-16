import logging
import pandas as pd

from pyNSRDB.requests import TMY_request

LOGGER = logging.getLogger(__name__)

def test_TMY_request():
    location = (-93.1567288182409, 45.15793882400205)
    data = TMY_request(location)
    assert isinstance(data, pd.DataFrame)

def test_TMY_request_bad_api_key(caplog):
    location = (-93.1567288182409, 45.15793882400205)
    with caplog.at_level(logging.WARNING):
        data = TMY_request(location, api_key="NotGoodKey")
    assert "NSRDB request returned an error." in caplog.text
    assert isinstance(data, str)
    assert "API" in data

def test_TMY_request_bad_params(caplog):
    location = (-93.1567288182409, 45.15793882400205)
    with caplog.at_level(logging.WARNING):
        data = TMY_request(location, names="NotReal")
    assert "NSRDB request returned an error." in caplog.text
    assert isinstance(data, dict)
    assert "errors" in data

