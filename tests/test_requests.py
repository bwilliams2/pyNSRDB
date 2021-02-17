import logging
import pandas as pd
from shapely.geometry import MultiPoint, Polygon

from pyNSRDB.requests import PSM_TMY_request, PSM_request

LOGGER = logging.getLogger(__name__)

def test_PSM_TMY_request():
    location = (-93.1567288182409, 45.15793882400205)
    data = PSM_TMY_request(location)
    assert isinstance(data, pd.DataFrame)

def test_PSM_TMY_request_bad_api_key(caplog):
    location = (-93.1567288182409, 45.15793882400205)
    with caplog.at_level(logging.WARNING):
        data = PSM_TMY_request(location, api_key="NotGoodKey")
    assert "NSRDB request returned an error." in caplog.text
    assert isinstance(data, str)
    assert "API" in data

def test_PSM_TMY_request_bad_params(caplog):
    location = (-93.1567288182409, 45.15793882400205)
    with caplog.at_level(logging.WARNING):
        data = PSM_TMY_request(location, names="NotReal")
    assert "NSRDB request returned an error." in caplog.text
    assert isinstance(data, dict)
    assert "errors" in data

def test_PSM_TMY_request_MultiPoint(caplog):
    location = MultiPoint(((-90, 45), (-88, 43)))
    with caplog.at_level(logging.INFO):
        data = PSM_TMY_request(location)
    assert "File generation" in caplog.text
    assert isinstance(data, pd.DataFrame)
    

def test_PSM_TMY_request_Polygon(caplog):
    location = Polygon(((-90, 45), (-88, 43)))
    with caplog.at_level(logging.INFO):
        data = PSM_TMY_request(location)
    assert "File generation" in caplog.text
    assert isinstance(data, pd.DataFrame)

def test_PSM_request():
    location = (-93.1567288182409, 45.15793882400205)
    data = PSM_request(location)
    assert isinstance(data, pd.DataFrame)

def test_PSM_request_bad_api_key(caplog):
    location = (-93.1567288182409, 45.15793882400205)
    with caplog.at_level(logging.WARNING):
        data = PSM_request(location, api_key="NotGoodKey")
    assert "NSRDB request returned an error." in caplog.text
    assert isinstance(data, str)
    assert "API" in data

def test_PSM_request_bad_params(caplog):
    location = (-93.1567288182409, 45.15793882400205)
    with caplog.at_level(logging.WARNING):
        data = PSM_request(location, names="NotReal")
    assert "NSRDB request returned an error." in caplog.text
    assert isinstance(data, dict)
    assert "errors" in data

def test_PSM_request_MultiPoint(caplog):
    location = MultiPoint(((-90, 45), (-88, 43)))
    with caplog.at_level(logging.INFO):
        data = PSM_request(location)
    assert "File generation" in caplog.text
    assert isinstance(data, pd.DataFrame)
    

def test_PSM_request_Polygon(caplog):
    location = Polygon((
      [
        -91.2991333,
        42.1002603
      ],
      [
        -90.9461975,
        42.1002603
      ],
      [
        -90.953064,
        42.3016903
      ],
      [
        -91.2812805,
        42.29458
      ],
      [
        -91.307373,
        42.0982224
      ],
      [
        -91.2991333,
        42.1002603
      ]
    ))
    with caplog.at_level(logging.INFO):
        data = PSM_request(location)
    assert "File generation" in caplog.text
    assert isinstance(data, pd.DataFrame)