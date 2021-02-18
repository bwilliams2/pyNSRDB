import logging

import pytest
import pandas as pd
from shapely.geometry import MultiPoint, Polygon

from pyNSRDB.requests import (
    NSRDB_data_query,
    PSM_TMY_request,
    PSM_request,
    PSM_temporal_request,
)

LOGGER = logging.getLogger(__name__)

poly_location = Polygon(
    (
        [-93.1968498, 44.6402006],
        [-93.1961632, 44.639712],
        [-93.1939316, 44.6086792],
        [-93.1202888, 44.6084348],
        [-93.1202888, 44.6411777],
        [-93.1968498, 44.6402006],
    )
)


def test_NSRDB_data_query_wkt():
    location = (-93.1567288182409, 45.15793882400205)
    data = NSRDB_data_query(location)
    assert isinstance(data, dict)
    # data comes back as array of available datasets
    assert isinstance(data["outputs"], list)
    assert len(data["outputs"]) > 0


@pytest.mark.parametrize(
    "query_type",
    [
        "satellite",
        "station",
    ],
)
def test_NSRDB_data_query_types(query_type):
    location = (-93.1567288182409, 45.15793882400205)
    data = NSRDB_data_query(location, query_type=query_type)
    # TODO: Both return no results but query_type=None returns data
    assert isinstance(data, dict)


#
#
# def test_NSRDB_data_address():
#     address = "1902 Miller Trunk Hwy, Duluth, MN 55811"
#     data = NSRDB_data_query(address=address)
#     assert isinstance(data, dict)
#     # data comes back as array of available datasets
#     assert isinstance(data["outputs"], list)
#     assert len(data["outputs"]) > 0


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
    with caplog.at_level(logging.INFO):
        data = PSM_TMY_request(poly_location)
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
    with caplog.at_level(logging.INFO):
        data = PSM_request(poly_location)
    assert "File generation" in caplog.text
    assert isinstance(data, pd.DataFrame)


def test_PSM_temporal_request():
    location = (-93.1567288182409, 45.15793882400205)
    data = PSM_temporal_request(location)
    assert isinstance(data, pd.DataFrame)


def test_PSM_temporal_request_bad_api_key(caplog):
    location = (-93.1567288182409, 45.15793882400205)
    with caplog.at_level(logging.WARNING):
        data = PSM_temporal_request(location, api_key="NotGoodKey")
    assert "NSRDB request returned an error." in caplog.text
    assert isinstance(data, dict)
    assert "API" in data["error"]["code"]


def test_PSM_temporal_request_bad_params(caplog):
    location = (-93.1567288182409, 45.15793882400205)
    with caplog.at_level(logging.WARNING):
        data = PSM_temporal_request(location, names="NotReal")
    assert "NSRDB request returned an error." in caplog.text
    assert isinstance(data, dict)
    assert "required 'name" in data["errors"][0]


def test_PSM_temporal_request_MultiPoint(caplog):
    location = MultiPoint(((-90, 45), (-88, 43)))
    with caplog.at_level(logging.INFO):
        data = PSM_temporal_request(location, timeout=120)
    assert "File generation" in caplog.text
    assert isinstance(data, pd.DataFrame)


def test_PSM_temporal_request_Polygon(caplog):
    with caplog.at_level(logging.INFO):
        data = PSM_temporal_request(poly_location, timeout=120)
    assert "File generation" in caplog.text
    assert isinstance(data, pd.DataFrame)
