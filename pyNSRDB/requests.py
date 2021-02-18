from typing import Union, Tuple, Optional, List, Dict, Any
import requests
import json
from pathlib import Path

from shapely.geometry import Point, MultiPoint, Polygon
import pandas as pd

from .credentials import _get_user_credentials
from .response import _process_response


def _parse_inputs(
    names: Union[str, int, List[Union[str, int]]],
    allowed_names: Optional[List[str]],
    one_allowed: bool = False,
) -> str:
    """Parses user provided inputs and removes not allowed entries.

    Args:
        inputs (Union[str, List[str]]): User provided inputs.
        allowed_inputs (Optional[List[str]]): Inputs allowed by API.
        one_allowed (bool): If true, only one input is allowed rather than
            comma-delimited string. Defaults to True.

    Returns:
        str: Provided attributes allowed by API formed into comma-delimited
            string.
    """
    if isinstance(names, int):
        names = str(names)

    if isinstance(names, str):
        if "," in names:
            # Assume names is a comma delimited list
            names = [name.strip() for name in names.split(",")]
        else:
            names = [names]

    if allowed_names is None:
        return ",".join(names)
    else:
        return ",".join(set(names).intersection(set(allowed_names)))


def _parse_query_location(
    location: Union[Tuple[float, float], Point, MultiPoint, Polygon]
) -> str:
    """Convert given locations into WKT representations.

    Args:
        location (QueryLocation): Provided location definition.

    Raises:
        ValueError: Raised for when unable to parse location.

    Returns:
        str: WKT representation of location.
    """
    if isinstance(location, (tuple, list)):
        # Assume this is [lon lat] following wkt format
        location = Point(location[0], location[1])
    if isinstance(location, (Point, MultiPoint, Polygon)):
        wkt = location.wkt
    else:
        raise ValueError("Location is not in correct format.")
    return wkt


def _assemble_query_params(
    location: Union[Tuple[float, float], Point, MultiPoint, Polygon],
    attributes: Optional[Union[str, List[str]]] = None,
    allowed_attributes: Optional[List[str]] = None,
    one_allowed_attributes: bool = False,
    names: Optional[Union[str, List[int]]] = "tmy",
    allowed_names: Optional[List[str]] = None,
    one_allowed_names: bool = False,
    utc: bool = False,
    api_key: Optional[str] = None,
    full_name: Optional[str] = None,
    affiliation: Optional[str] = None,
    email: Optional[str] = None,
    reason: Optional[str] = None,
    mailing_list: Optional[bool] = None,
    **kwargs,
):
    user_credientials = _get_user_credentials(
        api_key,
        full_name,
        affiliation,
        email,
        reason,
        mailing_list,
    )
    query_params = {}
    if attributes is not None:
        attributes = _parse_inputs(
            attributes, allowed_attributes, one_allowed_attributes
        )
        query_params["attributes"] = attributes
    query_params["wkt"] = _parse_query_location(location)
    query_params.update(user_credientials)
    query_params["names"] = _parse_inputs(
        names, allowed_names, one_allowed_names
    )
    query_params.update(kwargs)

    def process_bool(input: Union[str, bool]):
        if isinstance(input, bool):
            return "true" if input else "false"
        else:
            return input

    query_params = {k: process_bool(v) for k, v in query_params.items()}

    return query_params


def _parse_base_url(url: str, wkt: str, names: str):
    """Selects appropriate download request url based on WKT location string"""
    # CSV allowed for single point location and single name/year
    if "POINT" == wkt[:5] and len(names.split(",")) == 0:
        return f"{url}.csv"
    else:
        return f"{url}.json"


def NSRDB_data_query(
    location: Union[Tuple[float, float], Point, MultiPoint, Polygon] = None,
    address: str = None,
    query_type: str = None,
    api_key: str = None,
    show_empty: bool = False,
) -> Dict[str, str]:
    """Performs data query with requested parameters.

    Args:
        location (Union[Tuple[float, float], Point, MultiPoint,
            Polygon], optional): Location to perform query at. If None,
            `address` must be supplied. Defaults to None.
        address (str, optional): Address of location to perform query at.
            If None, `location` must be supplied. Defaults to None.
        query_type (str, optional): Types of datasets to include.
            Defaults to None.
        api_key (str, optional): [description]. Defaults to None.
        show_empty (bool, optional): [description]. Defaults to False.

    Returns:
        Dict[str, str]: [description]

    See Also:
        https://developer.nrel.gov/docs/solar/nsrdb/nsrdb_data_query/
    """
    user_credientials = _get_user_credentials(
        api_key,
    )
    query_params = {"api_key": user_credientials["api_key"]}
    if location is None and address is None:
        raise ValueError("Both 'location' or 'address' cannot be `None`.")

    if query_type is not None:
        query_params["type"] = _parse_inputs(
            query_type, ["station", "satellite"], one_allowed=True
        )

    if location is not None:
        query_params["wkt"] = _parse_query_location(location)
    else:
        query_params["address"] = address

    query_params["show_empty"] = "true" if show_empty else "false"
    query_params["format"] = "json"

    base_url = "https://developer.nrel.gov/api/solar/nsrdb_data_query.json"

    response = requests.get(base_url, params=query_params)
    return json.loads(response.text)


def PSM_request(
    location: Union[Tuple[float, float], Point, MultiPoint, Polygon],
    attributes: Union[str, List[str]] = None,
    names: Union[str, int, List[Union[str, int]]] = None,
    utc: bool = False,
    leap_day: bool = False,
    interval: int = 60,
    api_key: str = None,
    full_name: str = None,
    affiliation: str = None,
    email: str = None,
    reason: str = None,
    mailing_list: bool = None,
    output_dir: Union[str, Path] = None,
    timeout: int = 60,
) -> Union[pd.DataFrame, Dict[str, Any], None]:
    """Submits Physical Solar Model v3 data request for given location(s).

        Allowed attributes:
            air_temperature, clearsky_dhi, clearsky_dni, clearsky_ghi,
            cloud_type, dew_point, dhi, dni, fill_flag, ghi, ghuv-280-400,
            ghuv-285-385, relative_humidity, solar_zenith_angle,
            surface_albedo, surface_pressure, total_precipitable_water,
            wind_direction, wind_speed

        Allowed names:
            1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008,
            2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019

    Args:
        location (Union[Tuple[float, float], Point, MultiPoint, Polygon]):
            Location to request data for.
        attributes (Optional[Union[str, List[str]]], optional): Attributes to
            request data for. Defaults to None.
        names (Union[str, int, List[Union[str, int]]], optional): Year(s) to
            request PSM V3 data for. If None, selects most recent year.
            Defaults to None.
        utc (bool, optional): If true, convert timestamps to UTC. Defaults to
            False.
        leap_day (bool, optional): If true, data includes leap_day.
        interval (int, optional): Returns 30 or 60 min interval data. Allowed
            values of 30 and 60.
        api_key (Optional[str], optional): User's api key to send with request.
            Credential file takes precedence. Defaults to None.
        full_name (Optional[str], optional): User's full name to send with
            request. Credential file takes precedence. Defaults to None.
        affiliation (Optional[str], optional): User's affiliation to send with
            request. Credential file takes precedence. Defaults to None.
        email (Optional[str], optional): User's email to send with request.
            Credential file takes precedence. Defaults to None.
        reason (Optional[str], optional): Reason for request. Defaults to None.
        mailing_list (Optional[bool], optional): If True, user is added to NREL
            NSRDB mailing list. Defaults to None.
        output_dir (Union[str, List[str]], optional): Output directory to save
            returned data. Defaults to None.
        timeout (int): Time to wait for valid download URL. Used only for
            requests that need file generation. Defaults to 60.

    Returns:
        Union[pd.DataFrame, Dict[str, Any], str]: If direct download is
            possible, pandas DataFrame populated with data is returned. In all
            other cases, response message from NSRDB API is returned
            as dictionary or string.

    See Also:
        https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
    """
    ALLOWED_ATTRIBUTES = [
        "air_temperature",
        "clearsky_dhi",
        "clearsky_dni",
        "clearsky_ghi",
        "cloud_type",
        "dew_point",
        "dhi",
        "dni",
        "fill_flag",
        "ghi",
        "ghuv-280-400",
        "ghuv-285-385",
        "relative_humidity",
        "solar_zenith_angle",
        "surface_albedo",
        "surface_pressure",
        "total_precipitable_water",
        "wind_direction",
        "wind_speed",
    ]
    ALLOWED_NAMES = [str(year) for year in range(1998, 2020)]

    if names is None:
        names = ALLOWED_NAMES[-1]

    URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-download"

    # Assemble params
    query_params = _assemble_query_params(
        location,
        attributes,
        ALLOWED_ATTRIBUTES,
        False,
        names,
        ALLOWED_NAMES,
        False,
        utc,
        api_key,
        full_name,
        affiliation,
        email,
        reason,
        mailing_list,
        leap_day=leap_day,
        interval=interval,
    )

    base_url = _parse_base_url(URL, query_params["wkt"], query_params["names"])

    response = requests.get(base_url, params=query_params)
    return _process_response(response, base_url, timeout)


def PSM_TMY_request(
    location: Union[Tuple[float, float], Point, MultiPoint, Polygon],
    attributes: Optional[Union[str, List[str]]] = None,
    names: str = "tmy",
    utc: bool = False,
    api_key: Optional[str] = None,
    full_name: Optional[str] = None,
    affiliation: Optional[str] = None,
    email: Optional[str] = None,
    reason: Optional[str] = None,
    mailing_list: Optional[bool] = None,
    output_dir: Optional[Union[str, Path]] = None,
    timeout: int = 60,
) -> Union[pd.DataFrame, Dict[str, Any], None]:
    """Submits TMY data request for given location(s).

        Allowed attributes:
            dhi, dni, ghi, dew_point, air_temperature, surface_pressure,
            wind_direction, wind_speed, surface_albedo

        Allowed names:
             tmy, tmy-2017, tdy-2017, tgy-2017. tmy-2018, tdy-2018, tgy-2018,
             tmy-2019, tdy-2019, tgy-2019

    Args:
        location (Union[Tuple[float, float], Point, MultiPoint, Polygon]):
            Location to request data for.
        attributes (Optional[Union[str, List[str]]], optional): Attributes to
            request data for. Defaults to None.
        names (str, optional): Name of tmy dataset to request. Defaults to
            "tmy".
        utc (bool, optional): If true, convert timestamps to UTC.
            Defaults to False.
        api_key (Optional[str], optional): User's api key to send with request.
            Credential file takes precedence. Defaults to None.
        full_name (Optional[str], optional): User's full name to send with
            request. Credential file takes precedence. Defaults to None.
        affiliation (Optional[str], optional): User's affiliation to send with
            request. Credential file takes precedence. Defaults to None.
        email (Optional[str], optional): User's email to send with request.
            Credential file takes precedence. Defaults to None.
        reason (Optional[str], optional): Reason for request. Defaults to None.
        mailing_list (Optional[bool], optional): If True, user is added to
            NREL NSRDB mailing list. Defaults to None.
        output_dir (Union[str, List[str]], optional): Output directory to save
            returned data. Defaults to None.
        timeout (int): Time to wait for valid download URL. Used only for
            requests that need file generation. Defaults to 60.

    Returns:
        Union[pd.DataFrame, Dict[str, Any], str]: If direct download is
            possible, pandas DataFrame populated with data is returned. In all
            other cases, response message from NSRDB API is returned
            as dictionary or string.

    See Also:
        https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
    """

    ALLOWED_ATTRIBUTES = [
        "dhi",
        "dni",
        "ghi",
        "dew_point",
        "air_temperature",
        "surface_pressure",
        "wind_direction",
        "wind_speed",
        "surface_albedo",
    ]

    ALLOWED_NAMES = [
        "tmy",
        "tmy-2017",
        "tdy-2017",
        "tgy-2017",
        "tmy-2018",
        "tdy-2018",
        "tgy-2018",
        "tmy-2019",
        "tdy-2019",
        "tgy-2019",
    ]

    URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-tmy-download"

    # Assemble params
    query_params = _assemble_query_params(
        location,
        attributes,
        ALLOWED_ATTRIBUTES,
        False,
        names,
        ALLOWED_NAMES,
        True,
        utc,
        api_key,
        full_name,
        affiliation,
        email,
        reason,
        mailing_list,
    )

    base_url = _parse_base_url(URL, query_params["wkt"], query_params["names"])

    response = requests.get(base_url, params=query_params)
    return _process_response(response, base_url, timeout)


def PSM_temporal_request(
    location: Union[Tuple[float, float], Point, MultiPoint, Polygon],
    attributes: Optional[Union[str, List[str]]] = None,
    names: Optional[Union[str, int, List[Union[str, int]]]] = None,
    utc: bool = False,
    leap_day: bool = False,
    interval: int = 5,
    api_key: Optional[str] = None,
    full_name: Optional[str] = None,
    affiliation: Optional[str] = None,
    email: Optional[str] = None,
    reason: Optional[str] = None,
    mailing_list: Optional[bool] = None,
    output_dir: Optional[Union[str, Path]] = None,
    timeout: int = 60,
) -> Union[pd.DataFrame, Dict[str, Any], None]:
    """Submits Physical Solar Model v3 Five Minute Temporal Resolution data
        request for given location(s).

        Allowed attributes:
            air_temperature, clearsky_dhi, clearsky_dni, clearsky_ghi,
            cloud_type, dew_point, dhi, dni, fill_flag, ghi, relative_humidity,
            solar_zenith_angle, surface_albedo, surface_pressure,
            total_precipitable_water, wind_direction, wind_speed

        Allowed names:
            2018, 2019

    Args:
        location (Union[Tuple[float, float], Point, MultiPoint, Polygon]):
            Location to request data for.
        attributes (Optional[Union[str, List[str]]], optional): Attributes to
            request data for. Defaults to None.
        names (Union[str, int, List[Union[str, int]]], optional): Year(s) to
            request PSM V3 data for. If None, selects most recent year.
            Defaults to None.
        utc (bool, optional): If true, convert timestamps to UTC. Defaults to
            False.
        leap_day (bool, optional): If true, data includes leap_day.
        interval (int, optional): Returns 30 or 60 min interval data. Allowed
            values of 5, 15, 30 and 60. Defaults to 5.
        api_key (Optional[str], optional): User's api key to send with request.
            Credential file takes precedence. Defaults to None.
        full_name (Optional[str], optional): User's full name to send with
            request. Credential file takes precedence. Defaults to None.
        affiliation (Optional[str], optional): User's affiliation to send with
            request. Credential file takes precedence. Defaults to None.
        email (Optional[str], optional): User's email to send with request.
            Credential file takes precedence. Defaults to None.
        reason (Optional[str], optional): Reason for request. Defaults to None.
        mailing_list (Optional[bool], optional): If True, user is added to NREL
            NSRDB mailing list. Defaults to None.
        output_dir (Union[str, List[str]], optional): Output directory to save
            returned data. Defaults to None.
        timeout (int): Time to wait for valid download URL. Used only for
            requests that need file generation. Defaults to 60.

    Returns:
        Union[pd.DataFrame, Dict[str, Any], str]: If direct download is
            possible, pandas DataFrame populated with data is returned. In all
            other cases, response message from NSRDB API is returned
            as dictionary or string.

    See Also:
        https://developer.nrel.gov/docs/solar/nsrdb/psm3-5min-download/
    """
    ALLOWED_ATTRIBUTES = [
        "air_temperature",
        "clearsky_dhi",
        "clearsky_dni",
        "clearsky_ghi",
        "cloud_type",
        "dew_point",
        "dhi",
        "dni",
        "fill_flag",
        "ghi",
        "relative_humidity",
        "solar_zenith_angle",
        "surface_albedo",
        "surface_pressure",
        "total_precipitable_water",
        "wind_direction",
        "wind_speed",
    ]
    ALLOWED_NAMES = [str(year) for year in range(2018, 2020)]

    if names is None:
        names = ALLOWED_NAMES[-1]

    URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-5min-download"

    # Assemble params
    query_params = _assemble_query_params(
        location,
        attributes,
        ALLOWED_ATTRIBUTES,
        False,
        names,
        ALLOWED_NAMES,
        False,
        utc,
        api_key,
        full_name,
        affiliation,
        email,
        reason,
        mailing_list,
        leap_day=leap_day,
        interval=interval,
    )

    base_url = _parse_base_url(URL, query_params["wkt"], query_params["names"])

    response = requests.get(base_url, params=query_params)
    return _process_response(response, base_url, timeout)
