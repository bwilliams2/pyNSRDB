from typing import Union, Tuple, Optional, List, Dict, Any, Optional
import requests
from pathlib import Path

from shapely.geometry import Point, MultiPoint, Polygon
import pandas as pd

from .credentials import get_user_credentials
from .response import process_response

ALLOWED_ATTRIBUTES = ["dhi", "dni", "ghi", "dew_point", "air_temperature", "surface_pressure", "wind_direction", "wind_speed", "surface_albedo"]

def _parse_attributes(attributes:Union[str, List[str]], allowed_attributes: Optional[List[str]]) -> str:
    """Parses users provided attributes and removes not allowed entries.

    Args:
        attributes (Union[str, List[str]]): Attributes provided. 
        allowed_attributes (Optional[List[str]]): Attributes allowed by API.

    Returns:
        str: Provided attributes allowed by API formed into comma-delimited string.
    """
    if isinstance(attributes, str):
        if "," in attributes:
            # Assume attributes is a comma delimited list
            attributes = [attribute.strip() for attribute in attributes.split(",")]
        else:
            attributes = [attributes]

    if allowed_attributes is None:
        return ",".join(attributes)
    else:
        return ",".join(set(attributes).intersection(set(allowed_attributes)))

def _parse_names(names:Union[str, int, List[Union[str, int]]], allowed_names: Optional[List[str]]) -> str:
    """Parses user provided names and removes not allowed entries.

    Args:
        names (Union[str, List[str]]): Attributes provided. 
        allowed_names (Optional[List[str]]): Attributes allowed by API.

    Returns:
        str: Provided attributes allowed by API formed into comma-delimited string.
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



def _parse_query_location(location: Union[Tuple[float, float], Point, MultiPoint, Polygon]) -> str:
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

def PSM_request(
    location: Union[Tuple[float, float], Point, MultiPoint, Polygon],
    attributes: Optional[Union[str, List[str]]] = None,
    names: Union[str, int, List[Union[str, int]]] = 2019,
    utc: bool = False,
    api_key: Optional[str] = None,
    full_name: Optional[str] = None,
    affiliation: Optional[str] = None,
    email: Optional[str] = None,
    reason: Optional[str] = None,
    mailing_list: Optional[bool] = None,
    output_dir: Optional[Union[str, Path]] = None,
    timeout: int = 30 
) -> Union[pd.DataFrame, Dict[str, Any], None]:
    """Submits TMY data request for given location(s).
    
    Allowed attributes: "dhi", "dni", "ghi", "dew_point", "air_temperature", "surface_pressure", "wind_direction", "wind_speed", "surface_albedo"

    Args:
        location (Union[Tuple[float, float], Point, MultiPoint, Polygon]): Location to request data for.
        attributes (Optional[Union[str, List[str]]], optional): Attributes to request data for. Defaults to None.
        names (str, optional): Name of tmy dataset to request. Defaults to "tmy".
        utc (bool, optional): If true, convert timestamps to UTC. Defaults to False.
        api_key (Optional[str], optional): User's api key to send with request. Credential file takes precedence. Defaults to None.
        full_name (Optional[str], optional): User's full name to send with request. Credential file takes precedence. Defaults to None.
        affiliation (Optional[str], optional): User's affiliation to send with request. Credential file takes precedence. Defaults to None.
        email (Optional[str], optional): User's email to send with request. Credential file takes precedence. Defaults to None.
        reason (Optional[str], optional): Reason for request. Defaults to None.
        mailing_list (Optional[bool], optional): If True, user is added to NREL NSRDB mailing list. Defaults to None.
        output_dir (Union[str, List[str]], optional): Output directory to save returned data. Defaults to None.
        timeout (int): Time to wait for valid download URL. Used only for requests that need file 
            generation. Defaults to 30.

    Returns:
        Union[pd.DataFrame, Dict[str, Any], str]: If direct download is possible, pandas DataFrame with dat
            populated with data is returned. In all other cases, response message from NSRDB API is returned
            as dictionary or string.

    See Also:
        https://developer.nrel.gov/docs/solar/nsrdb/psm3-tmy-download/
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

    query_params = {}

    user_credientials = get_user_credentials(
        api_key,
        full_name,
        affiliation,
        email,
        reason, mailing_list,
    )

    location = _parse_query_location(location)
    query_params["wkt"] = location

    if attributes is not None:
        attributes = _parse_attributes(attributes, ALLOWED_ATTRIBUTES)
        query_params["attributes"] = attributes

    query_params["names"] = _parse_names(names, ALLOWED_NAMES)

    if "POINT" == query_params["wkt"][:5] and len(query_params["names"].split(",")) == 1:
        base_url = f"https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-download.csv"
    else:
        base_url = f"https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-download.json"

    # Assemble params
    query_params.update(user_credientials)
    query_params["utc"] = "true" if utc else "false"

    response = requests.get(base_url, params=query_params)
    return process_response(response, base_url, timeout)

def PSM_TMY_request(
    location: Union[Tuple[float, float], Point, MultiPoint, Polygon],
    attributes: Optional[Union[str, List[str]]] = None,
    names: Optional[Union[str, List[int]]] = "tmy",
    utc: bool = False,
    api_key: Optional[str] = None,
    full_name: Optional[str] = None,
    affiliation: Optional[str] = None,
    email: Optional[str] = None,
    reason: Optional[str] = None,
    mailing_list: Optional[bool] = None,
    output_dir: Optional[Union[str, Path]] = None,
    timeout: int = 30
) -> Union[pd.DataFrame, Dict[str, Any], None]:
    """Submits TMY data request for given location(s).
    
    Allowed attributes: dhi, dni, ghi, dew_point, air_temperature, surface_pressure, wind_direction, wind_speed, surface_albedo

    Args:
        location (Union[Tuple[float, float], Point, MultiPoint, Polygon]): Location to request data for.
        attributes (Optional[Union[str, List[str]]], optional): Attributes to request data for. Defaults to None.
        names (str, optional): Name of tmy dataset to request. Defaults to "tmy".
        utc (bool, optional): If true, convert timestamps to UTC. Defaults to False.
        api_key (Optional[str], optional): User's api key to send with request. Credential file takes precedence. Defaults to None.
        full_name (Optional[str], optional): User's full name to send with request. Credential file takes precedence. Defaults to None.
        affiliation (Optional[str], optional): User's affiliation to send with request. Credential file takes precedence. Defaults to None.
        email (Optional[str], optional): User's email to send with request. Credential file takes precedence. Defaults to None.
        reason (Optional[str], optional): Reason for request. Defaults to None.
        mailing_list (Optional[bool], optional): If True, user is added to NREL NSRDB mailing list. Defaults to None.
        output_dir (Union[str, List[str]], optional): Output directory to save returned data. Defaults to None.
        timeout (int): Time to wait for valid download URL. Used only for requests that need file 
            generation. Defaults to 30.

    Returns:
        Union[pd.DataFrame, Dict[str, Any], str]: If direct download is possible, pandas DataFrame with dat
            populated with data is returned. In all other cases, response message from NSRDB API is returned
            as dictionary or string.

    See Also:
        https://developer.nrel.gov/docs/solar/nsrdb/psm3-download/
    """
    query_params = {}

    user_credientials = get_user_credentials(
        api_key,
        full_name,
        affiliation,
        email,
        reason, mailing_list,
    )

    location = _parse_query_location(location)
    query_params["wkt"] = location

    if attributes is not None:
        attributes = _parse_attributes(attributes, ALLOWED_ATTRIBUTES)
        query_params["attributes"] = attributes

    if "POINT" == query_params["wkt"][:5]:
        base_url = f"https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-tmy-download.csv"
    else:
        base_url = f"https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-tmy-download.json"

    # Assemble params
    query_params.update(user_credientials)
    query_params["names"] = names
    query_params["utc"] = "true" if utc else "false"

    response = requests.get(base_url, params=query_params)
    return process_response(response, base_url, timeout)