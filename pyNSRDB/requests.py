from typing import Union, Tuple, Optional, List, Dict, Any
import logging
import requests
import json
import io

from shapely.geometry import Point, MultiPoint, Polygon
import pandas as pd

from .credentials import get_user_credentials


def _parse_attributes(attributes:Union[str, List[str]], allowed_attributes: Optional[List[str]]) -> str:
    """Parses users provided attributes and removes not allowed entries.

    Args:
        attributes (Union[str, List[str]]): Attributes provided. 
        allowed_attributes (Optional[List[str]]): Attributes allowed by API.

    Returns:
        str: Provided attributes allowed by API formed into comma-delimited string.
    """
    if isinstance(attributes, str) and "," in attributes:
        # Assume attributes is a comma delimited list
        attributes = [attribute.strip() for attribute in attributes.split(",")]

    if allowed_attributes is None:
        return ",".join(attributes)
    else:
        return ",".join(set(attributes).intersection(set(allowed_attributes)))


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

def TMY_request(
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
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """Submits TMY data request for given location(s).

    Args:
        location (Union[Tuple[float, float], Point, MultiPoint, Polygon]): [description]
        attributes (Optional[Union[str, List[str]]], optional): [description]. Defaults to None.
        names (str, optional): [description]. Defaults to "tmy".
        utc (bool, optional): [description]. Defaults to False.
        api_key (Optional[str], optional): [description]. Defaults to None.
        full_name (Optional[str], optional): [description]. Defaults to None.
        affiliation (Optional[str], optional): [description]. Defaults to None.
        email (Optional[str], optional): [description]. Defaults to None.
        reason (Optional[str], optional): [description]. Defaults to None.
        mailing_list (Optional[bool], optional): [description]. Defaults to None.

    Returns:
        Union[pd.DataFrame, Dict[str, Any]]: [description]
    """
    ALLOWED_ATTRIBUTES = ["dhi", "dni", "ghi", "dew_point", "air_temperature", "surface_pressure", "wind_direction", "wind_speed", "surface_albedo"]

    query_params = {}

    user_credientials = get_user_credentials(
        api_key,
        full_name,
        affiliation,
        email,
        reason,
        mailing_list,
    )


    location = _parse_query_location(location)
    query_params["wkt"] = location

    if attributes is not None:
        attributes = _parse_attributes(attributes, ALLOWED_ATTRIBUTES)
        query_params["attributes"] = attributes

    base_url = f"https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-tmy-download.csv"

    # Assemble params
    query_params.update(user_credientials)
    query_params["names"] = names
    query_params["utc"] = "true" if utc else "false"

    response = requests.get(base_url, params=query_params)
    if response.status_code == 200:
        info = pd.read_csv(io.StringIO(response.text), nrows=1)
        info.attrs = {"pyNSRDB": {"base_url": base_url, "params": query_params}}
    else:
        logging.warning("NSRDB request returned an error.")
        try:
            # For invalid parameters
            return json.loads(response.content)
        except:
            # For invalid API keys a text response is returned
            return response.text
