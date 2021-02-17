# pyNRSDB
Community-created functional Python interface for the [National Solar Radiation Database](https://nsrdb.nrel.gov/) (NSRDB) API from the [National Renewable Energy Laboratory](https://www.nrel.gov/) (NREL).

Simplifies requests to API and automates processing of response data in CSV and Zip file formats.

## Getting Started
pyNSRDB can be installed from PyPI for general use.

```
pip install pyNSRDB
```

For requests to the NSRDB, you must obtain a personal API key from https://developer.nrel.gov/signup/ before use.
The API key must be included with each request submitted through pyNSRDB.
You can supply the API key and email with each pyNSRDB request function or set-up an .pyNSRDB credential file that is automatically included with requests.

The credential file should be placed in the user's home directory (e.g. `C:\Users\%USERNAME%\\.pyNRSDB` for Windows and `\home\%USERNAME%\\.pyNSRDB` for Linux, Mac)
Entries for API_KEY and EMAIL are required, but all entires are encouraged

```
API_KEY=%YOURAPIKEY%
FULL_NAME=%YOURNAME%
EMAIL=%YOUREMAIL%
AFFILIATION=%AFFILIATION%
REASON=%REASONFORUSE%
MAILING_LIST=true
```

## Examples

The simplest requests can be made for single longitude, latitude points.

```jupyter
>>> from pyNSRDB.requests import PSM_TMY_request
>>> location = (-93.1567288182409, 45.15793882400205)
>>> data = PSM_TMY_request(location)
>>> data.head()
   Year  Month  Day  Hour  Minute  Dew Point  DHI  DNI  GHI  Surface Albedo  Pressure  Temperature  Wind Direction  Wind Speed
0  2004      1    1     0      30      -14.0    0    0    0            0.87       980         -9.0             141         2.5
1  2004      1    1     1      30      -13.0    0    0    0            0.87       980         -9.0             140         2.9
2  2004      1    1     2      30      -13.0    0    0    0            0.87       980         -9.0             135         3.3
3  2004      1    1     3      30      -13.0    0    0    0            0.87       980         -9.0             130         3.7
4  2004      1    1     4      30      -12.0    0    0    0            0.87       980         -8.0             120         4.0
```

More complicated geographical locations can be constructed using the [`shapely`](https://shapely.readthedocs.io/en/stable/manual.html) library to define WKT-compatible geometric shapes.

```jupyter
>>> from shapely.geometry import MultiPoint
>>> location = MultiPoint(((-90, 45), (-88, 43)))
>>> data = PSM_TMY_request(location)
>>> data.head()
   Year  Month  Day  Hour  Minute  Dew Point  DHI  DNI  GHI  Surface Albedo  Pressure  Temperature  Wind Direction  Wind Speed
0  2008      1    1     0      30      -14.0    0    0    0            0.87       960        -13.0             316         4.1
1  2008      1    1     1      30      -16.0    0    0    0            0.87       960        -14.0             316         4.2
2  2008      1    1     2      30      -17.0    0    0    0            0.87       960        -15.0             316         4.3
3  2008      1    1     3      30      -18.0    0    0    0            0.87       960        -16.0             316         4.4
4  2008      1    1     4      30      -19.0    0    0    0            0.87       960        -17.0             316         4.5
```

## Additional information on NSRDB

NSRDB Site: https://nsrdb.nrel.gov/

NSRDB API Documentation: https://nsrdb.nrel.gov/data-sets/api-instructions.html

References:

[1] M. Sengupta, Y. Xie, A. Lopez, A. Habte, G. Maclaurin, and J. Shelby, “The National Solar Radiation Data Base (NSRDB),” Renewable and Sustainable Energy Reviews, vol. 89, pp. 51–60, Jun. 2018, doi: [10.1016/j.rser.2018.03.003.](https://doi.org/10.1016/j.rser.2018.03.003)