import enum
import requests
from typing import List
import pandas as pd
import io

#### ENUM OBJECTS ####

class EnumBase(str, enum.Enum):
    pass
    def __str__(self):
        return self.value

class Url(EnumBase):
    CSV_URL = 'https://data.geo.admin.ch/ch.meteoschweiz.prognosen/punktprognosen/COSMO-E-all-stations.csv'

class WeatherParameters(EnumBase):
    TEMPERATURE = 'T_2M'
    WIND_SPEED = 'FF_10M'
    WIND_DIRECTION = 'DD_10M'
    TOTAL_PRECIPITATION = 'TOT_PREC'
    RELATIVE_HUMIDITY = 'RELHUM_2M'
    DURATION_SUNSHINE = 'DURSUN'

class StationsCols(EnumBase):
    STATION_NAME = 'station_name'
    GRID_LONGITUDE = 'grid_longitude'
    GRID_LATITUDE = 'grid_latitude'
    GRID_I = 'grid_i'
    GRID_J = 'grid_j'
    GRID_HEIGHT = 'grid_height'

class UnitsCols(EnumBase):
    WEATHER_PARAM = 'weather_param'
    UNIT = 'unit'

class DataCols(EnumBase):
    WEATHER_PARAM = UnitsCols.WEATHER_PARAM
    STATION = 'stn'
    TIME = 'time'
    LEADTIME = 'leadtime'
    MEMBER = 'member'
    FORECASTED_VALUE = 'forecasted_value'

#### UTILS ####

def csv_to_pd(
    content: requests.Response.content,
    skiprows: List[int],
    sep: str=";",
    delimiter: str=None
) -> pd.DataFrame:
    """Read csv content into pandas Dataframe
    Generic method

    Args:
        content (requests.Response.content): scraped csv content
        skiprows (List[int]): used to filter csv and collect relevant fields/rows
        sep (str, optional): field seperator. Defaults to ";".
        delimiter (str, optional): delimiter. Defaults to None.

    Returns:
        pd.DataFrame: Pandas DataFrame processed from scraped csv content
    """
    return pd.read_csv(
        io.StringIO(
            content.decode('utf-8')
        ),
        skiprows=skiprows,
        sep=sep,
        delimiter=delimiter,
    )

def transpose_df(
    df: pd.DataFrame,
    cols: List[str],
) -> pd.DataFrame:
    """Transposing DataFrame to shape data in DB row-like format

    Args:
        df (pd.DataFrame): input pandas DataFrame to transpose
        cols (List[str]): columns to transpose

    Returns:
        pd.DataFrame: Transposed pandas DataFrame
    """
    df = df.transpose().reset_index(inplace=False)
    return df.set_axis(
        cols,
        axis=1,
        inplace=False,
    )
