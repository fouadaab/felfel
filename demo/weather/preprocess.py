import pandas as pd
import weather.models as models
import weather.helper as helper
from typing import Tuple
import re


def process_df(
    df_stations: pd.DataFrame,
    df_units: pd.DataFrame,
    df_data: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Process data inside pandas DataFrames to format before ingestion to DB

    Args:
        df_stations (pandas.DataFrame): Dataframe containing all stations and their geographical locations
        df_units (pandas.DataFrame): DataFrame containing details about each weather parameter units
        df_data (pandas.DataFrame): DataFrame containing forecasted data for each station/member

    Returns:
        tuple of post-processed DFs 
        df_stations (pandas.DataFrame), df_units (pandas.DataFrame), df_data (pandas.DataFrame)
    """
    # Cleaning dataset and transposing to shape in DB row-like format
    df_stations = df_stations.drop(columns=df_stations.columns[[0,-1]], axis=1)
    df_stations = helper.transpose_df(df=df_stations, cols=list(helper.StationsCols))

    # Selecting only relevant columns defined in WeatherParameters from helper
    df_units = df_units[list(helper.WeatherParameters)]
    df_units = helper.transpose_df(df=df_units, cols=list(helper.UnitsCols))

    # Renaming columns and defining regex rules to process them with melt() from pandas
    df_data = df_data.dropna(how='all', axis=1)
    df_data = df_data.rename(columns=lambda x: re.sub('\.','_',x))
    re_member = re.compile(r'\d{1,2}$')
    re_param = re.compile(r'.+?(?=_\d{1,2}$)')

    # Using melt() method to get all data points (forecasted values) for each station and for every member
    df_data = pd.DataFrame(
        df_data.melt(
            id_vars=[helper.DataCols.STATION, helper.DataCols.TIME, helper.DataCols.LEADTIME],
            var_name=helper.DataCols.WEATHER_PARAM,
            value_name=helper.DataCols.FORECASTED_VALUE,
        )
    )

    # Extracting member from weather_param attributes after melt and cleaning weather_param afterwards
    # This step assumes that the data points are always written to csv in ascending order (from member 0 to 20)
    df_data[helper.DataCols.MEMBER] = df_data[helper.DataCols.WEATHER_PARAM].apply(
        lambda x: int(re_member.search(x).group(0)) if re_member.search(x) else 0
    ) 
    df_data[helper.DataCols.WEATHER_PARAM] = df_data[helper.DataCols.WEATHER_PARAM].apply(
        lambda x: re_param.search(x).group(0) if re_param.search(x) else x
    )

    # Convert string to datetime format using fixed datetime format(assuming data always written in same format)
    df_data[helper.DataCols.TIME] = pd.to_datetime(df_data[helper.DataCols.TIME], format='%Y%m%d %H:%M')

    # Cleaning leadtime -> only hours carry added value
    df_data= df_data.replace({helper.DataCols.LEADTIME: r':00$'}, {helper.DataCols.LEADTIME: ''}, regex=True)

    return df_stations, df_units, df_data

def write_to_db(
    df_stations: pd.DataFrame,
    df_units: pd.DataFrame,
    df_data: pd.DataFrame,
) -> int:
    """Writing each dataset into DB tables
    Both df_stations and df_units are only written if the tables do not already exist (fixed data fields)
    df_data treated at row-level (no bulk insert):
        - If PK already exists: update value
        - Otherwise: Create new row with given value

    Args:
        df_stations (pandas.DataFrame): Dataframe containing all stations and their geographical locations
        df_units (pandas.DataFrame): DataFrame containing details about each weather parameter units
        df_data (pandas.DataFrame): DataFrame containing forecasted data for each station/member

    Returns:
        int: Number of new rows written to DB's Entry table (weather_entry) -> Additional forecasted weather data
    """
    # Write to DB
    out = []
    if len(models.Station.objects.all()) > 0:
        pass
    else:
        for row in df_stations.itertuples():
            obj = models.Station(
                name = row.station_name,
                longitude = row.grid_longitude,
                lattitude = row.grid_latitude,
                grid_i = row.grid_i,
                grid_j = row.grid_j,
                grid_height = row.grid_height,
            )
            out.append(obj)
        models.Station.objects.bulk_create(out)
    
    out = []
    if len(models.Unit.objects.all()) > 0:
        pass
    else:
        for row in df_units.itertuples():
            obj = models.Unit(
                parameter = row.weather_param,
                unit = row.unit,
            )
            out.append(obj)
        models.Unit.objects.bulk_create(out)

    out = []
    for row in df_data.itertuples():
        print(row)
        current_entry = models.Entry.objects.filter(
            station_id=row.stn,
            weather_param_id=row.weather_param,
            time=row.time,
            member=row.member
        )
        if len(current_entry) > 0:
            current_entry.update(leadtime=row.leadtime, value=row.forecasted_value)
        else:
            obj = models.Entry(
                station_id = row.stn,
                weather_param_id = row.weather_param,
                time = row.time,
                member = row.member,
                leadtime = row.leadtime,
                value = row.forecasted_value,
            )
            obj.validate_unique()
            out.append(obj)

    models.Entry.objects.bulk_create(out)

    return len(out)