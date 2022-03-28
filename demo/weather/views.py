import weather.models as models
import weather.helper as helper
import weather.preprocess as preprocess
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.core.handlers.wsgi import WSGIRequest
import requests
import json
from django.core import serializers


def scraping_data(
    request: WSGIRequest,
) -> HttpResponse:
    # Download content and initialize dataframes
    CSV_URL = helper.Url.CSV_URL
    rows_to_keep_ref = list(range(17,24))
    rows_to_keep_units = list(range(24,26))

    with requests.get(CSV_URL, stream=True) as r:
        r_content = r.content

        df_stations = helper.csv_to_pd(
            r_content,
            skiprows=lambda x: x not in rows_to_keep_ref,
        )

        df_units = helper.csv_to_pd(
            r_content,
            skiprows=lambda x: x not in rows_to_keep_units,
        )

        df_data = helper.csv_to_pd(
            r_content,
            skiprows=[*list(range(24)),25,26],
        ) 

    # Process dataframes before DB write
    df_stations, df_units, df_data = preprocess.process_df(df_stations, df_units, df_data)

    # Write scraped data into backend
    uploaded_rows = preprocess.write_to_db(df_stations, df_units, df_data)

    return HttpResponse(
        json.dumps({'Weather data successfully updated. New rows added': uploaded_rows}),
        'application/json'
    )


def get_all(
    request: WSGIRequest,
) -> JsonResponse:
    # Filter for temp and precipitation
    objects_to_return = models.Entry.objects.filter( \
        weather_param_id=(helper.WeatherParameters.TEMPERATURE, helper.WeatherParameters.TOTAL_PRECIPITATION)
    )

    # serialize to return
    json_data = serializers.serialize("json", objects_to_return.all())
    
    return JsonResponse(json_data, safe=False)


def get_most_recent(
    request: WSGIRequest,
) -> JsonResponse:
    # Most recent date
    recent_date = models.Entry.objects.latest(helper.DataCols.TIME).time.date()
    objects_to_return = models.Entry.objects.filter(time=recent_date)

    # Filter for temp and precipitation
    objects_to_return = objects_to_return.filter( \
        weather_param_id=(helper.WeatherParameters.TEMPERATURE, helper.WeatherParameters.TOTAL_PRECIPITATION)
    )

    # serialize to return
    json_data = serializers.serialize("json", objects_to_return)
    
    return JsonResponse(json_data, safe=False)

