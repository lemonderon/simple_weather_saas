import urllib
import datetime as dt
from dateutil import relativedelta
import json

import requests
from flask import Flask, jsonify, request

import functools
import operator


def foldl(func, acc, xs): return functools.reduce(func, xs, acc)


API_TOKEN = ""

app = Flask(__name__)


def get_weather_historical_output(locationDetails, date):
    date_for_weather_obj = dt.datetime.strptime(date, '%Y-%m-%d')

    dateDelta = relativedelta.relativedelta(date_for_weather_obj, dt.datetime.now())
    approx_n_days_in_5_month = 150
    isForForecastAPI = (dt.datetime.now() - date_for_weather_obj).days <= approx_n_days_in_5_month

    weather_api_base_url = "https://api.open-meteo.com/v1" if isForForecastAPI else "https://archive-api.open-meteo.com/v1"
    weather_api_url_endpoint = "forecast" if isForForecastAPI else "archive"
    weather_api_hourly_info_url_params = "relativehumidity_2m,surface_pressure,windspeed_10m"
    weather_api_daily_info_url_params = "sunrise,sunset,rain_sum,windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,temperature_2m_max,temperature_2m_min"

    lat = locationDetails.get("locLat")
    loc = locationDetails.get("locLon")
    timezone = locationDetails.get("timezone")

    response = requests.request("GET",f"{weather_api_base_url}/{weather_api_url_endpoint}?latitude={lat}&longitude={loc}&start_date={date}&end_date={date}&hourly={weather_api_hourly_info_url_params}&daily={weather_api_daily_info_url_params}&timezone={timezone}", headers={}, data={})
    

    hours_in_day = 24
    mmHg_to_mBar_mult = 1.33322387415

    weather_hourly_dets = response.json().get("hourly")

    mean_humidity = (foldl(operator.add, 0, weather_hourly_dets.get( "relativehumidity_2m"))) / hours_in_day
    mean_surface_pressure_mmHg = (foldl(operator.add, 0, weather_hourly_dets.get("surface_pressure"))) / hours_in_day
    mean_surface_pressure_mBar = mean_surface_pressure_mmHg * mmHg_to_mBar_mult
    mean_wind_speed = (foldl(operator.add, 0, weather_hourly_dets.get("windspeed_10m"))) / hours_in_day


    weather_day_dets = response.json().get("daily")

    max_temperature = weather_day_dets.get("temperature_2m_max")[0]
    min_temperature = weather_day_dets.get("temperature_2m_min")[0]
    sunrise = weather_day_dets.get("sunrise")[0]
    sunset = weather_day_dets.get("sunset")[0]
    rain_sum = weather_day_dets.get("rain_sum")[0]
    max_wind_speed = weather_day_dets.get("windspeed_10m_max")[0]
    max_wind_gusts = weather_day_dets.get("windgusts_10m_max")[0]
    dominant_wind_direction = weather_day_dets.get("winddirection_10m_dominant")[0]


    weather_info = {
        "max_temp_c": max_temperature,
        "min_temp_c": min_temperature,
        "wind_kph": "%.1f" % mean_wind_speed,
        "wind_max_kph": max_wind_speed,
        "wind_max_gusts": max_wind_gusts,
        "wind_dominant_direction": dominant_wind_direction,
        "pressure_mb": "%.1f" % mean_surface_pressure_mBar,
        "humidity": "%.1f" % mean_humidity,
        "rain_sum": rain_sum,
        "sunrise": sunrise,
        "sunset": sunset,
    }

    return weather_info


def get_location_details(location):
    geocoding_api_base_url = "https://geocoding-api.open-meteo.com/v1"
    geocoding_api_url_endpoint = "search"
    geocoding_api_res_count = 1

    responseCoords = requests.request("GET", f"{geocoding_api_base_url}/{geocoding_api_url_endpoint}?name={location}&count={geocoding_api_res_count}", headers={}, data={})

    locationDets = responseCoords.json().get("results")[0]
    locLat = locationDets.get("latitude")
    locLon = locationDets.get("longitude")
    name = locationDets.get("name")
    country = locationDets.get("country")
    country_code = locationDets.get("country_code")
    timezone = locationDets.get("timezone")

    location_details = {
        "locLat": locLat,
        "locLon": locLon,
        "name": name,
        "country": country,
        "country_code": country_code,
        "timezone": timezone,
    }

    return location_details


class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv["message"] = self.message
        return rv


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route("/")
def home_page():
    return "<p><h2>KMA PW01: Weather API Saas.</h2></p>"


@app.route(
    "/api/v1/weather",
    methods=["POST"],
)
def weather_endpoint():
    json_data = request.get_json()

    if json_data.get("token") is None:
        raise InvalidUsage("token is required", status_code=400)

    api_token = json_data.get("token")

    if api_token != API_TOKEN:
        raise InvalidUsage("invalid API token", status_code=403)

    requester_name = json_data.get("requester_name")
    location = json_data.get("location")
    weather_for_date = json_data.get("date")

    locationDetails = get_location_details(location)
    weather = get_weather_historical_output(locationDetails, weather_for_date)

    result = {
        "requester_name": requester_name,
        "timestamp": dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "location": locationDetails,
        "date": weather_for_date,
        "weather": weather,
    }

    return result
