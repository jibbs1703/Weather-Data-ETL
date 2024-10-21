from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from dotenv import load_dotenv
from aws_resources.s3 import S3Buckets
import requests
import os, yaml
import pandas as pd

# Access .env File
load_dotenv()

# Access Config File
with open('/opt/airflow/config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

default_args = {
    'owner': 'Abraham Ajibade',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email': ['abraham0ajibade@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}


@dag(
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
)
def WeatherDataETL():
    # LOCATION: Obtain the location for which the data would be collected
    @task(task_id="get_coordinates")
    def address_to_coordinates(address=config['location'], base_url='https://geocode.maps.co/search?',
                               api_key=os.getenv("COORDINATES_KEY")):
        coordinates = {}
        url = f"{base_url}q={address.replace(' ', ',')}&api_key={api_key}"
        response = requests.get(url)
        json_data = response.json()[0]
        coordinates['latitude'] = json_data['lat']
        coordinates['longitude'] = json_data['lon']
        return coordinates

    @task(task_id="get_weather_api_status")
    def weather_data_sensor(coordinates: dict, base_url='https://api.openweathermap.org/data/2.5/weather?',
                            key=os.getenv("WEATHER_KEY")):
        # Get Coordinates as output from task1
        lon = coordinates['longitude']
        lat = coordinates['latitude']
        url = f'{base_url}lat={lat}&lon={lon}&appid={key}&units=imperial'
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
            else:
                return False

        except requests.exceptions.RequestException as e:
            print(f"HTTP request failed: {e}")
            return False

    @task(task_id="get_aqi_api_status")
    def aqi_data_sensor(coordinates: dict, base_url='http://api.openweathermap.org/data/2.5/air_pollution?',
                        key=os.getenv("WEATHER_KEY")):
        # Get Coordinates as output from task1
        lon = coordinates['longitude']
        lat = coordinates['latitude']
        url = f'{base_url}lat={lat}&lon={lon}&appid={key}&units=imperial'
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return True
            else:
                return False

        except requests.exceptions.RequestException as e:
            print(f"HTTP request failed: {e}")
            return False

    # Extract Data After Api Check
    @task(task_id="get_weather_data")
    def extract_weather_data(api_status: bool, coordinates: dict,
                             base_url='https://api.openweathermap.org/data/2.5/weather?', key=os.getenv("WEATHER_KEY")):
        # Get Coordinates and Response Status Output from Previous Tasks
        lon = coordinates['longitude']
        lat = coordinates['latitude']
        url = f'{base_url}lat={lat}&lon={lon}&appid={key}&units=imperial'
        if api_status == True:
            response = requests.get(url)
            json_data = response.json()
            print('JSON file has been collected')
            return json_data
        else:
            return print('No JSON file has been collected')

    @task(task_id="get_aqi_data")
    def extract_air_quality_data(api_status: bool, coordinates: dict,
                                 base_url='http://api.openweathermap.org/data/2.5/air_pollution?',
                                 key=os.getenv("WEATHER_KEY")):
        # Get Coordinates as output from task1
        # Get Coordinates and Response Status Output from Previous Tasks
        lon = coordinates['longitude']
        lat = coordinates['latitude']
        url = f'{base_url}lat={lat}&lon={lon}&appid={key}&units=imperial'
        if api_status == True:
            response = requests.get(url)
            json_data = response.json()
            print('JSON file has been collected')
            return json_data
        else:
            return print('No JSON file has been collected')

    @task(task_id="transform_weather_data")
    def transform_weather_data(weather_data):

        city = [weather_data["name"]]
        latitude = [weather_data['coord']['lat']]
        longitude = [weather_data['coord']['lon']]
        date = [
            datetime.fromtimestamp(weather_data['dt'] + weather_data['timezone'], tz=timezone.utc).date().isoformat()]
        time = [
            datetime.fromtimestamp(weather_data['dt'] + weather_data['timezone'], tz=timezone.utc).time().isoformat()]
        description = [weather_data["weather"][0]['description']]
        temperature = [weather_data["main"]["temp"]]
        temperature_feel = [weather_data["main"]["feels_like"]]
        minimum_temp = [weather_data["main"]["temp_min"]]
        maximum_temp = [weather_data["main"]["temp_max"]]
        pressure = [weather_data["main"]["pressure"]]
        humidity = [weather_data["main"]["humidity"]]
        wind_speed = [weather_data["wind"]["speed"]]
        sunrise = [datetime.fromtimestamp(weather_data['sys']['sunrise'] + weather_data['timezone'],
                                          tz=timezone.utc).time().isoformat()]
        sunset = [datetime.fromtimestamp(weather_data['sys']['sunset'] + weather_data['timezone'],
                                         tz=timezone.utc).time().isoformat()]

        weather_data = {"City": city,
                        "Latitude": latitude,
                        "Longitude": longitude,
                        "Date": date,
                        "Record Time": time,
                        "Description": description,
                        "Temperature": temperature,
                        "Temperature Feel (F)": temperature_feel,
                        "Minimum Temperature (F)": minimum_temp,
                        "Maximum Temperature (F)": maximum_temp,
                        "Pressure": pressure,
                        "Humidity": humidity,
                        "Wind Speed": wind_speed,
                        "Sunrise (Local Time)": sunrise,
                        "Sunset (Local Time)": sunset}
        data = pd.DataFrame.from_dict(weather_data)

        return data

    @task(task_id="transform_aqi_data")
    def transform_air_quality_data(aqi_data):

        date = [datetime.fromtimestamp(aqi_data['list'][0]['dt'] + (-14400), tz=timezone.utc).date().isoformat()]
        time = [datetime.fromtimestamp(aqi_data['list'][0]['dt'] + (-14400), tz=timezone.utc).time().isoformat()]
        aqi_value = [aqi_data['list'][0]['main']['aqi']]
        co = [aqi_data['list'][0]['components']['co']]
        no = [aqi_data['list'][0]['components']['no']]
        no2 = [aqi_data['list'][0]['components']['no2']]
        so2 = [aqi_data['list'][0]['components']['so2']]
        nh3 = [aqi_data['list'][0]['components']['nh3']]
        pm2_5 = [aqi_data['list'][0]['components']['pm2_5']]
        pm10 = [aqi_data['list'][0]['components']['pm10']]

        aqi_data = {"AQI": aqi_value,
                    "Date": date,
                    "Record Time": time,
                    "co": co,
                    "no": no,
                    "no2": no2,
                    "so2": so2,
                    "nh3": nh3,
                    "pm2_5": pm2_5,
                    "pm10": pm10}

        data = pd.DataFrame.from_dict(aqi_data)

        return data

    @task
    def load_weather_data(transformed_weather_data, current_time=datetime.now().strftime("%d%m%Y%H%M%S")):

        # Initialize S3 Bucket To Load Data
        s3 = S3Buckets.credentials('us-east-2')

        # Creates Bucket For Collecting Hourly Data (Creates Only During the First Collection Schedule)
        s3.create_bucket(bucket_name="weather-data-landing-bucket")

        # Upload the Transformed Data to an S3 Bucket
        s3.upload_dataframe_to_s3(transformed_weather_data, 'weather-data-landing-bucket',
                                  f'weather_data_{current_time}.csv')

        return f'weather_data_{current_time} was successfully uploaded to weather-data-landing-bucket '

    @task
    def load_air_quality_data(transformed_air_quality_data, current_time=datetime.now().strftime("%d%m%Y%H%M%S")):

        # Initialize S3 Bucket To Load Data
        s3 = S3Buckets.credentials('us-east-2')

        # Creates Bucket For Collecting Hourly Data (Creates Only During the First Collection Schedule)
        s3.create_bucket(bucket_name="aqi-data-landing-bucket")

        # Upload the Transformed Data to an S3 Bucket
        s3.upload_dataframe_to_s3(transformed_air_quality_data, 'aqi-data-landing-bucket',
                                  f'aqi_data_{current_time}.csv')

        return f'aqi_data_{current_time} was successfully uploaded to aqi-data-landing-bucket '

    # Set Tasks Dependencies
    coordinates = address_to_coordinates()
    weather_api_check = weather_data_sensor(coordinates)
    aqi_api_check = aqi_data_sensor(coordinates)
    air_quality_data = extract_air_quality_data(aqi_api_check, coordinates)
    weather_data = extract_weather_data(weather_api_check, coordinates)
    transformed_air_quality_data = transform_air_quality_data(air_quality_data)
    transformed_weather_data = transform_weather_data(weather_data)
    load_weather_data(transformed_weather_data)
    load_air_quality_data(transformed_air_quality_data)


# Instantiate DAG
WeatherDataETL()