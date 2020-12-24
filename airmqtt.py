import paho.mqtt.client as mqtt
import urllib.request, json
import logging, os, time

def from_env_var(env_var):
  if env_var in os.environ:
    return os.environ[env_var]
  else:
    print(env_var, "env var must be set")
    exit(1)

def from_env_var_int(env_var):
  if env_var in os.environ:
    return int(os.environ[env_var])
  else:
    print(env_var, "env var must be set")
    exit(1)

def from_env_var_optional(env_var):
  if env_var in os.environ:
    return os.environ[env_var]
  else:
    pass

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

sensor_url = from_env_var("AIRMQTT_SENSOR_URL")
mqtt_host = from_env_var("AIRMQTT_HOST")
mqtt_port = from_env_var_int("AIRMQTT_PORT")
mqtt_username = from_env_var("AIRMQTT_USERNAME")
mqtt_password = from_env_var("AIRMQTT_PASSWORD")
pm_10_topic = from_env_var_optional("AIRMQTT_PM10_TOPIC")
pm_25_topic = from_env_var_optional("AIRMQTT_PM25_TOPIC")
temperature_topic = from_env_var_optional("AIRMQTT_TEMPERATURE_TOPIC")
pressure_topic = from_env_var_optional("AIRMQTT_PRESSURE_TOPIC")
humidity_topic = from_env_var_optional("AIRMQTT_HUMIDITY_TOPIC")

while True:
  client = mqtt.Client("airbg")
  client.username_pw_set(username=mqtt_username, password=mqtt_password)
  client.connect(mqtt_host, mqtt_port)
  logging.info("mqtt connected to %s:%s", mqtt_host, mqtt_port)
  
  with urllib.request.urlopen(sensor_url) as url:
    data = json.loads(url.read().decode())
    logging.info("sensor data downloaded from %s", sensor_url)

    if pm_10_topic:
      pm_10_value = data["sensordatavalues"][0]["value"]
      client.publish(pm_10_topic, pm_10_value)
      logging.info("published PM10 value of %s to topic %s", pm_10_value, pm_10_topic)

    if pm_25_topic:
      pm_25_value = data["sensordatavalues"][1]["value"]
      client.publish(pm_25_topic, pm_25_value)
      logging.info("published PM2.5 value of %s to topic %s", pm_25_value, pm_25_topic)

    if temperature_topic:
      temperature_value = data["sensordatavalues"][2]["value"]
      client.publish(temperature_topic, temperature_value)
      logging.info("published Temperature value of %s to topic %s", temperature_value, temperature_topic)

    if pressure_topic:
      pressure_value = data["sensordatavalues"][3]["value"]
      client.publish(pressure_topic, pressure_value)
      logging.info("published Pressure value of %s to topic %s", pressure_value, pressure_topic)

    if humidity_topic:
      humidity_value = data["sensordatavalues"][4]["value"]
      client.publish(humidity_topic, humidity_value)
      logging.info("published Humidity value of %s to topic %s", humidity_value, humidity_topic)

    client.disconnect()
    logging.info("mqtt disconnected")

    interval_value = int(data["sensordatavalues"][8]["value"]) / 1000
    logging.info("sleeping for %d seconds", interval_value)
    time.sleep(interval_value)
