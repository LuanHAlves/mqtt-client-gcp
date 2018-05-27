# ************************************************************************************
# Descricao: Script para conectar a Raspberry PI a Google IoT Core via protocolo mqtt
# Autor: Google Inc
# Data: 2018/03/08
# Modificado por: Luan H Alves
# Versao 1.2
# ************************************************************************************


import ssl
import time
import datetime
from random import uniform, randint, random

import jwt
import json
import paho.mqtt.client as mqtt

credentials = json.loads(open("credentials.json").read())

minimum_backoff_time = 1
MAXIMUM_BACKOFF_TIME = 32
should_backoff = False

project_id = credentials["project_id"]
registry_id = credentials["registry_id"]
device_id = credentials["device_id"]
cloud_region = credentials["cloud_region"]
algorithm = credentials["algorithm"]
ca_certs = credentials["ca_certs"]
private_key_file = credentials["private_key_file"]
mqtt_bridge_hostname = credentials["mqtt_bridge_hostname"]
mqtt_bridge_port = credentials["mqtt_bridge_port"]


# [START iot_mqtt_jwt]
def create_jwt(project_id, private_key_file, algorithm):

    token = {
            'iat': datetime.datetime.utcnow(),
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
            'aud': project_id
    }
    with open(private_key_file, 'r') as f:
        private_key = f.read()

    print('Creating JWT using {} from private key file {}'.format(
            algorithm, private_key_file))

    return jwt.encode(token, private_key, algorithm=algorithm)
# [END iot_mqtt_jwt]


# [START iot_mqtt_config]
def error_str(rc):
    #Convert a Paho error to a human readable string
    return '{}: {}'.format(rc, mqtt.error_string(rc))


def on_connect(unused_client, unused_userdata, unused_flags, rc):
    
    print('on_connect', mqtt.connack_string(rc))
    global should_backoff
    global minimum_backoff_time
    should_backoff = False
    minimum_backoff_time = 1


def on_disconnect(unused_client, unused_userdata, rc):
    print('on_disconnect', error_str(rc))

    global should_backoff
    should_backoff = True


def on_publish(unused_client, unused_userdata, unused_mid):
    print('on_publish')


def on_message(unused_client, unused_userdata, message):
    payload = str(message.payload)
    print('Received message \'{}\' on topic \'{}\' with Qos {}'.format(
            payload, message.topic, str(message.qos)))


def on_log(client, userdata, level, msg):
    print(msg)


def get_client(project_id, cloud_region, registry_id, device_id, private_key_file, 
    algorithm, ca_certs, mqtt_bridge_hostname, mqtt_bridge_port):
    
    client = mqtt.Client(client_id=('projects/{}/locations/{}/registries/{}/devices/{}'
        .format(project_id, cloud_region, 'raspi', device_id)))

    client.username_pw_set(username='unused',password=create_jwt(project_id, private_key_file, algorithm))

    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_log = on_log

    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    mqtt_config_topic = '/devices/{}/config'.format(device_id)

    client.subscribe(mqtt_config_topic, qos=1)

    return client
# [END iot_mqtt_config]


def Time():
    str_time = datetime.datetime.today().strftime('%H:%M:%S')
    return str_time


def timestamp():
    str_timestamp = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    return str_timestamp


# [START iot_mqtt_run]
def main():
    global minimum_backoff_time

    num_messages = int(input("num_messages: "))
    
    client = get_client(project_id, cloud_region, project_id, device_id, private_key_file,
                            algorithm, ca_certs, mqtt_bridge_hostname, mqtt_bridge_port)

    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    mqtt_topic = ('/devices/{}/events'.format(device_id))

    
    for i in range(0, num_messages): # While True
        
        client.loop()
        
        # Wait if backoff is required.
        if should_backoff:
            # If backoff time is too large, give up.
            if (minimum_backoff_time > MAXIMUM_BACKOFF_TIME):
                print('Exceeded maximum backoff time. Giving up.')
                break

            # Otherwise, wait and connect again.
            delay = minimum_backoff_time + random.randint(0, 1000)/1000.0
            print('Waiting for {} before reconnecting.'.format(delay))
            time.sleep(delay)
            minimum_backoff_time *= 2
        
        try:

            # DADOS SIMULADOS PARA OS TESTES
            sensorValues = {
                            "state": "true",
                            "gateway": randint(1, 2),
                            "node": randint(1, 10),
                            "timestamp": timestamp(),
                            "time": Time(),
                            "QY": round(uniform(0.6, 0.78), 4),
                            "temperature": round(uniform(17.0, 18.0), 1),
                            "location": str(-19.883971) +"," + str(-44.415545)
                           }
                           
            state = sensorValues["state"]
            gateway = sensorValues["gateway"]
            node = sensorValues["node"]
            date = sensorValues["timestamp"]
            hour = sensorValues["time"]
            qy = sensorValues["QY"]
            temperature = sensorValues["temperature"]
            location = sensorValues["location"]

            DATA = {
                    "gateway_id": gateway,
                    "node_id": node,
                    "state": state,
                    "datetime": date,
                    "QY": qy,
                    "temperature": temperature,
                    "location": location
            }
            payload = json.dumps(DATA)

            print('\nMsg Publicada {}/{}:\n{}\''.format(1+i, num_messages, payload))

            client.publish(mqtt_topic, payload, qos=1)
        
            time.sleep(2)

        except Exception as e:
            client.disconnect()
            print("[Exc01] Gateway desconectado. %s" % str(e))

# [END iot_mqtt_run]


if __name__ == '__main__':
    main()
