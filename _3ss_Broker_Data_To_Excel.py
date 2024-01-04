import json
import os
import pandas as pd
from paho.mqtt import client as mqtt_client
import ssl
import time

# Configuration
broker = '[Broker Address]'
port = [Port]
topic = '[Topic]'
client_id = 'mqtt_client'
output_directory = '[Output location for data]'

# Paths to certificate files
client_certificate = '[location]'
client_private_key = '[location]'
ca_certificate = '[location]'

# Store metadata, measuredvalues, and devicehealth in separate lists
metadata_list = []
measuredvalues_list = []
devicehealth_list = []

# MQTT callback functions
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(topic)
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        topic_parts = msg.topic.split('/')
        asset_name = topic_parts[4]
        message_type = topic_parts[-1]

        if asset_name:
            # Replace any characters that are not allowed in folder names
            allowed_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
            asset_folder_name = ''.join(c if c in allowed_chars else '_' for c in asset_name)

            # Create the main folder if it doesn't exist
            main_folder = os.path.join(output_directory, asset_folder_name)
            if not os.path.exists(main_folder):
                os.makedirs(main_folder)

            # Create the subfolder if it doesn't exist inside the main folder
            subfolder = os.path.join(main_folder, topic_parts[5])
            if not os.path.exists(subfolder):
                os.makedirs(subfolder)

            # Save data to a JSON file inside the subfolder
            filename = os.path.join(subfolder, f"{message_type}.json")
            with open(filename, 'w') as json_file:
                json.dump(json.loads(payload), json_file)
            print(f"Saved {message_type} to {filename}")

            # Store data in the appropriate list based on message_type
            if message_type == 'metadata':
                metadata_list.append(json.loads(payload))
            elif message_type == 'measuredvalues':
                measuredvalues_list.append(json.loads(payload))
            elif message_type == 'devicehealth':
                devicehealth_list.append(json.loads(payload))

        else:
            print("Invalid topic structure")

    except Exception as e:
        print(f"Error processing message: {e}")

# MQTT client setup with TLS/SSL
client = mqtt_client.Client(client_id)
client.on_connect = on_connect
client.on_message = on_message

# Set TLS/SSL options
client.tls_set(
    ca_certs=ca_certificate,
    certfile=client_certificate,
    keyfile=client_private_key,
    cert_reqs=ssl.CERT_REQUIRED,
    tls_version=ssl.PROTOCOL_TLS,
)

# Connect to the broker
client.connect(broker, port)

# Start the MQTT client loop
client.loop_start()

# Sleep for some time to collect JSON files (you can adjust this time as needed)
time.sleep(60)  # Sleep for 60 seconds, for example

# Disconnect from the broker
client.disconnect()

# Create an Excel sheet with metadata, measuredvalues, and devicehealth in separate tabs
with pd.ExcelWriter(os.path.join(output_directory, 'data_output.xlsx'), engine='xlsxwriter') as writer:
    if metadata_list:
        metadata_df = pd.DataFrame(metadata_list)
        metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
    if measuredvalues_list:
        measuredvalues_df = pd.DataFrame(measuredvalues_list)
        measuredvalues_df.to_excel(writer, sheet_name='MeasuredValues', index=False)
    if devicehealth_list:
        devicehealth_df = pd.DataFrame(devicehealth_list)
        devicehealth_df.to_excel(writer, sheet_name='DeviceHealth', index=False)

print("Data saved to data_output.xlsx")
