# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np
from collections import defaultdict
import math

#emissions_iam
#arn:aws:iam::737214208252:role/service-role/emissions_iam

#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 0
device_end = 5

#Path to the dataset, modify this
data_path = "data2/vehicle{}.csv"
#ENDPOINT a21fbu4zq0j8ok-ats.iot.us-east-2.amazonaws.com

#Path to your certificates, modify this
certificate_formatter = "certificates/v{}-certificate.pem.crt"
key_formatter = "certificates/v{}-private.pem.key"

trigger_topic = "all/emissions/trigger"
shared_topic = "all/emissions"
own_topic = "v{}/emissions"

class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        #TODO 2: modify your broker address
        self.client.configureEndpoint("a21fbu4zq0j8ok-ats.iot.us-east-2.amazonaws.com", 8883)
        self.client.configureCredentials("certificates/core_root.cer", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        #TODO 3: fill in the function to show your received message
        print("client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, message):
        #TODO 4: fill in this function for your publish
        # self.client.subscribeAsync("myTopic", 0, ackCallback=self.customSubackCallback)
        # self.client.publishAsync("myTopic", Payload, 0, ackCallback=self.customPubackCallback)
        messageJson = json.dumps(message)
        self.client.subscribeAsync(trigger_topic, 0, ackCallback=self.customSubackCallback)
        self.client.publishAsync(trigger_topic, messageJson, 0, ackCallback=self.customPubackCallback)


print("Loading vehicle data...")
data = []
for i in range(5):
    a = pd.read_csv(data_path.format(i))
    data.append(a)

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    cert_f = certificate_formatter.format(device_id)
    key_f = key_formatter.format(device_id)
    print(cert_f, key_f)
    client = MQTTClient(device_id, cert_f,key_f)
    client.client.connect()
    #SUBSCRIBING TO ALL EMISSIONS AND TO OWN EMISSIONS
    client.client.subscribeAsync(shared_topic, 0)
    client.client.subscribeAsync(own_topic.format(device_id), 0)
    clients.append(client)

#TODO subscribe all clients to their particular thread
#TODO subscribe all clients to shared thread

readings = defaultdict(lambda: [])
minlen = math.inf
for i in range(device_st, device_end):
    with open('data2/vehicle'+str(i)+'.csv', 'r') as f:
        rows = f.read().split('\n')
        readings[i] = rows
        currlen = len(rows)
        if currlen < minlen:
            minlen = currlen

for r in range(1,minlen-1):
    for i in range(device_st, device_end):
        code = "v"+str(i)
        v_client = clients[i]
        message = {}
        message['vnum'] = code
        row_parsed = readings[i][r].split(',')
        message['emission'] = row_parsed[2]
        v_client.publish(message)

while True:
    x = input()
    if x == "stop":
        break

# while True:
#     print("send now?")
#     x = input()
#     if x == "s":
#         for i,c in enumerate(clients):
#             c.publish()
#             print()
#     elif x == "d":
#         for c in clients:
#             c.client.disconnect()
#         print("All devices disconnected")
#         exit()
#     else:
#         print("wrong key pressed")

#     time.sleep(3)





