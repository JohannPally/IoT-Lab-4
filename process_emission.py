import json
import logging
import sys
import time

import greengrasssdk

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# SDK Client
client = greengrasssdk.client("iot-data")

# Counter
my_counter = 0

vehicle_data = {"v0": 0.0,
                "v1": 0.0,
                "v2": 0.0,
                "v3": 0.0,
                "v4": 0.0}


def lambda_handler(event, context):
    global my_counter
    global vehicle_data
    #TODO1: Get your data

    # print(event)

    vnum = event.get('vnum')
    em_str = event.get('emission')
    emission = -1.0
    max_emission = -1.0

    try:
        emission = float(em_str)

        # #TODO2: Calculate max CO2 emission

        if emission > vehicle_data[vnum]:
            vehicle_data[vnum] = emission
        #     #TODO publish max? or publish whatever is in the vehicle_data storage

        max_emission = vehicle_data[vnum] 
    except Exception as e:
        print(e)
    
    # #TODO3: Return the result

    # client.publish(
    #     topic="all/emissions",
    #     queueFullPolicy="AllOrException",
    #     payload=json.dumps(
    #         {"message": "hi. test"}
    #     ),
    # )
    try:
        client.publish(
            topic= "all/emissions",
            payload=json.dumps(
                {"message": "{} most recent reading from "+vnum+" : "+str(emission)+"".format(my_counter)}
            ),
        )
        #this is to one vehicle
        client.publish(
            topic= vnum+"/emissions",
            payload=json.dumps(
                {"message": "{} all time maximum "+vnum+" : "+str(max_emission)+"".format(my_counter)}
            ),
        )
    except Exception as e:
        print(e)

    # my_counter += 1

    # client.publish(
    #     topic="hello/world/counter",
    #     payload=json.dumps(
    #         {"message": "Hello world! Sent from Greengrass Core.  Invocation Count: {}".format(my_counter)}
    #     ),
    # )

    #TODO stop sleeping?
    my_counter += 1
    # time.sleep(5)
    return