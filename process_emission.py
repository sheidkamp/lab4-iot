import json
import logging
import sys

import greengrasssdk

co2_maxes = {}

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# SDK Client
client = greengrasssdk.client("iot-data")

# Counter

def lambda_handler(event, context):
    global co2_maxes

    #TODO1: Get your data
    # Message looks like:
    # {
    #     "thing_id": thing_num,
    #     "co2_level": co2_level,
    #     "timestep": data[timestep_index]
    # }
    thing_id = event['thing_id']
    co2_level = event['co2_level']

    topic = f'lab4/carbon-data/{thing_id}'

    #TODO2: Calculate max CO2 emission
    current_max = co2_maxes.get(thing_id, "nil") 
    if current_max == 'nil' or co2_level > current_max:
        co2_maxes[thing_id] = co2_level
        message = {"max_co2": co2_level}
        messageJson = json.dumps(message)
        print(f'{thing_id}: New max {co2_level} more than {current_max}')
        client.publish(topic=topic,payload=messageJson)
        print('Published topic %s: %s\n' % (topic, messageJson))
    else:
        print(f'{thing_id}: {current_max} more than {co2_level}')