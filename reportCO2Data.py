# /*
# * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# *
# * Licensed under the Apache License, Version 2.0 (the "License").
# * You may not use this file except in compliance with the License.
# * A copy of the License is located at
# *
# *  http://aws.amazon.com/apache2.0
# *
# * or in the "license" file accompanying this file. This file is distributed
# * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# * express or implied. See the License for the specific language governing
# * permissions and limitations under the License.
# */


import os
import sys
import time
import uuid
import json
import logging
import argparse
from AWSIoTPythonSDK.core.greengrass.discovery.providers import DiscoveryInfoProvider
from AWSIoTPythonSDK.core.protocol.connection.cores import ProgressiveBackOffCore
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception.AWSIoTExceptions import DiscoveryInvalidRequestException

AllowedActions = ['both', 'publish', 'subscribe']

# General message notification callback
def customOnMessage(message):
    print('Received message on topic %s: %s\n' % (message.topic, message.payload))

MAX_DISCOVERY_RETRIES = 10
GROUP_CA_PATH = "./groupCA/"

# Read in command-line parameters
parser = argparse.ArgumentParser()
# parser.add_argument("-e", "--endpoint", action="store", required=True, dest="host", help="Your AWS IoT custom endpoint")
# parser.add_argument("-r", "--rootCA", action="store", required=True, dest="rootCAPath", help="Root CA file path")
# parser.add_argument("-c", "--cert", action="store", dest="certificatePath", help="Certificate file path")
# parser.add_argument("-k", "--key", action="store", dest="privateKeyPath", help="Private key file path")
# parser.add_argument("-n", "--thingName", action="store", dest="thingName", default="Bot", help="Targeted thing name")
# parser.add_argument("-t", "--publish_data_topic", action="store", dest="publish_data_topic", default="sdk/test/Python", help="Targeted publish_data_topic")
# parser.add_argument("-m", "--mode", action="store", dest="mode", default="both",
#                     help="Operation modes: %s"%str(AllowedActions))
# parser.add_argument("-M", "--message", action="store", dest="message", default="Hello World!",
#                     help="Message to publish")
# #--print_discover_resp_only used for delopyment testing. The test run will return 0 as long as the SDK installed correctly.
# parser.add_argument("-p", "--print_discover_resp_only", action="store_true", dest="print_only", default=False)
parser.add_argument("-i", "--vehicle_num", action="store", dest="thing_num")

args = parser.parse_args()
thing_num = args.thing_num
host = "a3p3gws86ciu4o-ats.iot.us-east-2.amazonaws.com" #args.host
rootCAPath = 'AmazonRootCA1.pem' #args.rootCAPath
certificatePath = f'certificates/device_{thing_num}/device_{thing_num}.certificate.pem' #args.certificatePath
privateKeyPath = f'certificates/device_{thing_num}/device_{thing_num}.private.pem' #args.privateKeyPath
clientId = f'device_{thing_num}'
thingName = f'device_{thing_num}'
publish_data_topic = 'lab4/carbon-data' #args.publish_data_topic
subscribe_data_topic = f'lab4/carbon-data/{thing_num}'
print_only = False #args.print_only

# if args.mode not in AllowedActions:
#     parser.error("Unknown --mode option %s. Must be one of %s" % (args.mode, str(AllowedActions)))
#     exit(2)

if not certificatePath or not privateKeyPath:
    parser.error("Missing credentials for authentication, you must specify --cert and --key args.")
    exit(2)

if not os.path.isfile(rootCAPath):
    parser.error("Root CA path does not exist {}".format(rootCAPath))
    exit(3)

if not os.path.isfile(certificatePath):
    parser.error("No certificate found at {}".format(certificatePath))
    exit(3)

if not os.path.isfile(privateKeyPath):
    parser.error("No private key found at {}".format(privateKeyPath))
    exit(3)

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.WARN)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# Progressive back off core
backOffCore = ProgressiveBackOffCore()

# Discover GGCs
discoveryInfoProvider = DiscoveryInfoProvider()
discoveryInfoProvider.configureEndpoint(host)
discoveryInfoProvider.configureCredentials(rootCAPath, certificatePath, privateKeyPath)
discoveryInfoProvider.configureTimeout(10)  # 10 sec

retryCount = MAX_DISCOVERY_RETRIES if not print_only else 1
discovered = False
groupCA = None
coreInfo = None
while retryCount != 0:
    try:
        print("looking for ", thingName)
        discoveryInfo = discoveryInfoProvider.discover(thingName)
        caList = discoveryInfo.getAllCas()
        coreList = discoveryInfo.getAllCores()

        # We only pick the first ca and core info
        groupId, ca = caList[0]
        coreInfo = coreList[0]
        print("Discovered GGC: %s from Group: %s" % (coreInfo.coreThingArn, groupId))

        print("Now we persist the connectivity/identity information...")
        groupCA = GROUP_CA_PATH + groupId + "_CA_" + str(uuid.uuid4()) + ".crt"
        if not os.path.exists(GROUP_CA_PATH):
            os.makedirs(GROUP_CA_PATH)
        groupCAFile = open(groupCA, "w")
        groupCAFile.write(ca)
        groupCAFile.close()

        discovered = True
        print("Now proceed to the connecting flow...")
        break
    except DiscoveryInvalidRequestException as e:
        print("Invalid discovery request detected!")
        print("Type: %s" % str(type(e)))
        print("Error message: %s" % str(e))
        print("Stopping...")
        break
    except BaseException as e:
        print("Error in discovery!")
        print("Type: %s" % str(type(e)))
        print("Error message: %s" % str(e))
        retryCount -= 1
        print("\n%d/%d retries left\n" % (retryCount, MAX_DISCOVERY_RETRIES))
        print("Backing off...\n")
        backOffCore.backOff()

if not discovered:
    # With print_discover_resp_only flag, we only woud like to check if the API get called correctly. 
    if print_only:
        sys.exit(0)
    print("Discovery failed after %d retries. Exiting...\n" % (MAX_DISCOVERY_RETRIES))
    sys.exit(-1)

# Iterate through all connection options for the core and use the first successful one
myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient.configureCredentials(groupCA, privateKeyPath, certificatePath)
myAWSIoTMQTTClient.onMessage = customOnMessage

connected = False
for connectivityInfo in coreInfo.connectivityInfoList:
    currentHost = connectivityInfo.host
    currentPort = connectivityInfo.port
    print("Trying to connect to core at %s:%d" % (currentHost, currentPort))
    myAWSIoTMQTTClient.configureEndpoint(currentHost, currentPort)
    try:
        myAWSIoTMQTTClient.connect()
        connected = True
        break
    except BaseException as e:
        print("Error in connect!")
        print("Type: %s" % str(type(e)))
        print("Error message: %s" % str(e))

if not connected:
    print("Cannot connect to core %s. Exiting..." % coreInfo.coreThingArn)
    sys.exit(-2)


def subscribeHandler(_, __, message):
    #print("PYTHON: Got the message: ", messageStr)
    #message = json.loads(message_str.decode('utf-8'))
    #print("PYTHON: new max: ", message.payload.decode('utf-8')) #message["max_co2"])
    payload = json.loads(message.payload.decode('utf-8'))
    print(f'{{thing_num={thing_num} , max_co2={payload["max_co2"]}}}')

if not myAWSIoTMQTTClient.subscribeAsync(subscribe_data_topic, 0, messageCallback=subscribeHandler):
    print("PYTHON: OH NOOOO! No subscribe   ")
time.sleep(2)

# Open data file:
filename = f'vehicle_data/vehicle{thing_num}.csv'
CO2_HEADER = "vehicle_CO2"
with open(filename) as file:
    header_str = file.readline()
    headers = header_str.split(",")
    header_index = { header: i for i,header in enumerate(headers)}
    co2_index = header_index[CO2_HEADER]
    timestep_index = header_index["timestep_time"]

    for line in file: # Does this include the already pulled line?
    #while True:
        data = line.split(",")
        co2_level = data[co2_index]
            
        message = {
            "thing_id": thing_num,
            "co2_level": co2_level,
            "timestep": data[timestep_index]
        }
        # message['message'] = args.message
        # message['sequence'] = loopCount
        messageJson = json.dumps(message)
        if not myAWSIoTMQTTClient.publish(publish_data_topic, messageJson, 0):
            print('PYTHON: !!FAILED!!! topic %s: %s\n' % (publish_data_topic, messageJson))
        else:
            pass
            #print('PYTHON: Published topic %s: %s\n' % (publish_data_topic, messageJson))

        time.sleep(0.1)

    # messageJson = json.dumps({"EOF":True})
    # myAWSIoTMQTTClient.publish(publish_data_topic, messageJson, 0)
    # print('Published publish_data_topic %s: %s\n' % (publish_data_topic, messageJson))
