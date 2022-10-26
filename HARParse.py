import json
import uuid
from optparse import OptionParser
from datetime import datetime
from elasticsearch import Elasticsearch

'''
We need to flatten records the same way that the logstash input mongodb plugin
does. See: https://github.com/phutchins/logstash-input-mongodb/blob/master/lib/logstash/inputs/mongodb.rb#L275
This is because we want to send records to the same index in elastic search.
'''
def flatten_record():

    return
    
# Creates a blank record
def create_record(data, metadata, sessionID, applicationID, flightID):
    data['type'] = "HAR_EVENT"
    data['name'] = "HAR_EVENT"

    record = {
        "eventType": "HAR_EVENT",
        "eventDetails": data,
        "sessionID": sessionID,
        "timestamps":{
            "eventTimestamp": data['startedDateTime']
        }
        "applicationSpecificData":{},
        "metadata": metadata["log"]["pages"],
        "applicationID": applicationID,
        "flightID": flightID 
    }
    return record

ELASTIC_PASSWORD = "" #No password

# Set up argument parser
parser = OptionParser()
parser.add_option("-f", "--file",  dest="input_file", help="HAR file to parse.")
parser.add_option("-s", "--session-id", dest="session_id", help="Corresponding session ID from LogUI if available.", default="00000000-0000-0000-0000-000000000000")
parser.add_option("-a", "--app-id", dest="application_id", help="Corresponding app id from LogUI", default="1ebf4ba1-1783-498b-96c7-4addc383b620")
parser.add_option("-l", "--flight-id", dest="flight_id", help="Corresponding flight id from LogUI", default="00000000-0000-0000-0000-000000000000")
parser.add_option("-i", "--index", dest="es_index", help="The elasticsearch index to send records to.", default="test-index")

(options, args) = parser.parse_args()

# Initalize ES
client = Elasticsearch(
    "http://localhost:9200"
)

# Open and load HAR file
har_file = open(options.input_file)
har_data = json.load(har_file)
entires = har_data['log']['entries']

# Iterate through the entries sending records to elastic search.
for i in entires:
    #print(i['request']['url'])
    record = create_record(i, har_data, options.session_id, options.application_id, options.flight_id)
    #print(json.dumps(record, indent=4))
    es_response = client.index(index=options.es_index, document=record)
    print(es_response['result'])
har_file.close()


print("ES client info:")
print(client.info())



