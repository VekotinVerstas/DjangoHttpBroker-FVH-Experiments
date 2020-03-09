from dateutil.parser import parse

from broker.providers.decoder import DecoderProvider
from broker.utils import create_dataline

"""
Ruuvi Station decoder

Example POST payload:

```
{  
   "deviceId" : "5d09a42c-47bf-bd2b-ac0f-58b41236cf57",
   "time" : "2020-03-09T13:20:09+0200",
   "tags" : [
      {  
         "rssi" : -42,
         "pressure" : 1008.28, 
         "defaultBackground" : 4,
         "favorite" : true,
         "id" : "DA:E5:DD:03:48:1F",
         "humidity" : 28.8,
         "temperature" : 23.54,
         "accelY" : 0.008,
         "rawDataBlob" : {
            "blob" : [
               5, 18, 100, 45, 0, -58, -116, 0, 28, 0, 8, -4, 4, -96, -10, 84, 15, 45, -54, -107, 125, 2, 73, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
         },
         "measurementSequenceNumber" : 11535,
         "updateAt" : "2020-03-09T13:20:09+0200",
         "accelZ" : -1.02,
         "voltage" : 2.887,
         "movementCounter" : 84,
         "txPower" : 4, 
         "dataFormat" : 5,
         "accelX" : 0.028
      }
   ],
   "eventId" : "6b9c6204-b85c-c249-4ba7-718fb5821217",
   "batteryLevel" : 88,
   "location" : { 
      "longitude" : 24.9512435994879,
      "latitude" : 60.1621621621622,
      "accuracy" : 2000
   }
}
```
"""


def parse_ruuvistation_data(data):
    """
    :param data: JSON object
    :return: dict of parsed Ruuvitag values
    """
    # Create object for all tags
    # TODO: add gateway data here too
    parsed_data = {
        'ruuvitags': [],
    }
    timestamp = parse(data['time'])
    fields = {
        'batteryLevel': data['batteryLevel'],
        'latitude': data['location']['latitude'],
        'longitude': data['location']['longitude'],
        'accuracy': data['location']['accuracy'],
    }
    dataline = create_dataline(timestamp, fields)
    parsed_data['gateway'] = {
        'devid': data['deviceId'],
        'datalines': [dataline]
    }
    fields2save = ['accelX', 'accelY', 'accelZ', 'humidity', 'measurementSequenceNumber', 'movementCounter',
                   'pressure', 'rssi', 'temperature', 'voltage']
    for item in data['tags']:
        datalines = []
        fields = {}
        devid = item.pop('id').upper()
        ts_str = item.pop('updateAt')
        timestamp = parse(ts_str)
        for f in fields2save:
            fields[f] = float(item[f])
        dataline = create_dataline(timestamp, fields)
        datalines.append(dataline)
        parsed_data['ruuvitags'].append({'devid': devid, 'datalines': datalines})
    return parsed_data


class RuuvistationDecoder(DecoderProvider):
    description = 'Decode Sentilo protocol payload'

    def decode_payload(self, payload, port, **kwargs):
        data = parse_ruuvistation_data(payload)
        return data
