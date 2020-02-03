from dateutil.parser import parse
import datetime
import json

from broker.providers.decoder import DecoderProvider
from broker.utils import create_dataline

"""
Sentilo / CESVA TA120 decoder

Payload  contains 2 interesting "sensor" objects: 
- "<sensor_id>-N" (1 minute LAeq dBA average)
- "<sensor_id>-S" (1 second values of LAeq dBA)

Example PUT payload:

```
{"sensors":[
 {
  "sensor":"TA120-T246177-N",
  "observations":[
   {"value":"45.4", "timestamp":"02/01/2018T11:22:59UTC"}
  ]
 },{
  "sensor":"TA120-T246177-O",
  "observations":[
   {"value":"false", "timestamp":"02/01/2018T11:22:59UTC"}
  ]
 },{
  "sensor":"TA120-T246177-U",
  "observations":[
   {"value":"false", "timestamp":"02/01/2018T11:22:59UTC"}
  ]
 },{
  "sensor":"TA120-T246177-M",
  "observations":[
   {"value":"100", "timestamp":"02/01/2018T11:22:59UTC"}
  ]
 },{
  "sensor":"TA120-T246177-S",
  "observations":[
   {"value":"044.0,0,0;043.9,0,0;044.2,0,0;044.0,0,0;043.8,0,0;043.9,0,0;044.5,0,0;044.2,0,0;043.8,0,0;044.2,0,0;044.5,0,0;044.7,0,0;044.4,0,0;044.8,0,0;044.2,0,0;045.3,0,0;046.1,0,0;046.5,0,0;046.6,0,0;046.1,0,0;046.3,0,0;046.7,0,0;048.1,0,0;048.5,0,0;048.4,0,0;049.7,0,0;051.6,0,0;047.8,0,0;047.7,0,0;046.7,0,0;046.0,0,0;044.9,0,0;043.9,0,0;043.5,0,0;043.1,0,0;042.5,0,0;043.8,0,0;043.5,0,0;043.4,0,0;043.4,0,0;042.9,0,0;045.2,0,0;043.0,0,0;044.2,0,0;043.4,0,0;044.3,0,0;044.1,0,0;043.2,0,0;043.6,0,0;042.9,0,0;043.1,0,0;043.9,0,0;044.2,0,0;044.1,0,0;048.0,0,0;043.7,0,0;042.9,0,0;048.0,0,0;044.4,0,0;044.5,0,0", "timestamp":"02/01/2018T11:22:59UTC"}
  ]
 }
]}
```
"""


def parse_sentilo_data(data):
    """
    Extract two data objects from source data:
    - 1 minute average
    - 60 values for every seconds
    :param data: JSON object
    :return: dict of parsed dBA values, one for 1 min average and 60 for every second.
    """
    # Create object for both 1 min and 1 sec data
    parsed_data = {
        'LAeq': {'datalines': []},
        'LAeq1s': {'datalines': []},
    }
    for item in data['sensors']:
        devid = item['sensor'][0:-2]
        parsed_data['LAeq']['devid'] = devid
        parsed_data['LAeq1s']['devid'] = devid
        ts_str = item['observations'][0].get('timestamp')
        if ts_str is not None:
            timestamp = parse(item['observations'][0]['timestamp'], dayfirst=True)
        else:
            timestamp = datetime.datetime.utcnow()
            print(timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ data without timestamp!"))
        if item['sensor'].endswith('N'):
            fields = {'dBA': float(item['observations'][0]['value'])}
            dataline = create_dataline(timestamp, fields)
            parsed_data['LAeq']['datalines'].append(dataline)
        if item['sensor'].endswith('S'):
            cnt = 0
            secvals = item['observations'][0]['value'].split(';')
            secvals.reverse()
            for val in secvals:
                fields = {'dBA': float(val.split(',')[0])}
                new_ts = (timestamp - datetime.timedelta(seconds=cnt))
                dataline = create_dataline(new_ts, fields)
                parsed_data['LAeq1s']['datalines'].append(dataline)
                cnt += 1
    return parsed_data


class SentiloDecoder(DecoderProvider):
    description = 'Decode Sentilo protocol payload'

    def decode_payload(self, payload, port, **kwargs):
        data = parse_sentilo_data(payload)
        return data
