from dateutil.parser import parse
import datetime
import json

from broker.providers.decoder import DecoderProvider

"""
Sentilo / CESVA TA120 decoder

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
    measurements = []
    for item in data['sensors']:
        ts_str = item['observations'][0].get('timestamp')
        if ts_str is not None:
            ts = parse(item['observations'][0]['timestamp'], dayfirst=True)
        else:
            ts = datetime.datetime.utcnow()
            print(ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ data without timestamp!"))
        dev_id = item['sensor'][0:-2]
        # if item['sensor'].endswith('N'):
        #     fields = {'dBA': float(item['observations'][0]['value'])}
        #     measurement = create_influxdb_obj(dev_id, 'LAeq', fields, timestamp=ts)
        #     measurements.append(measurement)
        # if item['sensor'].endswith('S'):
        #     cnt = 0
        #     secvals = item['observations'][0]['value'].split(';')
        #     secvals.reverse()
        #     for val in secvals:
        #         fields = {'dBA': float(val.split(',')[0])}
        #         measurement = create_influxdb_obj(dev_id, 'LAeq1s', fields,
        #                                           timestamp=(ts - datetime.timedelta(seconds=cnt)))
        #         cnt += 1
        #         measurements.append(measurement)
    return measurements


class SentiloDecoder(DecoderProvider):
    description = 'Decode Sentilo protocol payload'

    def decode_payload(self, hex_payload, port, **kwargs):
        """
        Payload example:
        e4050000004058460078dc46000003430050114600c03544000010410000254300003042000033430000000003000000

        C Source:
        typedef struct  {
          int16_t batteryVoltageRaw;
          int16_t panelVoltageRaw;
          float mainVoltage_V;
          float panelVoltage_VPV;
          float panelPower_PPV;
          float batteryCurrent_I;
          float yieldTotal_H19;
          float yieldToday_H20;
          float maxPowerToday_H21;
          float yieldYesterday_H22;
          float maxPowerYesterday_H23;
          int errorCode_ERR;
          int stateOfOperation_CS;
        } measurement ;

        :param str hex_payload:
        :param port:
        :param kwargs:
        :return:
        """
        n = 8  # Split hex_payload into 8 character long chunks
        s = [hex_payload[i:i + n] for i in range(0, len(hex_payload), n)]
        data = {
            'mainVoltage_V': decode_lsb_float(s[2]),
            'panelVoltage_VPV': decode_lsb_float(s[3]),
            'panelPower_PPV': decode_lsb_float(s[4]),
            'batteryCurrent_I': decode_lsb_float(s[5]),
            'yieldTotal_H19': decode_lsb_float(s[6]),
            'yieldToday_H20': decode_lsb_float(s[7]),
            'maxPowerToday_H21': decode_lsb_float(s[8]),
        }
        return data
