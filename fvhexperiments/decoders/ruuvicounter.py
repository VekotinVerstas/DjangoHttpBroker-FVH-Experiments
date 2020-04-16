"""
Ruuvi Counter (LoRaWAN) decoder
See: https://bitbucket.org/iotpetri/hki_kuva_iot/src/master/ESP32/LORA/ESP32_RuuviTagGW_Lora_v2/
"""
import json

import pytz
from dateutil.parser import parse

from broker.providers.decoder import DecoderProvider
from broker.utils import create_dataline
from fvhexperiments.parsers.ruuvicounter import parse_ruuvicounter


def parse_ruuvicounter_data(data, port, serialised_request):
    """
    :param data: JSON object
    :return: dict of parsed Ruuvitag values
    """
    # Create object for all tags and gateway
    lora = json.loads(serialised_request['request.body'].decode("utf-8"))
    ruuvicounterdata = parse_ruuvicounter(data, port)
    devid = serialised_request['devid']
    timestamp = parse(lora['DevEUI_uplink']['Time']).astimezone(pytz.UTC)
    dataline = create_dataline(timestamp, ruuvicounterdata['gateway'])
    parsed_data = {}
    parsed_data['gateway'] = {'devid': devid, 'datalines': [dataline]}
    parsed_data['ruuvicounter'] = {'datalines': []}
    for item in ruuvicounterdata['tags']:
        mac = item.pop('mac')
        dataline = create_dataline(timestamp, item, extra={'extratags': {'mac': mac}})
        parsed_data['ruuvicounter']['datalines'].append(dataline)
    return parsed_data


class RuuvicounterDecoder(DecoderProvider):
    description = 'Decode Ruuvicounter payload'

    def decode_payload(self, payload, port, **kwargs):
        #  serialised_request is needed to parse data correctly
        serialised_request = kwargs.get('serialised_request')
        data = parse_ruuvicounter_data(payload, port, serialised_request)
        return data
