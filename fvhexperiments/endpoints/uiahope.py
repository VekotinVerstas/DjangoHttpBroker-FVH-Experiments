from django.http.response import HttpResponse

from broker.providers.endpoint import EndpointProvider
from broker.utils import (
    serialize_django_request, decode_json_body, epoch2datetime,
    create_dataline, create_parsed_data_message
)
from broker.utils import get_datalogger, datalogger_get_config
from broker.utils.influxdb import get_influxdb_client, create_influxdb_objects, save_parsed_data2influxdb


class UiahopeEndpoint(EndpointProvider):
    description = 'Receive custom UIA HOPE data from portable air quality sensor'

    def handle_request(self, request):
        serialised_request = serialize_django_request(request)
        ok, body = decode_json_body(serialised_request['request.body'])
        if ok is False:
            return HttpResponse(f'JSON ERROR: {body}', status=400, content_type='text/plain')
        key = body.get('key', '')
        key_splitted = key.split('/')
        if len(key_splitted) == 3:
            devid = '/'.join(key_splitted[:2])
        else:
            return HttpResponse(f'Key error: key "{key}" is not correctly formed', status=400,
                                content_type='text/plain')
        datalogger, created = get_datalogger(devid=devid, update_activity=False, create=False)
        if datalogger is None:
            return HttpResponse(f'Datalogger "{devid}" does not exist', status=400, content_type='text/plain')
        data = body['data']
        location = data.pop('location')
        data.update(location)
        epoch = data.pop('timestamp') / 1000
        timestamp = epoch2datetime(epoch)
        dataline = create_dataline(timestamp, data)
        datalines = [dataline]
        parsed_data = create_parsed_data_message(devid, datalines)
        config = datalogger_get_config(datalogger, parsed_data)
        db_name = config.get('influxdb_database')
        measurement_name = config.get('influxdb_measurement')
        if db_name is not None and measurement_name is not None:
            save_parsed_data2influxdb(db_name, measurement_name, parsed_data)
            return HttpResponse('OK', content_type='text/plain')
        else:
            return HttpResponse(f'InfluxDB database and measurement are not defined for Datalogger "{devid}"',
                                status=400, content_type='text/plain')

