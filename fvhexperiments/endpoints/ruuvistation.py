import datetime

from django.conf import settings
from django.http.response import HttpResponse

from broker.providers.endpoint import EndpointProvider
from broker.utils import (
    decode_json_body, get_datalogger, create_routing_key,
    serialize_django_request, data_pack, send_message
)


class RuuvistationEndpoint(EndpointProvider):
    description = 'Receive Ruuvi Station type Ruuvitag data (mobile app)'

    def handle_request(self, request):
        if request.method != 'POST':
            return HttpResponse('Only POST with JSON body is allowed', status=405)
        serialised_request = serialize_django_request(request)
        ok, body = decode_json_body(serialised_request['request.body'])
        if ok is False:
            return HttpResponse(f'JSON ERROR: {body}', status=400, content_type='text/plain')
        try:
            devid = body['deviceId']
        except KeyError as err:
            return HttpResponse(f'Invalid request payload, no devid found: {body}', status=400,
                                content_type='text/plain')
        serialised_request['devid'] = devid
        serialised_request['time'] = datetime.datetime.utcnow().isoformat() + 'Z'
        message = data_pack(serialised_request)
        key = create_routing_key('ruuvistation', devid)
        send_message(settings.RAW_HTTP_EXCHANGE, key, message)
        if devid is not None:
            datalogger, created = get_datalogger(devid=devid, update_activity=True)
        return HttpResponse('OK', content_type='text/plain')
