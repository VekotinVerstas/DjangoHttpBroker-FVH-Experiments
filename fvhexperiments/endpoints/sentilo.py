import datetime

from django.conf import settings
from django.http.response import HttpResponse

from broker.providers.endpoint import EndpointProvider
from broker.utils import (
    decode_json_body, get_datalogger, create_routing_key,
    serialize_django_request, data_pack, send_message
)


class SentiloEndpoint(EndpointProvider):
    description = 'Receive Sentilo protocol data (e.g. from noise sensors)'

    def handle_request(self, request):
        if request.method != 'PUT':
            return HttpResponse('Only PUT with JSON body is allowed', status=405)
        serialised_request = serialize_django_request(request)
        ok, body = decode_json_body(serialised_request['request.body'])
        if ok is False:
            return HttpResponse(f'JSON ERROR: {body}', status=400, content_type='text/plain')
        try:
            devid = body['sensors'][0]['sensor'][0:-2]
        except KeyError as err:
            return HttpResponse(f'Invalid request payload, no devid found: {body}', status=400,
                                content_type='text/plain')
        serialised_request['devid'] = devid
        serialised_request['time'] = datetime.datetime.utcnow().isoformat() + 'Z'
        message = data_pack(serialised_request)
        key = create_routing_key('sentilo', devid)
        send_message(settings.RAW_HTTP_EXCHANGE, key, message)
        if devid is not None:
            datalogger, created = get_datalogger(devid=devid, update_activity=True)
        return HttpResponse('OK', content_type='text/plain')
