import json
import logging
from django.conf import settings

from broker.management.commands import RabbitCommand
from fvhexperiments.decoders.sentilo import parse_sentilo_data

from broker.utils import (
    create_dataline, create_parsed_data_message,
    data_pack, data_unpack,
    get_datalogger_decoder,
    save_parse_fail_datalogger_message,
    decode_json_body, get_datalogger, decode_payload,
    create_routing_key, send_message
)

logger = logging.getLogger('sentilo')


def parse_sentilo_request(serialised_request, data):
    # TODO: This may fail, so prepare to handle exception properly
    devid = data['sensors'][0]['sensor'][0:-2]
    datalogger, created = get_datalogger(devid=devid, update_activity=False)
    # Test it by configuring wrong decoder for some Datalogger
    try:
        parsed_data = decode_payload(datalogger, data, '')
        # print(json.dumps(parsed_data, indent=1))
    except ValueError as err:
        decoder = get_datalogger_decoder(datalogger)
        err_msg = f'Failed to parse "{data}" using "{decoder}" for "{devid}": {err}'
        logger.warning(err_msg)
        # print(err_msg)
        serialised_request['parse_fail'] = {
            'error_message': str(err),
            'decoder': get_datalogger_decoder(datalogger)
        }
        save_parse_fail_datalogger_message(devid, data_pack(serialised_request))
        return True
    logging.debug(parsed_data)

    # RabbitMQ part
    key = create_routing_key('sentilo', devid)
    exchange = settings.PARSED_DATA_HEADERS_EXCHANGE
    # We need to loop both keys (LAeq, LAeq1s) and create separate message for them
    for k in parsed_data.keys():
        datalines = parsed_data[k]['datalines']
        message = create_parsed_data_message(devid, datalines=datalines)
        packed_message = data_pack(message)
        logger.debug(f'exchange={settings.PARSED_DATA_EXCHANGE} key={key}  packed_message={packed_message}')
        config = {}
        # TODO: implement and use get_datalogger_config()
        if datalogger.application:
            config = json.loads(datalogger.application.config)
        # TODO: get influxdb variables from Application / Datalogger / Forward etc config
        if 'influxdb_database' in config and 'influxdb_measurement' in config:
            config['influxdb'] = '1'
            # Use key name as measurement's name, this will override what is set elsewhere
            # (e.g. in Application config), check save2influxdb.py
            config['influxdb_measurement'] = k
        send_message(exchange, '', packed_message, headers=config)
    return True


def consumer_callback(channel, method, properties, body, options=None):
    serialised_request = data_unpack(body)
    ok, data = decode_json_body(serialised_request['request.body'])
    if ok:
        parse_sentilo_request(serialised_request, data)
        # logger.debug(json.dumps(data, indent=2))
    else:
        logger.warning(f'Failed to parse senstilo data.')
    channel.basic_ack(method.delivery_tag)


class Command(RabbitCommand):
    help = 'Decode Sentilo'

    def add_arguments(self, parser):
        parser.add_argument('--prefix', type=str,
                            help='queue and routing_key prefix, overrides settings.ROUTING_KEY_PREFIX')
        super().add_arguments(parser)

    def handle(self, *args, **options):
        logger.info(f'Start handling {__name__}')
        name = 'sentilo'
        # FIXME: constructing options should be in a function in broker.utils
        if options["prefix"] is None:
            prefix = settings.RABBITMQ["ROUTING_KEY_PREFIX"]
        else:
            prefix = options["prefix"]
        options['exchange'] = settings.RAW_HTTP_EXCHANGE
        options['routing_key'] = f'{prefix}.{name}.#'
        options['queue'] = f'{prefix}_decode_{name}_http_queue'
        options['consumer_callback'] = consumer_callback
        super().handle(*args, **options)
