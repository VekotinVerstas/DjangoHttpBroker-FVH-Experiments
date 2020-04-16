"""
Parser for Petri's ruuvi counter ESP32 himmeli
https://bitbucket.org/iotpetri/hki_kuva_iot/src/master/ESP32/LORA/ESP32_RuuviTagGW_Lora_v2/
"""


def parse_ruuvicounter(hex_str, port=None):
    if port == '1':
        last_field = 'battery'
    elif port == '2':
        last_field = 'btrssi'
    else:
        last_field = 'unknown'
    parsed_data = {'tags': []}
    gw_batt_hex, hex_str = hex_str[:2], hex_str[2:]
    parsed_data['gateway'] = {'battery': (int(gw_batt_hex, 16) * 8) + 2500}
    chunk_size = 4 * 2  # 4 bytes, 8 hex chars
    while len(hex_str) >= chunk_size:
        data = {}
        chunk, hex_str = hex_str[:chunk_size], hex_str[chunk_size:]
        if chunk.startswith('0000'):
            continue
        rv_mac, rv_mc_hex, rv_batt_hex = chunk[:4], chunk[4:6], chunk[6:8]
        data['mac'] = rv_mac
        data['movement_counter'] = int(rv_mc_hex, 16)
        data[last_field] = ((int(rv_batt_hex, 16) * 8) + 1500)
        parsed_data['tags'].append(data)
    return parsed_data


if __name__ == '__main__':
    import sys

    try:
        print(parse_ruuvicounter(sys.argv[1], sys.argv[2]))
    except IndexError as err:
        print('Some examples:')
        for s in [
            ('bd0164d6c711647ecc0c10e0a8000000000000000000000000000000000000000000000000000000000000000000000000', 1),
            ('ba0164d8c711647ecc0c1078a8000000000000000000000000000000000000000000000000000000000000000000000000', 1),
        ]:
            print(parse_ruuvicounter(s[0], s[1]))
        print(f'\nUsage: {sys.argv[0]} hex_payload port\n\n')
        raise
